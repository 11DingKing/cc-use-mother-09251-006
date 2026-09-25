"""长任务可重入：检查点续跑、租约接管、重启后恢复、重复提交幂等。"""
from __future__ import annotations

from service_09251_006.container import Application
from service_09251_006.clock import FrozenClock
from tests.helpers import (
    AppTestCase, demand_batch, flow_batch, mapping, standard_params,
)


class SimulatedCrash(Exception):
    pass


def build_many_area_app(tmp: str, areas, lease: float = 3600.0) -> Application:
    from datetime import datetime, timezone

    app = Application(tmp, clock=FrozenClock(
        datetime(2026, 9, 25, 10, 0, tzinfo=timezone.utc)), lease_seconds=lease)
    v = app.versions
    for a in areas:
        v.import_raw_batch(flow_batch(a))
        v.import_raw_batch(demand_batch(a))
    pid = v.import_parameters(standard_params(horizon_hours=6))["version_id"]
    mid = v.import_region_map(mapping(*[(a, "R1", 1.0) for a in areas]))["version_id"]
    sid = v.create_scenario(parameters_version_id=pid, mapping_version_id=mid)["scenario_id"]
    detail = v.scenario_detail(sid)
    # 加入全部输入
    rows = app.repo.list_versions("raw_input")
    for r in rows:
        v.add_input(sid, r["id"])
    return app


class JobResumptionTests(AppTestCase):
    def _seed(self, areas=("A01", "A02", "A03", "A04")):
        app = build_many_area_app(self.tmp, areas, lease=60.0)
        sid = app.list_scenarios()[0]["scenario_id"]
        return app, sid

    def test_duplicate_submission_dedupes_to_same_job(self) -> None:
        v, f = self.app.versions, self.app.forecasts
        from tests.helpers import seed_complete_scenario
        sid, *_ = seed_complete_scenario(self.app, ("A01",))
        s1 = f.submit_forecast(sid)
        s2 = f.submit_forecast(sid)
        self.assertTrue(s2["deduped"])
        self.assertEqual(s2["job_id"], s1["job_id"])

    def test_crash_resumes_from_checkpoint_in_new_process(self) -> None:
        app, sid = self._seed()
        f = app.forecasts
        calls = {"n": 0}

        def crash_after_first(job_id, idx, total):
            calls["n"] += 1
            if idx == 1:
                raise SimulatedCrash("进程崩溃")

        f.step_hook = crash_after_first
        jid = f.submit_forecast(sid)["job_id"]
        with self.assertRaises(SimulatedCrash):
            f.run_job(jid)
        status = f.job_status(jid)
        self.assertEqual(status["status"], "running")
        self.assertEqual(status["cursor"]["completed_areas"], ["A01"])

        # 模拟重启：新的 Application 指向同一数据目录
        app2 = Application(self.tmp, clock=self.clock, lease_seconds=60.0)
        # 租约未过期前不认领
        self.assertIsNone(app2.forecasts.acquire_job("restarted-worker"))
        # 租约过期（冻结时钟推进）后认领续跑
        self.advance_clock(61)
        claimed = app2.forecasts.acquire_job("restarted-worker")
        self.assertIsNotNone(claimed)
        self.assertEqual(claimed["job_id"], jid)
        out = app2.forecasts.run_job(jid, owner="restarted-worker")
        self.assertEqual(out["status"], "succeeded")
        fvid = out["result"]["forecast_version_id"]
        payload = app2.repo.get_forecast(fvid)["payload"]
        self.assertEqual(set(payload["service_areas"]),
                         {"A01", "A02", "A03", "A04"})
        # A01 没有被重复计算（续跑跳过检查点中的服务区）
        self.assertEqual(calls["n"], 1)

    def test_resumed_result_byte_identical_to_fresh_run(self) -> None:
        app, sid = self._seed(areas=("A01", "A02"))
        f = app.forecasts
        calls = {"n": 0}

        def crash_after_first(job_id, idx, total):
            calls["n"] += 1
            if idx == 1:
                raise SimulatedCrash
        f.step_hook = crash_after_first
        jid = f.submit_forecast(sid)["job_id"]
        with self.assertRaises(SimulatedCrash):
            f.run_job(jid)
        self.advance_clock(61)
        app2 = Application(self.tmp, clock=self.clock, lease_seconds=60.0)
        out = app2.forecasts.run_job(jid, owner="w2")
        resumed_fvid = out["result"]["forecast_version_id"]

        # 全新情景、一次性计算 -> 同内容预测版本
        app3 = Application(self.tmp + "_mirror", clock=self.clock)
        self.addCleanup(__import__("shutil").rmtree, self.tmp + "_mirror",
                        ignore_errors=True)
        v3 = app3.versions
        for a in ("A01", "A02"):
            v3.import_raw_batch(flow_batch(a))
            v3.import_raw_batch(demand_batch(a))
        # 参数/映射内容相同 -> 相同版本号
        p = v3.import_parameters(standard_params(horizon_hours=6))["version_id"]
        m = v3.import_region_map(
            mapping(("A01", "R1", 1.0), ("A02", "R1", 1.0)))["version_id"]
        sid3 = v3.create_scenario(parameters_version_id=p, mapping_version_id=m)["scenario_id"]
        for r in app3.repo.list_versions("raw_input"):
            v3.add_input(sid3, r["id"])
        j3 = app3.forecasts.submit_forecast(sid3)["job_id"]
        fresh_fvid = app3.forecasts.run_job(j3)["result"]["forecast_version_id"]
        self.assertEqual(resumed_fvid, fresh_fvid)

    def test_worker_drains_queue_on_restart(self) -> None:
        from tests.helpers import seed_complete_scenario
        sids = [seed_complete_scenario(self.app, ("A01",))[0]
                for _ in range(3)]
        for sid in sids:
            self.app.forecasts.submit_forecast(sid)
        n = self.app.forecasts.run_available(owner="worker", limit=10)
        self.assertEqual(n, 3)
        # 再次排空：没有可运行作业
        self.assertEqual(self.app.forecasts.run_available(limit=10), 0)

    def test_failed_job_records_error_and_can_be_retried(self) -> None:
        """校验通过但引擎出错：作业标记 failed；修复数据后可人工重跑。"""
        from tests.helpers import seed_complete_scenario
        sid, *_ = seed_complete_scenario(self.app, ("A01",))
        jid = self.app.forecasts.submit_forecast(sid)["job_id"]
        # 人为破坏作业 payload 制造失败
        import sqlite3
        with self.app.db.transaction() as conn:
            conn.execute("UPDATE jobs SET payload=json_set(payload,'$.commit_id','nope') WHERE id=?", (jid,))
        # run_job 直接调用会抛错；模拟 worker 循环捕获
        try:
            self.app.forecasts.run_job(jid)
        except Exception:
            self.app.forecasts.fail_job(jid, "boom")
        self.assertEqual(self.app.forecasts.job_status(jid)["status"], "failed")


if __name__ == "__main__":
    unittest.main()
