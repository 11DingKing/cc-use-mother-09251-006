"""情景提交链：派生、替换、并发冲突与快照解析。"""
from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor

from service_09251_006.errors import ConflictError, InvalidStateError, ValidationError
from tests.helpers import AppTestCase, flow_batch, mapping, standard_params


class ScenarioTests(AppTestCase):
    def _seed(self):
        v = self.app.versions
        a1 = v.import_raw_batch(flow_batch("A01"))["version_id"]
        a2 = v.import_raw_batch(flow_batch("A02"))["version_id"]
        pid = v.import_parameters(standard_params())["version_id"]
        mid = v.import_region_map(mapping(("A01", "R1", 1.0), ("A02", "R1", 1.0)))["version_id"]
        sid = v.create_scenario(parameters_version_id=pid, mapping_version_id=mid)["scenario_id"]
        return sid, a1, a2, pid, mid

    def test_commit_chain_resolves_snapshot(self) -> None:
        v = self.app.versions
        sid, a1, a2, pid, mid = self._seed()
        v.add_input(sid, a1)
        v.add_input(sid, a2)
        detail = v.scenario_detail(sid)
        self.assertEqual(detail["seq"], 2)
        snap = v.snapshot(sid)
        self.assertEqual(snap.inputs["traffic_flow:A01"], a1)
        self.assertEqual(snap.inputs["traffic_flow:A02"], a2)
        self.assertEqual(snap.parameters_version_id, pid)
        self.assertEqual(snap.mapping_version_id, mid)

    def test_add_same_source_twice_conflicts(self) -> None:
        v = self.app.versions
        sid, a1, _a2, _p, _m = self._seed()
        v.add_input(sid, a1)
        with self.assertRaises(ConflictError):
            v.add_input(sid, a1)

    def test_replace_input_requires_current(self) -> None:
        v = self.app.versions
        sid, a1, _a2, _p, _m = self._seed()
        v.add_input(sid, a1)
        doc = flow_batch("A01")
        doc["records"][0]["value"] = 5
        corrected = v.correct_raw_batch(a1, doc)["corrected_version_id"]
        # 用一个不属于该数据源的版本替换 -> 拒绝
        with self.assertRaises(ValidationError):
            v.replace_input(sid, a1, _a2)
        # 旧版本不是当前生效版本（corrected 尚未进入情景）-> 拒绝
        doc2 = flow_batch("A01")
        doc2["records"][0]["value"] = 6
        corrected2 = v.correct_raw_batch(a1, doc2)["corrected_version_id"]
        with self.assertRaises(ConflictError):
            v.replace_input(sid, corrected, corrected2)
        commit = v.replace_input(sid, a1, corrected)
        self.assertEqual(commit["seq"], 2)
        self.assertEqual(v.snapshot(sid).inputs["traffic_flow:A01"], corrected)

    def test_replace_same_version_rejected(self) -> None:
        v = self.app.versions
        sid, a1, _a2, _p, _m = self._seed()
        v.add_input(sid, a1)
        with self.assertRaises(ValidationError):
            v.replace_input(sid, a1, a1)

    def test_noop_parameter_derivation_conflicts(self) -> None:
        v = self.app.versions
        sid, _a1, _a2, pid, _m = self._seed()
        with self.assertRaises(ConflictError):
            v.set_parameters(sid, pid)

    def test_branch_from_historical_commit(self) -> None:
        """可以对历史提交做预测（不可变谱系的可复算性）。"""
        v = self.app.versions
        sid, a1, _a2, pid, mid = self._seed()
        c1 = v.add_input(sid, a1)
        head_after_first = c1["commit_id"]
        v.add_input(sid, _a2)  # HEAD 继续前进
        snap = v.snapshot(sid, head_after_first)
        self.assertEqual(set(snap.inputs), {"traffic_flow:A01"})
        self.assertEqual(snap.commit_id, head_after_first)

    def test_concurrent_derivation_single_winner(self) -> None:
        v = self.app.versions
        sid, a1, a2, _pid, _m = self._seed()
        head = v.scenario_detail(sid)["head_commit_id"]
        n = 12
        errors: list[str] = []
        commits: list = []
        barrier = threading.Barrier(n)

        def derive(version_id: str) -> None:
            barrier.wait()
            try:
                commits.append(v.set_parameters(
                    sid, version_id, base_commit_id=head))
            except ConflictError as exc:
                errors.append(exc.code)

        new_pid = v.import_parameters(standard_params(growth_factor=1.3))["version_id"]
        with ThreadPoolExecutor(max_workers=n) as pool:
            list(pool.map(lambda _: derive(new_pid), range(n)))
        self.assertEqual(len(commits), 1)
        self.assertEqual(len(errors), n - 1)
        self.assertEqual(v.scenario_detail(sid)["seq"], 1)

    def test_concurrent_add_different_inputs_both_succeed(self) -> None:
        """并发派生不同数据源（同 base）：靠唯一索引串行接链，两次都成功。"""
        v = self.app.versions
        sid, a1, a2, _p, _m = self._seed()
        results = {}

        def add(which: str, vid: str) -> None:
            for attempt in range(10):
                try:
                    v.add_input(sid, vid)
                    results[which] = "ok"
                    return
                except ConflictError:
                    # HEAD 被其他线程推进：刷新 base 后重试（乐观并发重试）
                    continue
            results[which] = "fail"

        t1 = threading.Thread(target=add, args=("a1", a1))
        t2 = threading.Thread(target=add, args=("a2", a2))
        t1.start(); t2.start(); t1.join(); t2.join()
        self.assertEqual(results, {"a1": "ok", "a2": "ok"})
        self.assertEqual(v.scenario_detail(sid)["seq"], 2)

    def test_forecast_requires_complete_snapshot(self) -> None:
        v = self.app.versions
        sid = v.create_scenario()["scenario_id"]
        with self.assertRaises(InvalidStateError):
            self.app.forecasts.submit_forecast(sid)


if __name__ == "__main__":
    unittest.main()
