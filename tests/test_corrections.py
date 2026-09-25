"""更正传播与部分数据失效：标记下游、重算核销、跨情景独立性。"""
from __future__ import annotations

from tests.helpers import (
    AppTestCase, demand_batch, flow_batch, mapping, seed_complete_scenario,
    standard_params,
)


class CorrectionPropagationTests(AppTestCase):
    def _forecast(self, sid: str) -> str:
        f = self.app.forecasts
        j = f.submit_forecast(sid)["job_id"]
        return f.run_job(j)["result"]["forecast_version_id"]

    def test_correction_marks_downstream_forecasts(self) -> None:
        v = self.app.versions
        sid, _p, _m, _inputs, _ = seed_complete_scenario(self.app, ("A01",))
        fvid = self._forecast(sid)

        # 找到 A01 车流输入版本并更正
        snap = v.snapshot(sid)
        flow_vid = snap.inputs["traffic_flow:A01"]
        doc = flow_batch("A01")
        doc["records"][0]["value"] = 3000
        c = v.correct_raw_batch(flow_vid, doc, reason="卡口误标")

        marks = self.app.invalidations(fvid)
        self.assertEqual(len(marks), 1)
        mark = marks[0]
        self.assertEqual(mark["status"], "affected")
        self.assertEqual(mark["correction_id"], c["correction_id"])
        self.assertEqual(mark["original_version_id"], flow_vid)
        self.assertIsNone(mark["resolution_forecast_version_id"])

    def test_partial_invalidation_isolates_data_sources(self) -> None:
        """只更正一个数据源：使用它的预测失效，不使用的预测不受影响。"""
        v = self.app.versions
        # 情景 1 使用 A01 车流；情景 2 只使用 A02 车流
        sid1, *_ = seed_complete_scenario(self.app, ("A01",))
        f1 = self._forecast(sid1)

        a2_flow = v.import_raw_batch(flow_batch("A02"))["version_id"]
        a2_dem = v.import_raw_batch(demand_batch("A02"))["version_id"]
        pid = v.import_parameters(standard_params())["version_id"]
        mid = v.import_region_map(mapping(("A02", "R2", 1.0)))["version_id"]
        sid2 = v.create_scenario(parameters_version_id=pid, mapping_version_id=mid)["scenario_id"]
        v.add_input(sid2, a2_flow); v.add_input(sid2, a2_dem)
        f2 = self._forecast(sid2)

        a1_flow = v.snapshot(sid1).inputs["traffic_flow:A01"]
        doc = flow_batch("A01"); doc["records"][0]["value"] = 1
        v.correct_raw_batch(a1_flow, doc)

        marks_f1 = self.app.invalidations(f1)
        marks_f2 = self.app.invalidations(f2)
        self.assertEqual(len(marks_f1), 1)
        self.assertEqual(marks_f1[0]["status"], "affected")
        self.assertEqual(marks_f2, [])

    def test_recompute_resolves_marks(self) -> None:
        v = self.app.versions
        sid, *_ = seed_complete_scenario(self.app, ("A01",))
        old_f = self._forecast(sid)
        flow_vid = v.snapshot(sid).inputs["traffic_flow:A01"]
        doc = flow_batch("A01"); doc["records"][0]["value"] = 2500
        c = v.correct_raw_batch(flow_vid, doc)
        self.assertTrue(any(m["status"] == "affected"
                            for m in self.app.invalidations(old_f)))

        v.replace_input(sid, flow_vid, c["corrected_version_id"])
        new_f = self._forecast(sid)
        self.assertNotEqual(old_f, new_f)
        marks = self.app.invalidations(old_f)
        self.assertTrue(marks)
        for m in marks:
            self.assertEqual(m["status"], "resolved")
            self.assertEqual(m["resolution_forecast_version_id"], new_f)

    def test_unrelated_recompute_does_not_resolve(self) -> None:
        """另一个情景重算不会核销本情景的标记。"""
        v = self.app.versions
        sid1, *_ = seed_complete_scenario(self.app, ("A01",))
        f1 = self._forecast(sid1)
        flow_vid = v.snapshot(sid1).inputs["traffic_flow:A01"]
        doc = flow_batch("A01"); doc["records"][0]["value"] = 2500
        c = v.correct_raw_batch(flow_vid, doc)

        sid2, *_ = seed_complete_scenario(self.app, ("A01",))
        # sid2 使用的是更正后的版本（更正版本已存在，add_input 新情景）
        snap2_inputs = v.snapshot(sid2).inputs
        # sid2 仍指向原版本（内容相同），替换成更正版本再算
        v.replace_input(sid2, flow_vid, c["corrected_version_id"])
        f2 = self._forecast(sid2)
        # sid1 的标记不应被 sid2 的重算核销
        self.assertTrue(any(m["status"] == "affected"
                            for m in self.app.invalidations(f1)))

    def test_adopted_forecast_still_marked_and_verifiable_after_correction(self) -> None:
        v = self.app.versions
        sid, *_ = seed_complete_scenario(self.app, ("A01",))
        fvid = self._forecast(sid)
        v.adopt(fvid, {"deployments": [{"deploy_region": "R1", "mobile_units": 2}]},
                actor="li")
        flow_vid = v.snapshot(sid).inputs["traffic_flow:A01"]
        doc = flow_batch("A01"); doc["records"][0]["value"] = 42
        v.correct_raw_batch(flow_vid, doc)
        # 采用状态不因更正而被改写（仍是 adopted），但存在未处理失效标记
        self.assertEqual(v.adoption_status(fvid)["status"], "adopted")
        marks = self.app.invalidations(fvid)
        self.assertEqual(marks[0]["status"], "affected")
        report = v.verify_lineage(fvid)
        self.assertFalse(report["ok"])  # 有未处理失效 -> 核验不通过


if __name__ == "__main__":
    unittest.main()
