"""谱系核验：哈希链、精确复算、指纹一致、防篡改与引擎版本隔离。"""
from __future__ import annotations

from tests.helpers import AppTestCase, seed_complete_scenario


def _forecast(app, sid: str) -> str:
    f = app.forecasts
    return f.run_job(f.submit_forecast(sid)["job_id"])["result"]["forecast_version_id"]


class LineageVerificationTests(AppTestCase):
    def test_clean_forecast_verifies(self) -> None:
        sid, *_ = seed_complete_scenario(self.app, ("A01",))
        fvid = _forecast(self.app, sid)
        report = self.app.versions.verify_lineage(fvid)
        self.assertTrue(report["ok"], report)
        names = {c["name"]: c["ok"] for c in report["checks"]}
        self.assertTrue(names["使用登记输入可精确复算预测"])
        self.assertTrue(names["内容指纹与登记一致"])

    def test_tampered_input_object_detected(self) -> None:
        sid, *_ = seed_complete_scenario(self.app, ("A01",))
        fvid = _forecast(self.app, sid)
        inp = self.app.repo.get_forecast(fvid)["inputs"][0]
        row = self.app.repo.get_version_row(inp["input_version_id"])
        path = self.app.repo.objects.path_for(row["object_id"])
        with open(path, "r+b") as fh:
            data = bytearray(fh.read())
            data[20] = (data[20] + 7) % 256
            fh.seek(0); fh.write(data)
        report = self.app.versions.verify_lineage(fvid)
        self.assertFalse(report["ok"])

    def test_tampered_forecast_detected(self) -> None:
        sid, *_ = seed_complete_scenario(self.app, ("A01",))
        fvid = _forecast(self.app, sid)
        row = self.app.repo.get_version_row(fvid)
        path = self.app.repo.objects.path_for(row["object_id"])
        with open(path, "r+b") as fh:
            data = bytearray(fh.read())
            data[-10] = ord("9") if data[-10] != ord("9") else ord("8")
            fh.seek(0); fh.write(data)
        report = self.app.versions.verify_lineage(fvid)
        self.assertFalse(report["ok"])

    def test_missing_input_version_detected(self) -> None:
        sid, *_ = seed_complete_scenario(self.app, ("A01",))
        fvid = _forecast(self.app, sid)
        inp = self.app.repo.get_forecast(fvid)["inputs"][0]
        # 删除输入的元数据行（模拟指针断裂）
        with self.app.db.transaction() as conn:
            conn.execute("DELETE FROM versions WHERE id=?",
                         (inp["input_version_id"],))
        report = self.app.versions.verify_lineage(fvid)
        self.assertFalse(report["ok"])

    def test_open_invalidation_fails_verification(self) -> None:
        from tests.helpers import flow_batch
        sid, *_ = seed_complete_scenario(self.app, ("A01",))
        fvid = _forecast(self.app, sid)
        flow_vid = self.app.versions.snapshot(sid).inputs["traffic_flow:A01"]
        doc = flow_batch("A01"); doc["records"][0]["value"] = 1
        self.app.versions.correct_raw_batch(flow_vid, doc)
        self.assertFalse(self.app.versions.verify_lineage(fvid)["ok"])

    def test_engine_version_participates_in_fingerprint(self) -> None:
        sid, *_ = seed_complete_scenario(self.app, ("A01",))
        fvid = _forecast(self.app, sid)
        fp_before = self.app.repo.get_forecast(fvid)["fingerprint"]
        # 同内容在不同引擎版本号下指纹不同
        from service_09251_006.services.versioning import VersionService
        snap = self.app.versions.snapshot(sid)
        fp1 = self.app.versions.forecast_fingerprint(snap)
        future = VersionService(self.app.repo, self.clock, self.app.idgen,
                                engine_version="engine-future")
        fp2 = future.forecast_fingerprint(snap)
        self.assertNotEqual(fp1, fp2)
        self.assertEqual(fp_before, fp1)


if __name__ == "__main__":
    unittest.main()
