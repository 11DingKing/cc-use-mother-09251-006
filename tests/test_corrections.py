"""部分数据失效：更正只波及使用了受影响站点数据的版本，历史不被篡改。"""
from __future__ import annotations

import unittest

from helpers import (
    ServiceTestCase,
    computed_version,
    hourly_records,
    import_batch,
    make_mapping,
    make_params,
)
from service_09251_006.domain import StaleVersionError


class PartialInvalidationTests(ServiceTestCase):
    def setUp(self) -> None:
        super().setUp()
        # S1/S2 → AreaA；S3 → AreaB。batch1 含 S1/S2，batch2 含 S3。
        self.mapping_id = make_mapping(
            self.service, {"S1": "AreaA", "S2": "AreaA", "S3": "AreaB"})
        self.param_id = make_params(self.service)
        self.batch1 = import_batch(
            self.service, "bureau",
            hourly_records("S1", 20, {8: 100.0}) + hourly_records("S2", 20, {8: 60.0}))
        self.batch2 = import_batch(
            self.service, "bureau", hourly_records("S3", 20, {8: 40.0}))
        self.version_a = computed_version(
            self.service, batch_ids=[self.batch1["batch"]["batch_id"]],
            param_id=self.param_id, mapping_id=self.mapping_id, scenario="area-a")
        self.version_b = computed_version(
            self.service, batch_ids=[self.batch2["batch"]["batch_id"]],
            param_id=self.param_id, mapping_id=self.mapping_id, scenario="area-b")

    def _correct(self, **kw):
        return import_batch(
            self.service, "bureau-fixed", hourly_records("S1", 20, {8: 130.0}),
            corrects=self.batch1["batch"]["batch_id"], **kw)

    def test_station_scoped_correction_marks_only_affected_versions(self) -> None:
        result = self._correct(correction_stations=["S1"],
                               correction_reason="S1 仪表漂移")
        impacted = result["correction"]["impacted_versions"]
        self.assertEqual([self.version_a["version_id"]], impacted)

        va = self.service.get_version(self.version_a["version_id"])
        vb = self.service.get_version(self.version_b["version_id"])
        self.assertTrue(va["stale"])
        self.assertIn("S1", va["stale_reason"])
        self.assertFalse(vb["stale"])

    def test_history_is_not_rewritten_by_correction(self) -> None:
        before = self.service.get_version(self.version_a["version_id"])
        rows_before = self.service.get_results(self.version_a["version_id"])["rows"]
        self._correct(correction_stations=["S1"])
        after = self.service.get_version(self.version_a["version_id"])
        rows_after = self.service.get_results(self.version_a["version_id"])["rows"]
        # 状态、结果与结果哈希保持原样，仅追加 stale 标记。
        self.assertEqual(before["status"], after["status"])
        self.assertEqual(before["result_hash"], after["result_hash"])
        self.assertEqual(rows_before, rows_after)

    def test_descendant_inheriting_bad_batch_is_stale_but_corrected_one_is_clean(self) -> None:
        corrected = self._correct(correction_stations=["S1"])
        corrected_batch_id = corrected["batch"]["batch_id"]

        inheriting = self.service.derive_version(
            self.version_a["version_id"], scenario="still-old-data")
        self.assertTrue(inheriting["stale"], "沿用被更正批次的派生版本应即建即标")

        fixed = self.service.derive_version(
            self.version_a["version_id"], scenario="uses-corrected",
            batch_ids=[corrected_batch_id])
        self.assertFalse(fixed["stale"], "换用更正批次的派生版本不受影响")

    def test_time_range_scoped_correction(self) -> None:
        # S1 只有 08 时记录，S2 只有 09 时记录；更正仅覆盖 08 时段。
        batch = import_batch(
            self.service, "ranged",
            hourly_records("S1", 21, {8: 10.0}) + hourly_records("S2", 21, {9: 20.0}))
        version = self.service.create_version(
            batch_ids=[batch["batch"]["batch_id"]], param_id=self.param_id,
            mapping_id=self.mapping_id, scenario="ranged")
        self.service.register_correction(
            target_batch_id=batch["batch"]["batch_id"],
            time_range=["2026-09-21T07:00:00Z", "2026-09-21T08:59:59Z"],
            reason="早高峰数据补录")
        self.assertTrue(self.service.get_version(version["version_id"])["stale"])

    def test_version_not_using_affected_station_stays_clean(self) -> None:
        # 映射只含 S2 的版本：更正针对 S1，不应波及。
        mapping_s2 = make_mapping(self.service, {"S2": "AreaC"})
        batch = import_batch(
            self.service, "s2-only", hourly_records("S2", 22, {8: 30.0}))
        version = computed_version(
            self.service, batch_ids=[batch["batch"]["batch_id"]],
            param_id=self.param_id, mapping_id=mapping_s2, scenario="s2-only")
        self.service.register_correction(
            target_batch_id=batch["batch"]["batch_id"], stations=["S1"],
            reason="S1 不在本批次，仅登记")
        self.assertFalse(self.service.get_version(version["version_id"])["stale"])

    def test_stale_signed_version_cannot_be_adopted(self) -> None:
        self.service.sign_version(self.version_a["version_id"], signed_by="analyst")
        self._correct(correction_stations=["S1"])
        with self.assertRaises(StaleVersionError):
            self.service.add_decision(
                self.version_a["version_id"], service_area="AreaA",
                action="deploy-mobile-chargers", units=2, decided_by="ops")

    def test_lineage_verification_flags_unmarked_impact(self) -> None:
        self._correct(correction_stations=["S1"])
        report = self.service.verify_lineage(self.version_a["version_id"])
        self.assertTrue(report["ok"], "更正已正确标记，谱系核验应通过")
        stale_checks = [c for c in report["checks"] if "staleness" in c["check"]]
        self.assertTrue(stale_checks)
        self.assertTrue(all(c["ok"] for c in stale_checks))


if __name__ == "__main__":
    unittest.main()
