"""重复批次：导入幂等、批内去重与更正导入登记。"""
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


class DuplicateBatchTests(ServiceTestCase):
    def test_same_payload_imported_twice_is_idempotent(self) -> None:
        records = hourly_records("S1", 20, {8: 100.0, 9: 200.0})
        first = import_batch(self.service, "bureau", records)
        second = import_batch(self.service, "bureau", records)

        self.assertTrue(first["created"])
        self.assertFalse(second["created"])
        self.assertEqual(first["batch"]["batch_id"], second["batch"]["batch_id"])
        self.assertEqual(1, len(self.service.list_batches()))
        self.assertEqual(2, self.service.get_batch(first["batch"]["batch_id"])["record_count"])

    def test_key_order_and_record_order_do_not_change_identity(self) -> None:
        records = [
            {"station_id": "S1", "observed_at": "2026-09-20T08:00:00Z",
             "vehicles": 1, "sessions": 1, "energy_kwh": 10.0},
            {"energy_kwh": 20.0, "sessions": 2, "vehicles": 2,
             "observed_at": "2026-09-20T09:00:00Z", "station_id": "S1"},
        ]
        shuffled = list(reversed(records))
        first = import_batch(self.service, "bureau", records)
        second = import_batch(self.service, "bureau", shuffled)
        self.assertEqual(first["batch"]["batch_id"], second["batch"]["batch_id"])
        self.assertFalse(second["created"])

    def test_different_content_creates_new_batch(self) -> None:
        first = import_batch(self.service, "bureau", hourly_records("S1", 20, {8: 100.0}))
        second = import_batch(self.service, "bureau", hourly_records("S1", 20, {8: 101.0}))
        third = import_batch(self.service, "other-source", hourly_records("S1", 20, {8: 100.0}))
        self.assertNotEqual(first["batch"]["batch_id"], second["batch"]["batch_id"])
        self.assertNotEqual(first["batch"]["batch_id"], third["batch"]["batch_id"])
        self.assertEqual(3, len(self.service.list_batches()))

    def test_duplicate_records_inside_one_batch_are_deduplicated(self) -> None:
        record = {"station_id": "S1", "observed_at": "2026-09-20T08:00:00Z",
                  "vehicles": 1, "sessions": 1, "energy_kwh": 10.0}
        result = import_batch(self.service, "bureau", [record, dict(record)])
        self.assertEqual(1, result["batch"]["record_count"])

    def test_correction_import_registers_correction_and_marks_downstream(self) -> None:
        mapping_id = make_mapping(self.service, {"S1": "AreaA"})
        param_id = make_params(self.service)
        base = import_batch(self.service, "bureau", hourly_records("S1", 20, {8: 100.0}))
        version = computed_version(
            self.service, batch_ids=[base["batch"]["batch_id"]],
            param_id=param_id, mapping_id=mapping_id)

        corrected = import_batch(
            self.service, "bureau", hourly_records("S1", 20, {8: 130.0}),
            corrects=base["batch"]["batch_id"], correction_reason="仪表校准")
        self.assertTrue(corrected["created"])
        correction = corrected["correction"]
        self.assertIsNotNone(correction)
        self.assertEqual([version["version_id"]], correction["impacted_versions"])
        self.assertTrue(self.service.get_version(version["version_id"])["stale"])

        # 重复导入同一更正文件：幂等，不产生第二条更正记录。
        again = import_batch(
            self.service, "bureau", hourly_records("S1", 20, {8: 130.0}),
            corrects=base["batch"]["batch_id"])
        self.assertFalse(again["created"])
        self.assertIsNone(again["correction"])
        self.assertEqual(1, len(self.service.repo.list_corrections()))


if __name__ == "__main__":
    unittest.main()
