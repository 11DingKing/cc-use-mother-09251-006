"""不可变版本：内容寻址、重复批次幂等、历史不可篡改。"""
from __future__ import annotations

import json
import os

from service_09251_006.errors import ConflictError, ValidationError
from tests.helpers import AppTestCase, flow_batch, mapping, standard_params


class ImmutabilityTests(AppTestCase):
    def test_duplicate_raw_batch_is_idempotent(self) -> None:
        doc = flow_batch("A01")
        first = self.app.versions.import_raw_batch(doc, created_by="zhang")
        # 键插入顺序不同不影响内容寻址
        shuffled = {"unit": "vehicles/h", **doc} if False else {
            "records": doc["records"], "source": doc["source"],
            "service_area": doc["service_area"],
        }
        second = self.app.versions.import_raw_batch(shuffled, created_by="li")
        self.assertEqual(first["version_id"], second["version_id"])
        self.assertFalse(second["created_new"])
        self.assertEqual(first["source_key"], "traffic_flow:A01")

    def test_different_content_gets_different_version(self) -> None:
        a = self.app.versions.import_raw_batch(flow_batch("A01"))
        b = self.app.versions.import_raw_batch(flow_batch("A02"))
        self.assertNotEqual(a["version_id"], b["version_id"])

    def test_parameters_and_mapping_dedup(self) -> None:
        p1 = self.app.versions.import_parameters(standard_params())
        p2 = self.app.versions.import_parameters(standard_params(growth_factor=1.2))
        p3 = self.app.versions.import_parameters(standard_params())
        self.assertNotEqual(p1["version_id"], p2["version_id"])
        self.assertEqual(p1["version_id"], p3["version_id"])
        m1 = self.app.versions.import_region_map(mapping(("A01", "R1", 1.0)))
        m2 = self.app.versions.import_region_map(mapping(("A01", "R1", 1.0)))
        self.assertEqual(m1["version_id"], m2["version_id"])

    def test_validation_rejects_bad_records(self) -> None:
        with self.assertRaises(ValidationError):
            self.app.versions.import_raw_batch({"source": "traffic_flow",
                                                "service_area": "A01", "records": []})
        bad = flow_batch("A01")
        bad["records"][1]["timestamp"] = "2026-09-22 09:00"  # 无时区
        with self.assertRaises(ValidationError):
            self.app.versions.import_raw_batch(bad)

    def test_object_file_is_write_once(self) -> None:
        r = self.app.versions.import_raw_batch(flow_batch("A01"))
        path = self.app.repo.objects.path_for(r["object_id"])
        with open(path, "rb") as fh:
            before = fh.read()
        # 直接改写磁盘文件 -> 读取即校验失败（防篡改）
        with open(path, "r+b") as fh:
            data = bytearray(before)
            data[30] = (data[30] + 1) % 256
            fh.seek(0)
            fh.write(data)
        from service_09251_006.errors import LineageError

        with self.assertRaises(LineageError):
            self.app.repo.load_object(r["version_id"])

    def test_correction_preserves_original(self) -> None:
        doc = flow_batch("A01")
        original = self.app.versions.import_raw_batch(doc)
        original_first = doc["records"][0]["value"]
        corrected_doc = flow_batch("A01")
        corrected_doc["records"][0]["value"] = 9999
        c = self.app.versions.correct_raw_batch(
            original["version_id"], corrected_doc, reason="设备误标")
        self.assertNotEqual(c["corrected_version_id"], original["version_id"])
        original_obj = self.app.repo.load_object(original["version_id"])
        self.assertEqual(original_obj["records"][0]["value"], original_first)
        row = self.app.repo.get_version_row(c["corrected_version_id"])
        self.assertEqual(row["corrects_version_id"], original["version_id"])

    def test_correction_same_content_rejected(self) -> None:
        original = self.app.versions.import_raw_batch(flow_batch("A01"))
        with self.assertRaises(ValidationError):
            self.app.versions.correct_raw_batch(
                original["version_id"], flow_batch("A01"))

    def test_correction_wrong_source_rejected(self) -> None:
        original = self.app.versions.import_raw_batch(flow_batch("A01"))
        with self.assertRaises(ValidationError):
            self.app.versions.correct_raw_batch(
                original["version_id"], flow_batch("A02"))

    def test_duplicate_correction_is_idempotent(self) -> None:
        original = self.app.versions.import_raw_batch(flow_batch("A01"))
        doc = flow_batch("A01")
        doc["records"][0]["value"] = 2000
        c1 = self.app.versions.correct_raw_batch(
            original["version_id"], doc, reason="r")
        c2 = self.app.versions.correct_raw_batch(
            original["version_id"], doc, reason="r")
        self.assertEqual(c1["correction_id"], c2["correction_id"])
        self.assertEqual(c1["corrected_version_id"], c2["corrected_version_id"])


if __name__ == "__main__":
    unittest.main()
