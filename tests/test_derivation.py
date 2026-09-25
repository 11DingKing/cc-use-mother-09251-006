"""并发派生：多线程从同一版本派生情景，序号与标识必须唯一。"""
from __future__ import annotations

import unittest
from concurrent.futures import ThreadPoolExecutor

from helpers import (
    ServiceTestCase,
    computed_version,
    hourly_records,
    import_batch,
    make_mapping,
    make_params,
)


class ConcurrentDerivationTests(ServiceTestCase):
    def setUp(self) -> None:
        super().setUp()
        mapping_id = make_mapping(self.service, {"S1": "AreaA"})
        self.param_id = make_params(self.service)
        batch = import_batch(self.service, "bureau", hourly_records("S1", 20, {8: 100.0}))
        self.root = computed_version(
            self.service, batch_ids=[batch["batch"]["batch_id"]],
            param_id=self.param_id, mapping_id=mapping_id)

    def test_concurrent_derives_get_unique_ids_and_seqs(self) -> None:
        total = 8

        def derive(index: int):
            return self.service.derive_version(
                self.root["version_id"], scenario=f"scenario-{index}")

        with ThreadPoolExecutor(max_workers=total) as pool:
            derived = list(pool.map(derive, range(total)))

        ids = [v["version_id"] for v in derived]
        seqs = sorted(v["seq"] for v in derived)
        self.assertEqual(total, len(set(ids)), "派生版本标识必须唯一")
        self.assertEqual(list(range(1, total + 1)), seqs, "兄弟序号必须连续无冲突")
        for version in derived:
            self.assertEqual(self.root["version_id"], version["parent_version_id"])
            self.assertEqual("DRAFT", version["status"])

    def test_derive_inherits_and_overrides_inputs(self) -> None:
        inherited = self.service.derive_version(self.root["version_id"], scenario="child")
        self.assertEqual(self.root["param_id"], inherited["param_id"])
        self.assertEqual(self.root["mapping_id"], inherited["mapping_id"])
        self.assertEqual(self.root["batch_ids"], inherited["batch_ids"])

        new_param = make_params(self.service, growth_factor=1.2)
        overridden = self.service.derive_version(
            self.root["version_id"], scenario="child-2", param_id=new_param)
        self.assertEqual(new_param, overridden["param_id"])
        self.assertEqual(self.root["batch_ids"], overridden["batch_ids"])

        lineage = self.service.verify_lineage(overridden["version_id"])
        self.assertTrue(lineage["ok"])
        self.assertEqual(2, len(lineage["chain"]))  # 子 → 根

    def test_derive_from_any_status_is_allowed(self) -> None:
        jobless = self.service.derive_version(self.root["version_id"], scenario="from-computed")
        self.assertEqual("DRAFT", jobless["status"])
        signed = self.service.sign_version(self.root["version_id"], signed_by="analyst")
        revoked = self.service.revoke_version(signed["version_id"],
                                              revoked_by="lead", reason="假设过期")
        from_revoked = self.service.derive_version(revoked["version_id"], scenario="retry")
        self.assertEqual("DRAFT", from_revoked["status"])


if __name__ == "__main__":
    unittest.main()
