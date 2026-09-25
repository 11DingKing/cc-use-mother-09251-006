"""生命周期：签署、撤销与调度决策关联的规则。"""
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
from service_09251_006.domain import ConflictError, NotSignedError, ValidationError


class LifecycleTests(ServiceTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.mapping_id = make_mapping(self.service, {"S1": "AreaA"})
        self.param_id = make_params(self.service)
        batch = import_batch(self.service, "bureau", hourly_records("S1", 20, {8: 100.0}))
        self.batch_id = batch["batch"]["batch_id"]

    def _computed(self, scenario="base") -> dict:
        return computed_version(
            self.service, batch_ids=[self.batch_id], param_id=self.param_id,
            mapping_id=self.mapping_id, scenario=scenario)

    def test_sign_requires_computed_status(self) -> None:
        draft = self.service.create_version(
            batch_ids=[self.batch_id], param_id=self.param_id,
            mapping_id=self.mapping_id, scenario="draft")
        with self.assertRaises(ConflictError):
            self.service.sign_version(draft["version_id"], signed_by="analyst")

    def test_sign_then_double_sign_rejected(self) -> None:
        version = self._computed()
        signed = self.service.sign_version(version["version_id"], signed_by="analyst")
        self.assertEqual("SIGNED", signed["status"])
        self.assertEqual("analyst", signed["signed_by"])
        self.assertTrue(signed["signed_at"])
        with self.assertRaises(ConflictError):
            self.service.sign_version(version["version_id"], signed_by="analyst")

    def test_decision_requires_signed_version(self) -> None:
        version = self._computed()
        with self.assertRaises(NotSignedError):
            self.service.add_decision(
                version["version_id"], service_area="AreaA",
                action="deploy", units=1, decided_by="ops")

    def test_decision_links_adopted_forecast(self) -> None:
        version = self._computed()
        self.service.sign_version(version["version_id"], signed_by="analyst")
        decision = self.service.add_decision(
            version["version_id"], service_area="AreaA",
            action="deploy-mobile-chargers", units=3,
            decided_by="ops-lead", note="国庆保障")
        self.assertEqual(version["version_id"], decision["version_id"])
        self.assertEqual("AreaA", decision["service_area"])
        decisions = self.service.list_decisions(version["version_id"])
        self.assertEqual(1, len(decisions))
        self.assertEqual(decision["decision_id"], decisions[0]["decision_id"])

    def test_decision_validates_area_and_units(self) -> None:
        version = self._computed()
        self.service.sign_version(version["version_id"], signed_by="analyst")
        with self.assertRaises(ValidationError):
            self.service.add_decision(
                version["version_id"], service_area="AreaZ",
                action="deploy", units=1, decided_by="ops")
        with self.assertRaises(ValidationError):
            self.service.add_decision(
                version["version_id"], service_area="AreaA",
                action="deploy", units=0, decided_by="ops")

    def test_revoke_is_terminal_and_keeps_history(self) -> None:
        version = self._computed()
        self.service.sign_version(version["version_id"], signed_by="analyst")
        self.service.add_decision(
            version["version_id"], service_area="AreaA",
            action="deploy", units=1, decided_by="ops")
        revoked = self.service.revoke_version(
            version["version_id"], revoked_by="lead", reason="假设被新政策取代")
        self.assertEqual("REVOKED", revoked["status"])
        self.assertEqual("假设被新政策取代", revoked["revoke_reason"])
        # 历史决策保留可查，但撤销的版本不再接受新决策与新签署。
        self.assertEqual(1, len(self.service.list_decisions(version["version_id"])))
        with self.assertRaises(ConflictError):
            self.service.add_decision(
                version["version_id"], service_area="AreaA",
                action="deploy", units=1, decided_by="ops")
        with self.assertRaises(ConflictError):
            self.service.sign_version(version["version_id"], signed_by="analyst")
        with self.assertRaises(ConflictError):
            self.service.revoke_version(
                version["version_id"], revoked_by="lead", reason="再次撤销")

    def test_revoke_requires_reason(self) -> None:
        version = self._computed()
        with self.assertRaises(ValidationError):
            self.service.revoke_version(
                version["version_id"], revoked_by="lead", reason="")

    def test_lineage_verification_passes_for_signed_version(self) -> None:
        version = self._computed()
        self.service.sign_version(version["version_id"], signed_by="analyst")
        report = self.service.verify_lineage(version["version_id"])
        self.assertTrue(report["ok"])
        self.assertEqual(version["version_id"], report["version_id"])
        self.assertTrue(all(c["ok"] for c in report["checks"]))


if __name__ == "__main__":
    unittest.main()
