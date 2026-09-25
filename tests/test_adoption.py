"""签署与撤销：追加事件、决策关联、状态机约束。"""
from __future__ import annotations

from service_09251_006.errors import InvalidStateError, ValidationError
from tests.helpers import AppTestCase, seed_complete_scenario


def _forecast(app, sid: str) -> str:
    f = app.forecasts
    return f.run_job(f.submit_forecast(sid)["job_id"])["result"]["forecast_version_id"]


class AdoptionTests(AppTestCase):
    def setUp(self) -> None:
        super().setUp()
        sid, *_ = seed_complete_scenario(self.app, ("A01",))
        self.sid = sid
        self.fvid = _forecast(self.app, sid)

    def test_adopt_with_decision_links_objects(self) -> None:
        v = self.app.versions
        decision = {"deployments": [
            {"deploy_region": "R1", "mobile_units": 3}],
            "scheduled_for": "2026-09-26"}
        out = v.adopt(self.fvid, decision, actor="li", reason="节前定稿")
        status = v.adoption_status(self.fvid)
        self.assertEqual(status["status"], "adopted")
        self.assertEqual(status["decision_version_id"], out["decision_version_id"])
        decision_obj = self.app.repo.load_object(out["decision_version_id"])
        self.assertEqual(decision_obj["deployments"][0]["mobile_units"], 3)
        # 预测对象仍可独立读取，且被决策版本引用（通过事件关联）
        self.app.repo.load_object(self.fvid)

    def test_cannot_adopt_twice(self) -> None:
        v = self.app.versions
        v.adopt(self.fvid, None, actor="li")
        with self.assertRaises(InvalidStateError):
            v.adopt(self.fvid, None, actor="zhang")

    def test_revoke_then_readopt_keeps_full_history(self) -> None:
        v = self.app.versions
        v.adopt(self.fvid, {"deployments": [
            {"deploy_region": "R1", "mobile_units": 3}]}, actor="li")
        v.revoke(self.fvid, actor="li", reason="交通团队修订")
        self.assertEqual(v.adoption_status(self.fvid)["status"], "revoked")
        v.adopt(self.fvid, None, actor="li")
        self.assertEqual(v.adoption_status(self.fvid)["status"], "adopted")
        actions = [e["action"] for e in v.adoption_status(self.fvid)["events"]]
        self.assertEqual(actions, ["adopted", "revoked", "adopted"])

    def test_revoke_without_adoption_rejected(self) -> None:
        with self.assertRaises(InvalidStateError):
            self.app.versions.revoke(self.fvid, actor="li")

    def test_invalid_decision_rejected(self) -> None:
        with self.assertRaises(ValidationError):
            self.app.versions.adopt(
                self.fvid, {"deployments": [
                    {"deploy_region": "R1", "mobile_units": -1}]})

    def test_adopted_forecast_recomputed_after_param_change_is_new_version(self) -> None:
        """采用后参数修订产生新预测版本；旧版本与采用记录原样保留。"""
        v = self.app.versions
        v.adopt(self.fvid, None, actor="li")
        from tests.helpers import standard_params
        new_p = v.import_parameters(standard_params(growth_factor=1.2))["version_id"]
        v.set_parameters(self.sid, new_p)
        new_f = _forecast(self.app, self.sid)
        self.assertNotEqual(self.fvid, new_f)
        # 旧预测仍是 adopted（决策不被静默迁移到新版本）
        self.assertEqual(v.adoption_status(self.fvid)["status"], "adopted")
        self.assertEqual(v.adoption_status(new_f)["status"], "never_adopted")


if __name__ == "__main__":
    unittest.main()
