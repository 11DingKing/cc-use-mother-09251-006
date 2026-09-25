"""HTTP API 端到端：导入、派生、计算、签署、比较、核验、错误码。"""
from __future__ import annotations

import json
import threading
import urllib.error
import urllib.request
from http.server import ThreadingHTTPServer

from service_09251_006.api.http_app import make_handler
from tests.helpers import (
    AppTestCase, demand_batch, flow_batch, mapping, standard_params,
)


class ApiHarness:
    def __init__(self, app) -> None:
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), make_handler(app))
        self.port = self.server.server_address[1]
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def stop(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)

    def request(self, method: str, path: str, body=None, actor=None):
        url = f"http://127.0.0.1:{self.port}{path}"
        data = json.dumps(body).encode() if body is not None else None
        headers = {"Content-Type": "application/json"}
        if actor:
            headers["X-Actor"] = actor
        req = urllib.request.Request(url, data=data, headers=headers, method=method)
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                return resp.status, json.loads(resp.read().decode())
        except urllib.error.HTTPError as exc:
            return exc.code, json.loads(exc.read().decode())


class HttpApiTests(AppTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.api = ApiHarness(self.app)
        self.addCleanup(self.api.stop)

    def _post(self, path, body=None, actor=None):
        return self.api.request("POST", path, body or {}, actor)

    def _get(self, path):
        return self.api.request("GET", path)

    def test_full_workflow_over_http(self) -> None:
        # 导入
        s, flow = self._post("/api/inputs/raw", flow_batch("A01"), actor="zhang")
        self.assertEqual(s, 200)
        flow_vid = flow["version_id"]
        s, dem = self._post("/api/inputs/raw", demand_batch("A01"))
        dem_vid = dem["version_id"]
        # 重复批次
        s, dup = self._post("/api/inputs/raw", flow_batch("A01"))
        self.assertFalse(dup["created_new"])
        s, params = self._post("/api/parameters", standard_params())
        pvid = params["version_id"]
        s, mp = self._post("/api/region-maps", mapping(("A01", "R1", 1.0)))
        mvid = mp["version_id"]

        # 情景与派生
        s, scn = self._post("/api/scenarios",
                            {"parameters_version_id": pvid, "mapping_version_id": mvid})
        self.assertEqual(s, 200)
        sid = scn["scenario_id"]
        s, c1 = self._post(f"/api/scenarios/{sid}/derive",
                           {"action": "add_input", "input_version_id": flow_vid})
        self.assertEqual(c1["seq"], 1)
        self._post(f"/api/scenarios/{sid}/derive",
                   {"action": "add_input", "input_version_id": dem_vid})

        # 预测（同步等待）
        s, submitted = self._post(f"/api/scenarios/{sid}/forecasts", {"wait": True})
        self.assertEqual(s, 200)
        fvid = submitted["run"]["result"]["forecast_version_id"]

        # 读取预测、比较、核验
        s, fc = self._get(f"/api/forecasts/{fvid}")
        self.assertEqual(s, 200)
        self.assertIn("A01", fc["payload"]["service_areas"])
        s, verify = self._get(f"/api/forecasts/{fvid}/verify")
        self.assertTrue(verify["ok"])

        # 采用 + 决策 + 撤销
        s, adopted = self._post(f"/api/forecasts/{fvid}/adopt", {
            "decision": {"deployments": [{"deploy_region": "R1", "mobile_units": 4}]},
            "actor": "li"})
        self.assertEqual(s, 200)
        self.assertIsNotNone(adopted["decision_version_id"])
        s, err = self._post(f"/api/forecasts/{fvid}/adopt", {})
        self.assertEqual(s, 409)
        s, revoked = self._post(f"/api/forecasts/{fvid}/revoke",
                                {"reason": "修订", "actor": "li"})
        self.assertEqual(s, 200)
        s, hist = self._get(f"/api/forecasts/{fvid}/adoption")
        self.assertEqual([e["action"] for e in hist["events"]],
                         ["adopted", "revoked"])

    def test_compare_endpoint_and_validation_errors(self) -> None:
        s, flow = self._post("/api/inputs/raw", flow_batch("A01"))
        s, dem = self._post("/api/inputs/raw", demand_batch("A01"))
        s, p1 = self._post("/api/parameters", standard_params())
        s, mp = self._post("/api/region-maps", mapping(("A01", "R1", 1.0)))
        s, scn = self._post("/api/scenarios",
                            {"parameters_version_id": p1["version_id"],
                             "mapping_version_id": mp["version_id"]})
        sid = scn["scenario_id"]
        self._post(f"/api/scenarios/{sid}/derive",
                   {"action": "add_input", "input_version_id": flow["version_id"]})
        self._post(f"/api/scenarios/{sid}/derive",
                   {"action": "add_input", "input_version_id": dem["version_id"]})
        s, r1 = self._post(f"/api/scenarios/{sid}/forecasts", {"wait": True})
        f1 = r1["run"]["result"]["forecast_version_id"]

        s, p2 = self._post("/api/parameters", standard_params(growth_factor=1.4))
        self._post(f"/api/scenarios/{sid}/derive",
                   {"action": "set_parameters",
                    "parameters_version_id": p2["version_id"]})
        s, r2 = self._post(f"/api/scenarios/{sid}/forecasts", {"wait": True})
        f2 = r2["run"]["result"]["forecast_version_id"]
        s, cmp_ = self._get(f"/api/forecasts/compare?left={f1}&right={f2}")
        self.assertEqual(s, 200)
        self.assertGreater(
            cmp_["service_areas"]["A01"]["delta_point"], 0)

        # 404 / 422 / 409 错误码
        s, notfound = self._get("/api/versions/v999999")
        self.assertEqual(s, 404)
        s, bad = self._post("/api/inputs/raw",
                            {"source": "nope", "service_area": "A01", "records": []})
        self.assertEqual(s, 422)
        s, badjson = self.api.request(
            "POST", "/api/inputs/raw", body=None)
        # 空 body -> {} -> 校验失败
        self.assertEqual(s, 422)

    def test_async_job_run_endpoint(self) -> None:
        s, flow = self._post("/api/inputs/raw", flow_batch("A01"))
        s, dem = self._post("/api/inputs/raw", demand_batch("A01"))
        s, p1 = self._post("/api/parameters", standard_params())
        s, mp = self._post("/api/region-maps", mapping(("A01", "R1", 1.0)))
        s, scn = self._post("/api/scenarios",
                            {"parameters_version_id": p1["version_id"],
                             "mapping_version_id": mp["version_id"]})
        sid = scn["scenario_id"]
        self._post(f"/api/scenarios/{sid}/derive",
                   {"action": "add_input", "input_version_id": flow["version_id"]})
        self._post(f"/api/scenarios/{sid}/derive",
                   {"action": "add_input", "input_version_id": dem["version_id"]})
        s, queued = self._post(f"/api/scenarios/{sid}/forecasts", {})
        jid = queued["job_id"]
        s, jst = self._get(f"/api/jobs/{jid}")
        self.assertEqual(jst["status"], "queued")
        s, ran = self._post(f"/api/jobs/{jid}/run", {})
        self.assertEqual(ran["status"], "succeeded")
        s, jst2 = self._get(f"/api/jobs/{jid}")
        self.assertEqual(jst2["status"], "succeeded")

    def test_correction_and_invalidation_visible_over_http(self) -> None:
        s, flow = self._post("/api/inputs/raw", flow_batch("A01"))
        flow_vid = flow["version_id"]
        self._post("/api/inputs/raw", demand_batch("A01"))
        s, p1 = self._post("/api/parameters", standard_params())
        s, mp = self._post("/api/region-maps", mapping(("A01", "R1", 1.0)))
        dem_vid = self._get("/api/versions?kind=raw_input")[1]["versions"]
        dem_vid = [v["id"] for v in dem_vid if v["source_key"] == "charging_demand:A01"][0]
        s, scn = self._post("/api/scenarios",
                            {"parameters_version_id": p1["version_id"],
                             "mapping_version_id": mp["version_id"]})
        sid = scn["scenario_id"]
        self._post(f"/api/scenarios/{sid}/derive",
                   {"action": "add_input", "input_version_id": flow_vid})
        self._post(f"/api/scenarios/{sid}/derive",
                   {"action": "add_input", "input_version_id": dem_vid})
        s, r = self._post(f"/api/scenarios/{sid}/forecasts", {"wait": True})
        fvid = r["run"]["result"]["forecast_version_id"]

        corrected = flow_batch("A01")
        corrected["records"][0]["value"] = 7777
        s, cor = self._post(f"/api/versions/{flow_vid}/corrections",
                            corrected, actor="zhang")
        self.assertEqual(s, 200)
        self.assertEqual(cor["affected_forecasts"], [fvid])
        s, inv = self._get("/api/invalidations")
        self.assertEqual(inv["invalidations"][0]["status"], "affected")
        s, verify = self._get(f"/api/forecasts/{fvid}/verify")
        self.assertFalse(verify["ok"])


if __name__ == "__main__":
    unittest.main()
