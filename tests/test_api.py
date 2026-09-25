"""HTTP API 端到端：真实服务器上走通完整业务流。"""
from __future__ import annotations

import http.client
import json
import threading
import unittest

from helpers import ServiceTestCase, hourly_records
from service_09251_006.api import make_server


class ApiTests(ServiceTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.server = make_server(self.service, "127.0.0.1", 0)
        self.port = self.server.server_address[1]
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.addCleanup(self.server.shutdown)
        self.addCleanup(self.server.server_close)

    def _request(self, method: str, path: str, body=None):
        conn = http.client.HTTPConnection("127.0.0.1", self.port, timeout=10)
        payload = json.dumps(body).encode("utf-8") if body is not None else None
        headers = {"Content-Type": "application/json"} if body is not None else {}
        conn.request(method, path, body=payload, headers=headers)
        response = conn.getresponse()
        data = json.loads(response.read().decode("utf-8"))
        conn.close()
        return response.status, data

    def _post(self, path: str, body: dict):
        return self._request("POST", path, body)

    def _get(self, path: str):
        return self._request("GET", path)

    def test_full_forecast_governance_flow(self) -> None:
        status, health = self._get("/api/health")
        self.assertEqual(200, status)
        self.assertEqual("ok", health["status"])

        # 导入批次（重复导入幂等）
        payload = {"source": "bureau", "records":
                   hourly_records("S1", 20, {8: 100.0, 9: 200.0}) +
                   hourly_records("S1", 21, {8: 100.0, 9: 220.0})}
        status, batch1 = self._post("/api/batches", payload)
        self.assertEqual(200, status)
        self.assertTrue(batch1["created"])
        status, again = self._post("/api/batches", payload)
        self.assertFalse(again["created"])
        batch_id = batch1["batch"]["batch_id"]

        status, params = self._post("/api/parameter-sets", {"growth_factor": 1.0})
        param_id = params["parameter_set"]["param_id"]
        status, mapping = self._post("/api/region-mappings", {"entries": {"S1": "AreaA"}})
        mapping_id = mapping["region_mapping"]["mapping_id"]

        # 版本 → 计算（同步等待）→ 结果
        status, created = self._post("/api/versions", {
            "batch_ids": [batch_id], "param_id": param_id,
            "mapping_id": mapping_id, "scenario": "base"})
        self.assertEqual(200, status)
        version_id = created["version"]["version_id"]

        status, computed = self._post(f"/api/versions/{version_id}/compute", {"wait": True})
        self.assertEqual(202, status)
        self.assertEqual("DONE", computed["job"]["status"])

        status, results = self._get(f"/api/versions/{version_id}/results")
        self.assertEqual(200, status)
        self.assertEqual(2, len(results["rows"]))

        # 签署 → 调度决策关联
        status, signed = self._post(f"/api/versions/{version_id}/sign",
                                    {"signed_by": "analyst"})
        self.assertEqual("SIGNED", signed["version"]["status"])
        status, decision = self._post("/api/decisions", {
            "version_id": version_id, "service_area": "AreaA",
            "action": "deploy-mobile-chargers", "units": 2, "decided_by": "ops"})
        self.assertEqual(200, status)
        status, decisions = self._get(f"/api/versions/{version_id}/decisions")
        self.assertEqual(1, len(decisions["decisions"]))

        # 派生高情景并比较峰值
        status, derived = self._post(f"/api/versions/{version_id}/derive",
                                     {"scenario": "high"})
        high_id = derived["version"]["version_id"]
        status, _ = self._post(f"/api/versions/{high_id}/compute", {"wait": True})
        status, report = self._get(f"/api/compare?a={version_id}&b={high_id}")
        self.assertEqual(200, status)
        self.assertEqual("AreaA", report["areas"][0]["service_area"])

        # 谱系核验
        status, lineage = self._get(f"/api/versions/{high_id}/lineage")
        self.assertEqual(200, status)
        self.assertTrue(lineage["ok"])
        self.assertEqual(2, len(lineage["chain"]))

        # 原始数据更正：历史不动，下游被标记
        status, corrected = self._post("/api/batches", {
            "source": "bureau", "corrects": batch_id,
            "correction_reason": "仪表校准",
            "records": hourly_records("S1", 20, {8: 130.0, 9: 200.0}) +
                       hourly_records("S1", 21, {8: 130.0, 9: 220.0})})
        self.assertEqual(200, status)
        impacted = corrected["correction"]["impacted_versions"]
        self.assertIn(version_id, impacted)
        self.assertIn(high_id, impacted)

        # 受影响版本禁止再被采用
        status, rejected = self._post("/api/decisions", {
            "version_id": version_id, "service_area": "AreaA",
            "action": "deploy", "units": 1, "decided_by": "ops"})
        self.assertEqual(409, status)
        self.assertEqual("version_stale", rejected["error"]["code"])

        # 撤销
        status, revoked = self._post(f"/api/versions/{version_id}/revoke",
                                     {"revoked_by": "lead", "reason": "数据已更正"})
        self.assertEqual("REVOKED", revoked["version"]["status"])

    def test_error_shapes(self) -> None:
        status, body = self._get("/api/versions/ver_missing")
        self.assertEqual(404, status)
        self.assertEqual("not_found", body["error"]["code"])

        status, body = self._post("/api/batches", {"source": "", "records": []})
        self.assertEqual(400, status)
        self.assertEqual("validation", body["error"]["code"])

        status, body = self._get("/api/no-such-route")
        self.assertEqual(404, status)

        status, body = self._post("/api/versions/ver_x/compute", {})
        self.assertEqual(404, status)


if __name__ == "__main__":
    unittest.main()
