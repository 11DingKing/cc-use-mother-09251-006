"""离线管理命令：导入、计算、签署、撤销、谱系核验全链路。"""
from __future__ import annotations

import contextlib
import io
import json
import os
import tempfile
import unittest

from service_09251_006 import manage


def run_cli(*argv: str):
    """执行 manage.main，返回 (退出码, 标准输出JSON)。"""
    buffer = io.StringIO()
    with contextlib.redirect_stdout(buffer):
        code = manage.main(list(argv))
    output = buffer.getvalue().strip()
    return code, json.loads(output) if output else None


class CliTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory(prefix="s09251_006_cli_")
        self.addCleanup(self._tmp.cleanup)
        self.db = os.path.join(self._tmp.name, "app.db")

    def _cli(self, *argv: str):
        return run_cli("--db", self.db, *argv)

    def test_offline_command_chain(self) -> None:
        # 导入
        batch_file = os.path.join(self._tmp.name, "batch.json")
        with open(batch_file, "w", encoding="utf-8") as fh:
            json.dump({
                "source": "bureau",
                "records": [
                    {"station_id": "S1", "observed_at": "2026-09-20T08:00:00Z",
                     "vehicles": 10, "sessions": 5, "energy_kwh": 100.0},
                    {"station_id": "S1", "observed_at": "2026-09-21T08:00:00Z",
                     "vehicles": 12, "sessions": 6, "energy_kwh": 120.0},
                ],
            }, fh)
        code, imported = self._cli("import-batch", batch_file)
        self.assertEqual(0, code)
        self.assertTrue(imported["created"])
        batch_id = imported["batch"]["batch_id"]

        # 重复导入幂等
        code, again = self._cli("import-batch", batch_file)
        self.assertEqual(0, code)
        self.assertFalse(again["created"])

        code, params = self._cli("create-params", '{"growth_factor": 1.1}')
        param_id = params["parameter_set"]["param_id"]
        code, mapping = self._cli("create-mapping", '{"S1": "AreaA"}')
        mapping_id = mapping["region_mapping"]["mapping_id"]

        code, created = self._cli(
            "create-version", "--batches", batch_id,
            "--params", param_id, "--mapping", mapping_id, "--scenario", "base")
        self.assertEqual(0, code)
        version_id = created["version"]["version_id"]

        # 计算（同步执行，任务落库）
        code, computed = self._cli("compute", version_id)
        self.assertEqual(0, code)
        self.assertEqual("DONE", computed["job"]["status"])

        code, results = self._cli("show-results", version_id)
        self.assertEqual(1, len(results["rows"]))
        self.assertAlmostEqual(121.0, results["rows"][0]["mean_kwh"], places=6)

        # 签署 → 决策 → 谱系核验
        code, signed = self._cli("sign", version_id, "--by", "analyst")
        self.assertEqual("SIGNED", signed["version"]["status"])
        code, decision = self._cli(
            "decide", version_id, "--area", "AreaA",
            "--action", "deploy-mobile-chargers", "--units", "2", "--by", "ops")
        self.assertEqual(0, code)
        self.assertEqual(version_id, decision["decision"]["version_id"])

        code, lineage = self._cli("verify-lineage", version_id)
        self.assertEqual(0, code)
        self.assertTrue(lineage["ok"])

        # 派生 → 比较 → 撤销
        code, derived = self._cli("derive", version_id, "--scenario", "high")
        high_id = derived["version"]["version_id"]
        code, _ = self._cli("compute", high_id)
        code, report = self._cli("compare", version_id, high_id)
        self.assertEqual(0, code)
        self.assertEqual("AreaA", report["areas"][0]["service_area"])

        code, revoked = self._cli("revoke", version_id,
                                  "--by", "lead", "--reason", "假设更新")
        self.assertEqual("REVOKED", revoked["version"]["status"])

    def test_cli_reports_domain_errors(self) -> None:
        code, error = self._cli("sign", "ver_missing", "--by", "analyst")
        self.assertEqual(2, code)
        self.assertEqual("not_found", error["error"]["code"])


if __name__ == "__main__":
    unittest.main()
