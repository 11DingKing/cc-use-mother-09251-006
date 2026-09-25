"""预测计算：跨日聚合、峰值与置信区间、确定性复算、版本比较。"""
from __future__ import annotations

import math

from service_09251_006.forecast import Forecaster, inverse_normal_cdf
from tests.helpers import (
    AppTestCase, demand_batch, flow_batch, mapping, seed_complete_scenario,
    standard_params,
)


class ForecastMathTests(AppTestCase):
    def test_inverse_normal_cdf_known_quantiles(self) -> None:
        self.assertAlmostEqual(inverse_normal_cdf(0.975), 1.959964, places=4)
        self.assertAlmostEqual(inverse_normal_cdf(0.5), 0.0, places=6)
        self.assertAlmostEqual(inverse_normal_cdf(0.025), -1.959964, places=4)

    def test_engine_deterministic(self) -> None:
        sid, _p, _m, inputs, _ = seed_complete_scenario(self.app, ("A01",))
        f1 = self._run(sid)
        f2 = self._run(sid)
        self.assertEqual(f1, f2)

    def _run(self, sid: str) -> str:
        f = self.app.forecasts
        j = f.submit_forecast(sid)["job_id"]
        return f.run_job(j)["result"]["forecast_version_id"]

    def test_cross_day_aggregation_and_peak_ci(self) -> None:
        sid, *_ = seed_complete_scenario(self.app, ("A01",))
        fvid = self._run(sid)
        fc = self.app.repo.get_forecast(fvid)
        area = fc["payload"]["service_areas"]["A01"]
        peak = area["peak"]
        # 24 小时预测、每小时一点
        self.assertEqual(len(area["demand_series"]), 24)
        # 峰值点估计落在置信区间内
        self.assertLessEqual(peak["ci_low"], peak["point"])
        self.assertGreaterEqual(peak["ci_high"], peak["point"])
        # 峰值确实是曲线最大点
        max_point = max(r["point"] for r in area["demand_series"])
        self.assertEqual(peak["point"], max_point)
        # 跨日聚合：样本天数被记录
        self.assertEqual(area["sample_days"], 3)
        # 晚高峰 18:00（UTC 10:00）为峰；增长因子 1.1 生效
        evening_row = next(r for r in area["sources"]["traffic_flow"]["series"]
                           if r["timestamp"].endswith("T10:00:00+00:00"))
        self.assertAlmostEqual(evening_row["point"], 1800 * 1.1 * 0.1, places=4)

    def test_confidence_interval_width_scales_with_level(self) -> None:
        v, f = self.app.versions, self.app.forecasts
        for a in ("A01",):
            v.import_raw_batch(flow_batch(a))
        p90 = v.import_parameters(standard_params(confidence_level=0.90))["version_id"]
        p99 = v.import_parameters(standard_params(confidence_level=0.99))["version_id"]
        mid = v.import_region_map(mapping(("A01", "R1", 1.0)))["version_id"]
        inp = v.import_raw_batch(demand_batch("A01"))["version_id"]
        flow = v.import_raw_batch(flow_batch("A01"))["version_id"]

        def forecast_with(pid: str) -> dict:
            sid = v.create_scenario(parameters_version_id=pid, mapping_version_id=mid)["scenario_id"]
            v.add_input(sid, inp); v.add_input(sid, flow)
            j = f.submit_forecast(sid)["job_id"]
            fvid = f.run_job(j)["result"]["forecast_version_id"]
            return self.app.repo.get_forecast(fvid)["payload"]["service_areas"]["A01"]["peak"]

        w90 = forecast_with(p90)
        w99 = forecast_with(p99)
        width90 = w90["ci_high"] - w90["ci_low"]
        width99 = w99["ci_high"] - w99["ci_low"]
        self.assertGreater(width99, width90)

    def test_region_aggregation_weights(self) -> None:
        v, f = self.app.versions, self.app.forecasts
        v.import_raw_batch(flow_batch("A01"))
        v.import_raw_batch(flow_batch("A02"))
        pid = v.import_parameters(standard_params())["version_id"]
        mid = v.import_region_map(mapping(("A01", "R1", 1.0), ("A02", "R1", 0.5)))["version_id"]
        sid = v.create_scenario(parameters_version_id=pid, mapping_version_id=mid)["scenario_id"]
        for doc in (flow_batch("A01"), flow_batch("A02")):
            v.add_input(sid, v.import_raw_batch(doc)["version_id"])
        j = f.submit_forecast(sid)["job_id"]
        fvid = f.run_job(j)["result"]["forecast_version_id"]
        region = self.app.repo.get_forecast(fvid)["payload"]["deploy_regions"]["R1"]
        self.assertEqual(set(region["service_areas"]), {"A01", "A02"})

    def test_compare_forecasts_reports_peak_deltas(self) -> None:
        sid, *_ = seed_complete_scenario(self.app, ("A01",))
        f1 = self._run(sid)
        new_pid = self.app.versions.import_parameters(
            standard_params(growth_factor=1.3))["version_id"]
        self.app.versions.set_parameters(sid, new_pid)
        f2 = self._run(sid)
        cmp_ = self.app.versions.compare_forecasts(f1, f2)
        self.assertTrue(cmp_["parameters_changed"])
        self.assertFalse(cmp_["mapping_changed"])
        area = cmp_["service_areas"]["A01"]
        self.assertGreater(area["delta_point"], 0.0)


if __name__ == "__main__":
    unittest.main()
