"""跨日聚合：按自然日与小时归桶、峰值与置信区间、版本比较。"""
from __future__ import annotations

import math
import statistics
import unittest

from helpers import (
    ServiceTestCase,
    computed_version,
    hourly_records,
    import_batch,
    make_mapping,
    make_params,
    results_by_area,
)


def _three_day_batch() -> list[dict]:
    records = []
    # S1/S2 同属 AreaA；08 时三天相同（std=0），09 时第三天突增。
    for day, energy9 in ((20, 200.0), (21, 200.0), (22, 300.0)):
        records += hourly_records("S1", day, {8: 100.0, 9: energy9})
        records += hourly_records("S2", day, {8: 60.0, 9: 40.0})
    return records


class CrossDayAggregationTests(ServiceTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.mapping_id = make_mapping(self.service, {"S1": "AreaA", "S2": "AreaA"})
        self.param_id = make_params(self.service)
        batch = import_batch(self.service, "bureau", _three_day_batch())
        self.version = computed_version(
            self.service, batch_ids=[batch["batch"]["batch_id"]],
            param_id=self.param_id, mapping_id=self.mapping_id)
        self.rows = results_by_area(self.service, self.version["version_id"])["AreaA"]
        self.by_hour = {row["hour"]: row for row in self.rows}

    def test_daily_totals_grouped_by_hour_of_day(self) -> None:
        # 每天 08 时 = S1(100) + S2(60) = 160，三天一致。
        row8 = self.by_hour[8]
        self.assertEqual(3, row8["days"])
        self.assertAlmostEqual(160.0, row8["mean_kwh"], places=6)
        self.assertAlmostEqual(0.0, row8["std_kwh"], places=6)
        # 标准差为零时置信区间退化为点。
        self.assertAlmostEqual(row8["mean_kwh"], row8["ci_low"], places=6)
        self.assertAlmostEqual(row8["mean_kwh"], row8["ci_high"], places=6)

    def test_confidence_interval_matches_statistics(self) -> None:
        daily_9 = [240.0, 240.0, 340.0]  # S1(200/200/300) + S2(40)
        mean = statistics.fmean(daily_9)
        std = statistics.stdev(daily_9)
        z = statistics.NormalDist().inv_cdf(0.975)
        half = z * std / math.sqrt(3)
        row9 = self.by_hour[9]
        self.assertEqual(3, row9["days"])
        self.assertAlmostEqual(mean, row9["mean_kwh"], places=6)
        self.assertAlmostEqual(std, row9["std_kwh"], places=6)
        self.assertAlmostEqual(mean - half, row9["ci_low"], places=6)
        self.assertAlmostEqual(mean + half, row9["ci_high"], places=6)

    def test_peak_hour_flagged(self) -> None:
        peaks = [row for row in self.rows if row["is_peak"]]
        self.assertEqual(1, len(peaks))
        self.assertEqual(9, peaks[0]["hour"])

    def test_records_spanning_midnight_bucketed_by_own_timestamp(self) -> None:
        records = hourly_records("S1", 20, {23: 50.0})
        records.append({"station_id": "S1", "observed_at": "2026-09-21T00:30:00Z",
                        "vehicles": 1, "sessions": 1, "energy_kwh": 70.0})
        batch = import_batch(self.service, "night", records)
        version = computed_version(
            self.service, batch_ids=[batch["batch"]["batch_id"]],
            param_id=self.param_id, mapping_id=self.mapping_id)
        rows = results_by_area(self.service, version["version_id"])["AreaA"]
        by_hour = {row["hour"]: row for row in rows}
        # 23:00 与 00:30 分属不同小时、不同自然日，不得合并。
        self.assertEqual({0, 23}, set(by_hour))
        self.assertEqual(1, by_hour[0]["days"])
        self.assertAlmostEqual(70.0, by_hour[0]["mean_kwh"], places=6)

    def test_unmapped_stations_are_skipped(self) -> None:
        records = hourly_records("S1", 20, {8: 100.0})
        records += hourly_records("S9", 20, {8: 999.0})  # S9 不在映射中
        batch = import_batch(self.service, "mixed", records)
        version = computed_version(
            self.service, batch_ids=[batch["batch"]["batch_id"]],
            param_id=self.param_id, mapping_id=self.mapping_id)
        grouped = results_by_area(self.service, version["version_id"])
        self.assertEqual(["AreaA"], sorted(grouped))
        row8 = {r["hour"]: r for r in grouped["AreaA"]}[8]
        self.assertAlmostEqual(100.0, row8["mean_kwh"], places=6)

    def test_growth_factor_scales_mean_and_interval(self) -> None:
        param_150 = make_params(self.service, growth_factor=1.5)
        batch = import_batch(self.service, "bureau", _three_day_batch())
        scaled = computed_version(
            self.service, batch_ids=[batch["batch"]["batch_id"]],
            param_id=param_150, mapping_id=self.mapping_id, scenario="growth-150")
        scaled9 = {r["hour"]: r for r in
                   results_by_area(self.service, scaled["version_id"])["AreaA"]}[9]
        # 从未舍入的日合计统计量推期望，避免二次舍入误差。
        daily_9 = [240.0, 240.0, 340.0]
        mean = statistics.fmean(daily_9) * 1.5
        std = statistics.stdev(daily_9) * 1.5
        z = statistics.NormalDist().inv_cdf(0.975)
        half = z * statistics.stdev(daily_9) / math.sqrt(3) * 1.5
        self.assertAlmostEqual(mean, scaled9["mean_kwh"], places=6)
        self.assertAlmostEqual(std, scaled9["std_kwh"], places=6)
        self.assertAlmostEqual(mean - half, scaled9["ci_low"], places=6)
        self.assertAlmostEqual(mean + half, scaled9["ci_high"], places=6)

    def test_compare_versions_reports_peak_and_interval_delta(self) -> None:
        param_150 = make_params(self.service, growth_factor=1.5)
        batch = import_batch(self.service, "bureau", _three_day_batch())
        derived = self.service.derive_version(
            self.version["version_id"], scenario="growth-150", param_id=param_150)
        job = self.service.start_compute(derived["version_id"])["job"]
        self.service.run_compute(job["job_id"])

        report = self.service.compare_versions(
            self.version["version_id"], derived["version_id"])
        self.assertEqual(1, len(report["areas"]))
        area = report["areas"][0]
        self.assertEqual("AreaA", area["service_area"])
        self.assertEqual(9, area["a"]["peak_hour"])
        self.assertEqual(9, area["b"]["peak_hour"])
        self.assertAlmostEqual(area["a"]["mean_kwh"] * 0.5,
                               area["delta_mean_kwh"], places=4)
        width_a = area["a"]["ci_high"] - area["a"]["ci_low"]
        self.assertAlmostEqual(width_a * 0.5, area["delta_ci_width_kwh"], places=4)


if __name__ == "__main__":
    unittest.main()
