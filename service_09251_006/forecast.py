"""确定性预测引擎：跨日聚合 -> 分时曲线 -> 服务区峰值与置信区间。

结果完全由输入观测、参数、映射的内容决定，不使用墙钟或随机数，因此
同一组版本重复计算必然得到同一内容对象（幂等、可复算）。

方法（可解释，便于向执行端说明）：
1. 每条观测按 *UTC 日 + 日内分时桶* 归类，跨多日聚合得到同日各桶样本；
2. 桶预测 = 样本均值 × growth_factor；标准误 = 样本标准差 / sqrt(n)；
3. 预测锚点取观测最晚时刻之后的下一个 UTC 零点，horizon 内每个预测
   时刻按其日内桶取值，置信区间为 点估计 ± z(confidence) × 标准误；
4. 服务区峰值 = horizon 内需求曲线最高点，并给出其置信区间与保守上下界；
5. 部署区域峰值按区域映射的权重聚合各服务区曲线。
"""
from __future__ import annotations

import math
from datetime import datetime, timedelta, timezone
from typing import Any

# 数值统一保留 6 位小数，避免浮点尾数破坏内容寻址稳定性。
NDIGITS = 6

# 引擎版本进入预测指纹：算法变更后旧预测不会被误认为可由新引擎复算。
ENGINE_VERSION = "engine-2026-09-25.1"


def _r(x: float) -> float:
    if x < 0 and x > -1e-9:
        x = 0.0
    return round(float(x), NDIGITS)


def inverse_normal_cdf(p: float) -> float:
    """标准正态分布分位数（Acklam 近似算法），确定性无第三方依赖。"""
    if not 0.0 < p < 1.0:
        raise ValueError("p 必须在 (0,1) 内")
    a = [
        -3.969683028665376e01, 2.209460984245205e02,
        -2.759285104469687e02, 1.383577518672690e02,
        -3.066479806614716e01, 2.506628277459239e00,
    ]
    b = [
        -5.447609879822406e01, 1.615858368580409e02,
        -1.556989798598866e02, 6.680131188771972e01,
        -1.328068155288572e01,
    ]
    c = [
        -7.784894002430293e-03, -3.223964580411365e-01,
        -2.400758277161838e00, -2.549732539343734e00,
        4.374664141464968e00, 2.938163982698783e00,
    ]
    d = [
        7.784695709041462e-03, 3.224671290700398e-01,
        2.445134137142996e00, 3.754408661907416e00,
    ]
    plow, phigh = 0.02425, 1 - 0.02425
    if p < plow:
        q = math.sqrt(-2 * math.log(p))
        return (((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5]) / \
               ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1)
    if p <= phigh:
        q = p - 0.5
        r2 = q * q
        return (((((a[0] * r2 + a[1]) * r2 + a[2]) * r2 + a[3]) * r2 + a[4]) * r2 + a[5]) * q / \
               (((((b[0] * r2 + b[1]) * r2 + b[2]) * r2 + b[3]) * r2 + b[4]) * r2 + 1)
    q = math.sqrt(-2 * math.log(1 - p))
    return -(((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5]) / \
            ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1)


def _parse_ts(ts: str) -> datetime:
    dt = datetime.fromisoformat(ts)
    return dt.astimezone(timezone.utc)


def _bucket_index(dt: datetime, interval_minutes: int) -> int:
    minutes = dt.hour * 60 + dt.minute
    return minutes // interval_minutes


def _buckets_per_day(interval_minutes: int) -> int:
    return 1440 // interval_minutes


def _sample_stats(values: list[float]) -> tuple[float, float]:
    """返回 (样本均值, 样本标准误)。"""
    n = len(values)
    mean = sum(values) / n
    if n < 2:
        return mean, float("nan")
    var = sum((v - mean) ** 2 for v in values) / (n - 1)
    return mean, math.sqrt(var) / math.sqrt(n)


class Forecaster:
    """对一个情景快照执行预测。"""

    def __init__(self, params: dict[str, Any], mapping: dict[str, Any]) -> None:
        self.p = params
        self.mapping = mapping
        self.z = inverse_normal_cdf((1 + params["confidence_level"]) / 2)
        self.buckets_per_day = _buckets_per_day(params["interval_minutes"])
        rate = params.get("options", {}).get("traffic_conversion", 0.1)
        self.traffic_conversion = float(rate)

    # ---- 单源剖面 -----------------------------------------------------

    def _source_profile(
        self, records: list[dict[str, Any]]
    ) -> tuple[dict[int, list[float]], datetime, datetime, int]:
        samples: dict[int, list[float]] = {}
        days: set[str] = set()
        earliest = latest = None
        for rec in records:
            dt = _parse_ts(rec["timestamp"])
            days.add(dt.date().isoformat())
            samples.setdefault(_bucket_index(dt, self.p["interval_minutes"]), []).append(
                rec["value"]
            )
            if earliest is None or dt < earliest:
                earliest = dt
            if latest is None or dt > latest:
                latest = dt
        assert earliest is not None and latest is not None
        return samples, earliest, latest, len(days)

    def _forecast_source(
        self, source: str, records: list[dict[str, Any]], anchor: datetime
    ) -> dict[str, Any]:
        samples, earliest, latest, ndays = self._source_profile(records)
        growth = self.p["growth_factor"]
        conversion = self.traffic_conversion if source == "traffic_flow" else 1.0

        bucket_stats: dict[int, tuple[float, float, int]] = {}
        all_means: list[float] = []
        any_sem = False
        for b, vals in samples.items():
            mean, sem = _sample_stats(vals)
            bucket_stats[b] = (mean, sem, len(vals))
            all_means.append(mean)
            if not math.isnan(sem):
                any_sem = True
        global_mean = sum(all_means) / len(all_means) if all_means else 0.0
        # 全局离散度用于样本不足桶的不确定度兜底。
        global_sem = (
            math.sqrt(sum((m - global_mean) ** 2 for m in all_means) /
                      max(len(all_means) - 1, 1))
            if len(all_means) >= 2 else global_mean * 0.25
        )

        def bucket_estimate(b: int) -> tuple[float, float, int]:
            if b in bucket_stats:
                mean, sem, n = bucket_stats[b]
                point = mean * growth * conversion
                half = (self.z * (sem if not math.isnan(sem) else mean * 0.25)
                        * growth * conversion)
                return point, half, n
            # 该日内桶无历史样本：用全局均值，区间更宽。
            point = global_mean * growth * conversion
            return point, self.z * global_sem * growth * conversion, 0

        series: list[dict[str, Any]] = []
        interval = self.p["interval_minutes"]
        steps = self.p["horizon_hours"] * 60 // interval
        for i in range(steps):
            t = anchor + timedelta(minutes=interval * i)
            b = _bucket_index(t, interval)
            point, half, n = bucket_estimate(b)
            series.append({
                "timestamp": t.isoformat(),
                "bucket": b,
                "sample_size": n,
                "point": _r(point),
                "lower": _r(max(0.0, point - half)),
                "upper": _r(point + half),
            })
        return {
            "series": series,
            "sample_days": ndays,
            "observed_range": [earliest.isoformat(), latest.isoformat()],
            "bucket_count": len(samples),
        }

    # ---- 服务区：合并充电需求与交通流量 ------------------------------

    def forecast_area(
        self, area: str, inputs_by_source: dict[str, list[dict[str, Any]]],
        anchor: datetime,
    ) -> dict[str, Any]:
        sources_out: dict[str, Any] = {}
        for source, records in sorted(inputs_by_source.items()):
            sources_out[source] = self._forecast_source(source, records, anchor)

        steps = len(next(iter(sources_out.values()))["series"])
        demand_series: list[dict[str, Any]] = []
        for i in range(steps):
            point = lower = upper = 0.0
            ts = bucket = None
            for src in sources_out.values():
                row = src["series"][i]
                point += row["point"]
                lower += row["lower"]
                upper += row["upper"]
                ts, bucket = row["timestamp"], row["bucket"]
            demand_series.append({
                "timestamp": ts, "bucket": bucket,
                "point": _r(point), "lower": _r(lower), "upper": _r(upper),
            })

        peak_idx = max(range(steps), key=lambda i: demand_series[i]["point"])
        peak_row = demand_series[peak_idx]
        sample_days = max(s["sample_days"] for s in sources_out.values())
        observed = [s["observed_range"] for s in sources_out.values()]
        return {
            "sources": sources_out,
            "demand_series": demand_series,
            "peak": {
                "timestamp": peak_row["timestamp"],
                "point": peak_row["point"],
                "ci_low": peak_row["lower"],
                "ci_high": peak_row["upper"],
                "conservative_low": _r(max(row["lower"] for row in demand_series)),
                "conservative_high": _r(max(row["upper"] for row in demand_series)),
            },
            "sample_days": sample_days,
            "observed_range": [
                min(o[0] for o in observed), max(o[1] for o in observed),
            ],
        }

    # ---- 部署区域聚合 -------------------------------------------------

    def aggregate_regions(
        self, area_results: dict[str, dict[str, Any]]
    ) -> dict[str, Any]:
        weights: dict[str, list[tuple[str, float]]] = {}
        for m in self.mapping["mappings"]:
            weights.setdefault(m["deploy_region"], []).append(
                (m["service_area"], m["weight"])
            )
        out: dict[str, Any] = {}
        for region, members in sorted(weights.items()):
            present = [(a, w) for a, w in members if a in area_results]
            if not present:
                continue
            steps = len(area_results[present[0][0]]["demand_series"])
            series: list[dict[str, Any]] = []
            for i in range(steps):
                point = lower = upper = 0.0
                ts = None
                for a, w in present:
                    row = area_results[a]["demand_series"][i]
                    point += row["point"] * w
                    lower += row["lower"] * w
                    upper += row["upper"] * w
                    ts = row["timestamp"]
                series.append({
                    "timestamp": ts,
                    "point": _r(point), "lower": _r(lower), "upper": _r(upper),
                })
            peak_idx = max(range(steps), key=lambda i: series[i]["point"])
            out[region] = {
                "service_areas": [a for a, _ in present],
                "demand_series": series,
                "peak": {
                    "timestamp": series[peak_idx]["timestamp"],
                    "point": series[peak_idx]["point"],
                    "ci_low": series[peak_idx]["lower"],
                    "ci_high": series[peak_idx]["upper"],
                    "conservative_low": _r(max(r["lower"] for r in series)),
                    "conservative_high": _r(max(r["upper"] for r in series)),
                },
            }
        return out

    # ---- 入口 ---------------------------------------------------------

    def plan(
        self, resolved_inputs: list[dict[str, Any]]
    ) -> tuple[dict[str, dict[str, list[dict[str, Any]]]], datetime, list[str]]:
        """把快照输入整理为 (按服务区的观测, 预测锚点, 服务区顺序)。

        锚点只依赖数据中的最晚观测时刻，使重算/续跑完全确定。
        """
        by_area: dict[str, dict[str, list[dict[str, Any]]]] = {}
        latest_ts: datetime | None = None
        for item in resolved_inputs:
            by_area.setdefault(item["service_area"], {})[item["source"]] = item["records"]
            for rec in item["records"]:
                dt = _parse_ts(rec["timestamp"])
                if latest_ts is None or dt > latest_ts:
                    latest_ts = dt
        if latest_ts is None:
            raise ValueError("没有可用观测，无法预测")
        anchor = (latest_ts + timedelta(days=1)).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        return by_area, anchor, sorted(by_area)

    def assemble(
        self,
        resolved_inputs: list[dict[str, Any]],
        parameters_version_id: str,
        mapping_version_id: str,
        area_results: dict[str, dict[str, Any]],
        anchor: datetime,
    ) -> dict[str, Any]:
        region_results = self.aggregate_regions(area_results)
        return {
            "schema": "forecast/1",
            "engine_version": ENGINE_VERSION,
            "anchor": anchor.isoformat(),
            "horizon_hours": self.p["horizon_hours"],
            "interval_minutes": self.p["interval_minutes"],
            "confidence_level": self.p["confidence_level"],
            "growth_factor": self.p["growth_factor"],
            "z_quantile": _r(self.z),
            "service_areas": area_results,
            "deploy_regions": region_results,
            "inputs": [
                {"source_key": i["source_key"], "version_id": i["version_id"]}
                for i in sorted(resolved_inputs, key=lambda x: x["source_key"])
            ],
            "parameters_version": parameters_version_id,
            "mapping_version": mapping_version_id,
        }

    def run(
        self,
        resolved_inputs: list[dict[str, Any]],
        parameters_version_id: str,
        mapping_version_id: str,
    ) -> dict[str, Any]:
        by_area, anchor, area_order = self.plan(resolved_inputs)
        area_results: dict[str, Any] = {}
        for area in area_order:
            area_results[area] = self.forecast_area(area, by_area[area], anchor)
        return self.assemble(
            resolved_inputs, parameters_version_id, mapping_version_id,
            area_results, anchor,
        )
