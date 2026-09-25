"""纯计算层：跨日聚合、服务区峰值与置信区间。

确定性、无副作用：同一组输入永远得到同一组结果，因此结果可用
内容哈希固化并用于谱系核验。
"""
from __future__ import annotations

import hashlib
import json
import math
import statistics
from collections import defaultdict
from dataclasses import dataclass

from .domain import ValidationError
from .ports import parse_iso

MODEL_HOURLY_MEAN = "hourly-mean"
_PARAM_KEYS = ("growth_factor", "holiday_factor", "confidence", "model")


def canonical_json(obj) -> str:
    """稳定 JSON 序列化：键排序、紧凑分隔，用于内容哈希。"""
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class ForecastParams:
    """预测参数：增长系数、节假日系数与置信水平。"""

    growth_factor: float = 1.0
    holiday_factor: float = 1.0
    confidence: float = 0.95
    model: str = MODEL_HOURLY_MEAN

    @classmethod
    def from_payload(cls, payload) -> "ForecastParams":
        if not isinstance(payload, dict):
            raise ValidationError("参数必须是 JSON 对象")
        unknown = sorted(set(payload) - set(_PARAM_KEYS))
        if unknown:
            raise ValidationError(f"未知参数: {', '.join(unknown)}")
        try:
            params = cls(
                growth_factor=float(payload.get("growth_factor", 1.0)),
                holiday_factor=float(payload.get("holiday_factor", 1.0)),
                confidence=float(payload.get("confidence", 0.95)),
                model=str(payload.get("model", MODEL_HOURLY_MEAN)),
            )
        except (TypeError, ValueError) as exc:
            raise ValidationError(f"参数取值非法: {exc}") from exc
        params.validate()
        return params

    def validate(self) -> None:
        if self.model != MODEL_HOURLY_MEAN:
            raise ValidationError(f"不支持的模型: {self.model}")
        if not 0 < self.growth_factor <= 10:
            raise ValidationError("growth_factor 必须在 (0, 10] 区间")
        if not 0 < self.holiday_factor <= 10:
            raise ValidationError("holiday_factor 必须在 (0, 10] 区间")
        if not 0.5 <= self.confidence < 0.9999:
            raise ValidationError("confidence 必须在 [0.5, 0.9999) 区间")

    def canonical(self) -> dict:
        return {
            "confidence": self.confidence,
            "growth_factor": self.growth_factor,
            "holiday_factor": self.holiday_factor,
            "model": self.model,
        }

    @property
    def factor(self) -> float:
        return self.growth_factor * self.holiday_factor

    @property
    def z(self) -> float:
        return statistics.NormalDist().inv_cdf((1.0 + self.confidence) / 2.0)


def aggregate_daily(records, mapping: dict):
    """把站点记录聚合为 (服务区, 自然日, 小时) 的充电量合计。

    跨日聚合的关键：先按记录自身时间戳归入自然日与小时桶，
    再跨天统计。跨午夜的两条记录各自归属其时间戳所在的日与小时，
    不会被合并或平移。未映射的站点被跳过并计数。
    """
    daily: dict[tuple[str, str, int], float] = defaultdict(float)
    skipped = 0
    for record in records:
        area = mapping.get(record["station_id"])
        if area is None:
            skipped += 1
            continue
        moment = parse_iso(record["observed_at"])
        daily[(area, moment.date().isoformat(), moment.hour)] += float(record["energy_kwh"])
    return daily, skipped


def compute_area_rows(area: str, daily: dict, params: ForecastParams) -> list[dict]:
    """计算单个服务区的逐小时预测行（均值、标准差、置信区间、峰值标记）。"""
    by_hour: dict[int, list[float]] = defaultdict(list)
    for (area_key, _day, hour), energy in daily.items():
        if area_key == area:
            by_hour[hour].append(energy)
    factor = params.factor
    rows = []
    for hour in sorted(by_hour):
        values = by_hour[hour]
        days = len(values)
        mean = statistics.fmean(values)
        std = statistics.stdev(values) if days >= 2 else 0.0
        adjusted = mean * factor
        half_width = params.z * std / math.sqrt(days) * factor
        rows.append({
            "service_area": area,
            "hour": hour,
            "days": days,
            "mean_kwh": round(adjusted, 6),
            "std_kwh": round(std * factor, 6),
            "ci_low": round(max(0.0, adjusted - half_width), 6),
            "ci_high": round(adjusted + half_width, 6),
            "is_peak": False,
        })
    if rows:
        # 峰值：均值最大者；并列时取当天最早小时，保证确定性。
        peak = max(rows, key=lambda row: (row["mean_kwh"], -row["hour"]))
        peak["is_peak"] = True
    return rows


def compute_all(records, mapping: dict, params: ForecastParams):
    """对全部服务区计算预测行，返回 {服务区: [行...]} 与跳过记录数。"""
    daily, skipped = aggregate_daily(records, mapping)
    areas = sorted({key[0] for key in daily})
    return {area: compute_area_rows(area, daily, params) for area in areas}, skipped


def results_hash(rows) -> str:
    """结果集内容哈希：用于固化与谱系核验，对行序不敏感。"""
    canonical = [
        [
            row["service_area"], row["hour"], row["days"],
            row["mean_kwh"], row["std_kwh"], row["ci_low"], row["ci_high"],
            bool(row["is_peak"]),
        ]
        for row in sorted(rows, key=lambda r: (r["service_area"], r["hour"]))
    ]
    return sha256_text(canonical_json(canonical))
