"""领域模型校验：原始车流观测、预测参数、区域映射。

校验只检查结构与基本业务约束；领域对象本身是普通 dict（便于内容寻址）。
原始记录一旦登记即不可变，更正通过新版本表达。
"""
from __future__ import annotations

from typing import Any

from .errors import ValidationError

# 允许的数据源键：交通流量、充电需求、两类历史观测
SOURCE_KINDS = {"traffic_flow", "charging_demand"}


def require(obj: Any, name: str, typ: type | tuple[type, ...]) -> Any:
    if not isinstance(obj, typ):
        raise ValidationError(f"{name} 类型错误", expect=str(typ), got=type(obj).__name__)
    return obj


def validate_raw_batch(doc: dict[str, Any]) -> dict[str, Any]:
    """校验并归一化一个原始数据批次。

    结构::

        {
          "source": "traffic_flow" | "charging_demand",
          "service_area": "A01",
          "records": [
             {"timestamp": "2026-09-24T08:00:00+08:00", "value": 120.5}, ...
          ],
          "unit": "vehicles/h"   # 可选
        }
    """
    require(doc, "批次文档", dict)
    source = doc.get("source")
    if source not in SOURCE_KINDS:
        raise ValidationError("source 必须是 traffic_flow 或 charging_demand", source=source)
    area = doc.get("service_area")
    if not isinstance(area, str) or not area.strip():
        raise ValidationError("service_area 必须是非空字符串")
    records = doc.get("records")
    require(records, "records", list)
    if not records:
        raise ValidationError("records 不能为空")

    norm_records: list[dict[str, Any]] = []
    seen: set[str] = set()
    for i, rec in enumerate(records):
        require(rec, f"records[{i}]", dict)
        ts = rec.get("timestamp")
        if not isinstance(ts, str) or not ts.strip():
            raise ValidationError(f"records[{i}].timestamp 必须是 ISO 时间字符串")
        # 解析校验（拒绝脏时间），但保留原始字符串
        try:
            from datetime import datetime

            parsed = datetime.fromisoformat(ts)
        except ValueError:
            raise ValidationError(f"records[{i}].timestamp 无法解析", timestamp=ts)
        if parsed.tzinfo is None:
            raise ValidationError(f"records[{i}].timestamp 必须带时区", timestamp=ts)
        value = rec.get("value")
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ValidationError(f"records[{i}].value 必须是数字")
        if value < 0:
            raise ValidationError(f"records[{i}].value 不能为负", value=value)
        if ts in seen:
            raise ValidationError(f"records[{i}] 时间戳重复", timestamp=ts)
        seen.add(ts)
        norm = {"timestamp": ts, "value": float(value)}
        if "metadata" in rec:
            require(rec["metadata"], f"records[{i}].metadata", dict)
            norm["metadata"] = rec["metadata"]
        norm_records.append(norm)

    norm_records.sort(key=lambda r: r["timestamp"])
    normalized: dict[str, Any] = {
        "source": source,
        "service_area": area,
        "records": norm_records,
    }
    if "unit" in doc:
        if not isinstance(doc["unit"], str):
            raise ValidationError("unit 必须是字符串")
        normalized["unit"] = doc["unit"]
    return normalized


def validate_parameters(doc: dict[str, Any]) -> dict[str, Any]:
    """校验预测参数。

    结构::

        {
          "horizon_hours": 24,
          "interval_minutes": 60,
          "peak_quantile": 0.95,
          "confidence_level": 0.90,
          "growth_factor": 1.05,
          "options": {...}        # 可选自由参数
        }
    """
    require(doc, "参数文档", dict)
    horizon = doc.get("horizon_hours", 24)
    if not isinstance(horizon, int) or isinstance(horizon, bool) or horizon <= 0:
        raise ValidationError("horizon_hours 必须是正整数")
    interval = doc.get("interval_minutes", 60)
    if not isinstance(interval, int) or isinstance(interval, bool) or interval <= 0:
        raise ValidationError("interval_minutes 必须是正整数")
    if 1440 % interval != 0:
        raise ValidationError("interval_minutes 应能整除 1440", interval=interval)
    peak_q = doc.get("peak_quantile", 0.95)
    if not isinstance(peak_q, (int, float)) or not 0.5 <= peak_q < 1:
        raise ValidationError("peak_quantile 必须在 [0.5, 1) 区间")
    conf = doc.get("confidence_level", 0.90)
    if not isinstance(conf, (int, float)) or not 0 < conf < 1:
        raise ValidationError("confidence_level 必须在 (0, 1) 区间")
    growth = doc.get("growth_factor", 1.0)
    if not isinstance(growth, (int, float)) or growth <= 0:
        raise ValidationError("growth_factor 必须为正数")
    normalized = {
        "horizon_hours": int(horizon),
        "interval_minutes": int(interval),
        "peak_quantile": float(peak_q),
        "confidence_level": float(conf),
        "growth_factor": float(growth),
    }
    if "options" in doc:
        require(doc["options"], "options", dict)
        normalized["options"] = doc["options"]
    return normalized


def validate_region_map(doc: dict[str, Any]) -> dict[str, Any]:
    """校验区域映射：观测服务区 -> 部署区域（移动设施调度单元）。

    结构:: {"mappings": [{"service_area": "A01", "deploy_region": "R1", "weight": 1.0}]}

    一个服务区在同一份映射中只能出现一次（防止执行端拿到歧义归属）。
    """
    require(doc, "映射文档", dict)
    mappings = doc.get("mappings")
    require(mappings, "mappings", list)
    if not mappings:
        raise ValidationError("mappings 不能为空")
    norm: list[dict[str, Any]] = []
    seen_areas: set[str] = set()
    for i, m in enumerate(mappings):
        require(m, f"mappings[{i}]", dict)
        sa = m.get("service_area")
        dr = m.get("deploy_region")
        if not isinstance(sa, str) or not sa.strip():
            raise ValidationError(f"mappings[{i}].service_area 必须是非空字符串")
        if not isinstance(dr, str) or not dr.strip():
            raise ValidationError(f"mappings[{i}].deploy_region 必须是非空字符串")
        if sa in seen_areas:
            raise ValidationError(f"mappings[{i}] 服务区重复映射", service_area=sa)
        seen_areas.add(sa)
        weight = m.get("weight", 1.0)
        if isinstance(weight, bool) or not isinstance(weight, (int, float)) or weight <= 0:
            raise ValidationError(f"mappings[{i}].weight 必须为正数")
        norm.append(
            {"service_area": sa, "deploy_region": dr, "weight": float(weight)}
        )
    norm.sort(key=lambda m: (m["service_area"], m["deploy_region"]))
    return {"mappings": norm}


def validate_decision(doc: dict[str, Any]) -> dict[str, Any]:
    """校验调度决策（与被采用预测绑定在服务层完成）。"""
    require(doc, "决策文档", dict)
    plan = doc.get("deployments")
    require(plan, "deployments", list)
    norm_plan = []
    for i, item in enumerate(plan):
        require(item, f"deployments[{i}]", dict)
        region = item.get("deploy_region")
        if not isinstance(region, str) or not region.strip():
            raise ValidationError(f"deployments[{i}].deploy_region 必须是非空字符串")
        units = item.get("mobile_units")
        if not isinstance(units, int) or isinstance(units, bool) or units < 0:
            raise ValidationError(f"deployments[{i}].mobile_units 必须是非负整数")
        entry: dict[str, Any] = {"deploy_region": region, "mobile_units": units}
        if "note" in item:
            entry["note"] = str(item["note"])
        norm_plan.append(entry)
    if not norm_plan:
        raise ValidationError("deployments 不能为空")
    norm_plan.sort(key=lambda d: d["deploy_region"])
    normalized: dict[str, Any] = {"deployments": norm_plan}
    if "scheduled_for" in doc:
        normalized["scheduled_for"] = str(doc["scheduled_for"])
    return normalized
