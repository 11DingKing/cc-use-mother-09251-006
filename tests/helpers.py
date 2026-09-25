"""测试公共辅助：临时数据目录、可复现时钟/ID、样例数据构造。"""
from __future__ import annotations

import shutil
import tempfile
import unittest
from typing import Any

from service_09251_006.clock import FrozenClock, SequenceIdGenerator
from service_09251_006.container import Application


class AppTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.mkdtemp(prefix="svc09251-test-")
        self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)
        self.clock = FrozenClock("2026-09-25T10:00:00+00:00")
        self.app = Application(
            self.tmp, clock=self.clock, idgen=SequenceIdGenerator(),
            lease_seconds=3600.0,  # 默认不租约过期，需要时自行缩短
        )

    def advance_clock(self, seconds: float) -> None:
        from datetime import timedelta

        self.clock._moment += timedelta(seconds=seconds)


def flow_batch(
    area: str, days: tuple[int, ...] = (22, 23, 24),
    profile: dict[int, float] | None = None,
    source: str = "traffic_flow",
) -> dict[str, Any]:
    """构造跨多日的车流批次（同一日内桶在多日重复，形成跨日聚合样本）。

    各日带 ±5% 自然波动（均值恰为基准值），使跨日样本方差非零，置信区间可检验。
    """
    profile = profile or {8: 1000.0, 9: 1400.0, 18: 1800.0}
    day_factors = {d: f for d, f in zip(days, _factors(len(days)))}
    records = [
        {"timestamp": f"2026-09-{d:02d}T{h:02d}:00:00+08:00",
         "value": round(v * day_factors[d], 3)}
        for d in days for h, v in sorted(profile.items())
    ]
    return {"source": source, "service_area": area, "records": records}


def _factors(n: int) -> list[float]:
    """均值为 1、跨度 ±5% 的逐日系数。"""
    if n <= 1:
        return [1.0]
    return [1.0 + 0.05 * (2 * i / (n - 1) - 1) for i in range(n)]


def demand_batch(
    area: str, days: tuple[int, ...] = (22, 23, 24),
    profile: dict[int, float] | None = None,
) -> dict[str, Any]:
    profile = profile or {8: 40.0, 9: 55.0, 18: 80.0}
    return flow_batch(area, days, profile, source="charging_demand")


def standard_params(**over: Any) -> dict[str, Any]:
    p = {
        "horizon_hours": 24, "interval_minutes": 60,
        "peak_quantile": 0.95, "confidence_level": 0.90,
        "growth_factor": 1.1,
    }
    p.update(over)
    return p


def mapping(*pairs: tuple[str, str, float]) -> dict[str, Any]:
    return {"mappings": [
        {"service_area": a, "deploy_region": r, "weight": w} for a, r, w in pairs
    ]}


def seed_complete_scenario(app: Application, areas=("A01",)) -> tuple[str, str, str, list[str], str]:
    """建立一个输入/参数/映射齐备的情景，返回 (sid, params_vid, mapping_vid, input_vids, mapping_vid)。"""
    v = app.versions
    input_ids = []
    for a in areas:
        input_ids.append(v.import_raw_batch(flow_batch(a))["version_id"])
        input_ids.append(v.import_raw_batch(demand_batch(a))["version_id"])
    pid = v.import_parameters(standard_params())["version_id"]
    mid = v.import_region_map(
        mapping(*[(a, "R1", 1.0) for a in areas])
    )["version_id"]
    sid = v.create_scenario(parameters_version_id=pid, mapping_version_id=mid)["scenario_id"]
    for iv in input_ids:
        v.add_input(sid, iv)
    return sid, pid, mid, input_ids, mid
