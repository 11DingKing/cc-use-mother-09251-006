"""测试公共构造：可复现的服务、样例数据与便捷函数。"""
from __future__ import annotations

import os
import tempfile
import unittest

from service_09251_006.ports import FixedClock, SequentialIds
from service_09251_006.services import ForecastService
from service_09251_006.storage import Repository


class ServiceTestCase(unittest.TestCase):
    """每个用例一个独立临时库，注入固定时钟与序列标识。"""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory(prefix="s09251_006_")
        self.addCleanup(self._tmp.cleanup)
        self.db_path = os.path.join(self._tmp.name, "app.db")
        self.service = self.make_service()

    def make_service(self) -> ForecastService:
        return ForecastService(Repository(self.db_path),
                               clock=FixedClock(), ids=SequentialIds())


def hourly_records(station: str, day: int, energies: dict,
                   *, vehicles: int = 10, sessions: int = 5) -> list[dict]:
    """构造某站点某天若干小时的记录。energies: {小时: kWh}"""
    return [
        {
            "station_id": station,
            "observed_at": f"2026-09-{day:02d}T{hour:02d}:00:00Z",
            "vehicles": vehicles,
            "sessions": sessions,
            "energy_kwh": float(kwh),
        }
        for hour, kwh in sorted(energies.items())
    ]


def import_batch(service: ForecastService, source: str, records: list[dict], **kw) -> dict:
    return service.import_batch(source=source, records=records, **kw)


def make_params(service: ForecastService, **overrides) -> str:
    payload = {"growth_factor": 1.0, "holiday_factor": 1.0, "confidence": 0.95}
    payload.update(overrides)
    return service.create_parameter_set(payload)["parameter_set"]["param_id"]


def make_mapping(service: ForecastService, entries: dict) -> str:
    return service.create_region_mapping(entries)["region_mapping"]["mapping_id"]


def computed_version(service: ForecastService, *, batch_ids, param_id, mapping_id,
                     scenario="base", parent=None) -> dict:
    version = service.create_version(
        batch_ids=batch_ids, param_id=param_id, mapping_id=mapping_id,
        scenario=scenario, parent_version_id=parent)
    job = service.start_compute(version["version_id"])["job"]
    service.run_compute(job["job_id"])
    return service.get_version(version["version_id"])


def results_by_area(service: ForecastService, version_id: str) -> dict:
    rows = service.get_results(version_id)["rows"]
    grouped: dict[str, list[dict]] = {}
    for row in rows:
        grouped.setdefault(row["service_area"], []).append(row)
    return grouped
