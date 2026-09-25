"""长任务可重入：中断后从检查点续跑，重启进程（新连接）亦可续跑。"""
from __future__ import annotations

import unittest

from helpers import (
    ServiceTestCase,
    hourly_records,
    import_batch,
    make_mapping,
    make_params,
    results_by_area,
)
from service_09251_006.domain import ConflictError, JobInterrupted
from service_09251_006.ports import FixedClock, SequentialIds
from service_09251_006.services import ForecastService
from service_09251_006.storage import Repository


class ResumableJobTests(ServiceTestCase):
    def setUp(self) -> None:
        super().setUp()
        mapping_id = make_mapping(
            self.service, {"S1": "AreaA", "S2": "AreaB", "S3": "AreaC"})
        param_id = make_params(self.service)
        records = []
        for station in ("S1", "S2", "S3"):
            records += hourly_records(station, 20, {8: 100.0, 9: 200.0})
        batch = import_batch(self.service, "bureau", records)
        self.version = self.service.create_version(
            batch_ids=[batch["batch"]["batch_id"]], param_id=param_id,
            mapping_id=mapping_id, scenario="three-areas")

    def test_start_compute_is_idempotent(self) -> None:
        first = self.service.start_compute(self.version["version_id"])
        second = self.service.start_compute(self.version["version_id"])
        self.assertTrue(first["created"])
        self.assertFalse(second["created"])
        self.assertEqual(first["job"]["job_id"], second["job"]["job_id"])
        self.assertEqual(3, first["job"]["total"])

    def test_interrupted_job_resumes_from_checkpoint(self) -> None:
        job = self.service.start_compute(self.version["version_id"])["job"]
        seen = []

        def stop_before_third(area: str) -> bool:
            seen.append(area)
            return len(seen) >= 3  # 完成两个服务区后中断

        with self.assertRaises(JobInterrupted):
            self.service.run_compute(job["job_id"], should_stop=stop_before_third)

        paused = self.service.get_job(job["job_id"])
        self.assertEqual("RUNNING", paused["status"])
        self.assertEqual(2, paused["done"])
        self.assertEqual(["AreaA", "AreaB"],
                         sorted(results_by_area(self.service, self.version["version_id"])))
        self.assertEqual("DRAFT", self.service.get_version(self.version["version_id"])["status"])

        # 续跑：跳过已完成步骤，只补算剩余服务区。
        finished = self.service.run_compute(job["job_id"])
        self.assertEqual("DONE", finished["status"])
        self.assertEqual(3, finished["done"])
        grouped = results_by_area(self.service, self.version["version_id"])
        self.assertEqual(["AreaA", "AreaB", "AreaC"], sorted(grouped))
        # 每个服务区恰好 2 行（两小时），重放未产生重复。
        for rows in grouped.values():
            self.assertEqual(2, len(rows))
        version = self.service.get_version(self.version["version_id"])
        self.assertEqual("COMPUTED", version["status"])
        self.assertTrue(version["result_hash"])

    def test_resume_survives_process_restart(self) -> None:
        job = self.service.start_compute(self.version["version_id"])["job"]
        with self.assertRaises(JobInterrupted):
            self.service.run_compute(job["job_id"], should_stop=lambda area: True)

        # 模拟重启：同一数据库文件上构造全新的仓储与服务（无任何内存状态）。
        restarted = ForecastService(Repository(self.db_path),
                                    clock=FixedClock(), ids=SequentialIds())
        finished = restarted.run_compute(job["job_id"])
        self.assertEqual("DONE", finished["status"])
        self.assertEqual("COMPUTED",
                         restarted.get_version(self.version["version_id"])["status"])
        self.assertEqual(6, len(restarted.get_results(self.version["version_id"])["rows"]))

        # 完成后再调用是安全的空操作。
        again = restarted.run_compute(job["job_id"])
        self.assertEqual("DONE", again["status"])

    def test_compute_rejected_once_results_frozen(self) -> None:
        job = self.service.start_compute(self.version["version_id"])["job"]
        self.service.run_compute(job["job_id"])
        with self.assertRaises(ConflictError):
            self.service.start_compute(self.version["version_id"])


if __name__ == "__main__":
    unittest.main()
