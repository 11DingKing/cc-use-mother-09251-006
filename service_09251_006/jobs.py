"""长任务运行器：按服务区分步计算，逐步落检查点。

可重入性保证：
- 每个检查点（一个服务区的结果写入 + 步骤登记 + 进度推进）在一个事务内提交；
- 中断只可能发生在检查点之间，已提交的步骤永不重复执行；
- 进程重启后用同一 job_id 再次调用 run() 即可从断点续跑；
- 结果写入使用 INSERT OR IGNORE，即使步骤被重放也不会产生重复行。
"""
from __future__ import annotations

import json

from . import forecast
from .domain import ConflictError, JobInterrupted, JobStatus, NotFoundError
from .forecast import ForecastParams


class ComputeRunner:
    """计算任务执行器：无内存状态，全部进度落在 jobs / job_steps 表。"""

    def __init__(self, repo, clock):
        self.repo = repo
        self.clock = clock

    def run(self, job_id: str, should_stop=None) -> dict:
        """执行（或续跑）计算任务。should_stop 仅供测试在检查点间注入中断。"""
        job = self.repo.get_job(job_id)
        if job is None:
            raise NotFoundError(f"任务不存在: {job_id}")
        if job["status"] == JobStatus.DONE.value:
            return self._view(job)
        if job["status"] == JobStatus.FAILED.value:
            raise ConflictError("任务已失败，请修正输入后重新发起计算",
                                details={"job_id": job_id, "error": job["error"]})

        version = self.repo.get_version(job["version_id"])
        params = ForecastParams.from_payload(
            json.loads(self.repo.get_params(version["param_id"])["payload"]))
        mapping = json.loads(self.repo.get_mapping(version["mapping_id"])["entries"])
        records = self.repo.records_for_batches(json.loads(version["batch_ids"]))
        daily, _skipped = forecast.aggregate_daily(records, mapping)
        areas = sorted({key[0] for key in daily})

        done_steps = self.repo.steps_done(job_id)
        self.repo.set_job_status(job_id, JobStatus.RUNNING.value, self.clock.now_iso())
        try:
            for area in areas:
                if area in done_steps:
                    continue
                if should_stop is not None and should_stop(area):
                    raise JobInterrupted(f"任务在 {area} 之前中断")
                rows = forecast.compute_area_rows(area, daily, params)
                now = self.clock.now_iso()
                with self.repo.tx():
                    self.repo.insert_results(job["version_id"], rows)
                    self.repo.insert_step(job_id, area, now)
                    self.repo.bump_job_done(job_id, now)
        except JobInterrupted:
            raise
        except Exception as exc:
            self.repo.set_job_status(job_id, JobStatus.FAILED.value,
                                     self.clock.now_iso(), error=str(exc))
            raise

        all_rows = self.repo.get_results(job["version_id"])
        digest = forecast.results_hash(all_rows)
        now = self.clock.now_iso()
        with self.repo.tx():
            changed = self.repo.set_computed(job["version_id"], digest)
            if not changed:
                raise ConflictError("版本状态已变化，无法固化计算结果",
                                    details={"version_id": job["version_id"]})
            self.repo.set_job_status(job_id, JobStatus.DONE.value, now)
        return self._view(self.repo.get_job(job_id))

    @staticmethod
    def _view(job: dict) -> dict:
        return {
            "job_id": job["job_id"],
            "kind": job["kind"],
            "version_id": job["version_id"],
            "status": job["status"],
            "total": job["total"],
            "done": job["done"],
            "error": job["error"],
            "created_at": job["created_at"],
            "updated_at": job["updated_at"],
        }
