"""预测执行服务：可重入长作业（租约 + 分服务区检查点）与更正影响传播。

作业生命周期：
    queued --acquire(租约)--> running --完成--> succeeded/failed
进程崩溃后租约到期，其他工作进程（或重启后的同一进程）可重新认领，
已完成的服务区结果记录在 cursor 中，续跑时跳过，不重复计算。

重复提交同一预测请求（相同情景提交、相同内容指纹）返回同一运行记录。
"""
from __future__ import annotations

import json
import os
import sqlite3
from datetime import timedelta
from typing import Any, Callable

from ..clock import Clock, IdGenerator
from ..errors import InvalidStateError
from ..forecast import Forecaster
from ..storage.database import json_dumps, json_loads
from .snapshots import Snapshot, load_resolved_inputs, resolve_commit
from .versioning import VersionService

DEFAULT_LEASE_SECONDS = 30.0


def mark_correction_impact_for_original(
    conn: sqlite3.Connection, correction_id: str, original_version_id: str,
    now: str, idgen: IdGenerator,
) -> list[str]:
    rows = conn.execute(
        "SELECT DISTINCT forecast_version_id FROM forecast_inputs "
        "WHERE input_version_id=?",
        (original_version_id,),
    ).fetchall()
    affected: list[str] = []
    for r in rows:
        fvid = r["forecast_version_id"]
        try:
            conn.execute(
                "INSERT INTO invalidation_marks(id,correction_id,"
                "forecast_version_id,status,marked_at) VALUES(?,?,?, 'affected',?)",
                (idgen.new_id("mrk"), correction_id, fvid, now),
            )
            affected.append(fvid)
        except sqlite3.IntegrityError:
            # 同一更正对同一预测的标记已存在（重复提交更正）。
            pass
    return affected


class ForecastService:
    def __init__(
        self, repo: Any, versions: VersionService, clock: Clock,
        idgen: IdGenerator, lease_seconds: float = DEFAULT_LEASE_SECONDS,
        step_hook: Callable[[str, int, int], None] | None = None,
    ) -> None:
        self.repo = repo
        self.versions = versions
        self.clock = clock
        self.idgen = idgen
        self.lease_seconds = lease_seconds
        self.step_hook = step_hook

    def _now(self) -> str:
        return self.clock.now().isoformat(timespec="microseconds")

    # ---- 提交预测请求 -------------------------------------------------

    def submit_forecast(
        self, scenario_id: str, *, commit_id: str | None = None,
        idempotency_key: str | None = None, created_by: str | None = None,
    ) -> dict[str, Any]:
        """在情景的某提交上请求预测；同内容预测幂等复用。"""
        snap = resolve_commit(self.repo, scenario_id, commit_id)
        snap.require_complete()
        fingerprint = self.versions.forecast_fingerprint(snap)

        with self.repo.db.transaction() as conn:
            # 同情景提交 + 同指纹：已完成则复用运行；进行中/排队则复用作业。
            existing_run = conn.execute(
                "SELECT fr.*, j.status AS job_status FROM forecast_runs fr "
                "LEFT JOIN jobs j ON j.id=fr.job_id "
                "WHERE fr.scenario_commit_id=?",
                (snap.commit_id,),
            ).fetchall()
            for r in existing_run:
                if r["fingerprint"] == fingerprint:
                    job = conn.execute(
                        "SELECT status FROM jobs WHERE id=?", (r["job_id"],)
                    ).fetchone() if r["job_id"] else None
                    return {
                        "run_id": r["id"],
                        "forecast_version_id": r["forecast_version_id"],
                        "scenario_commit_id": snap.commit_id,
                        "fingerprint": fingerprint,
                        "deduped": True, "job_id": r["job_id"],
                        "job_status": job["status"] if job else "succeeded",
                    }
            pending = conn.execute(
                "SELECT id FROM jobs WHERE kind='forecast' AND status IN "
                "('queued','running') AND json_extract(payload,'$.commit_id')=? "
                "AND json_extract(payload,'$.fingerprint')=?",
                (snap.commit_id, fingerprint),
            ).fetchone()
            if pending is not None:
                return {
                    "job_id": pending["id"],
                    "scenario_commit_id": snap.commit_id,
                    "fingerprint": fingerprint, "deduped": True,
                }

            job_id = self.idgen.new_id("job")
            payload = {
                "scenario_id": scenario_id,
                "commit_id": snap.commit_id,
                "fingerprint": fingerprint,
                "requested_by": created_by,
            }
            try:
                conn.execute(
                    "INSERT INTO jobs(id,kind,status,idempotency_key,payload,"
                    "cursor,attempts,created_at,updated_at) "
                    "VALUES(?, 'forecast', 'queued', ?, ?, NULL, 0, ?, ?)",
                    (job_id, idempotency_key, json_dumps(payload),
                     self._now(), self._now()),
                )
            except sqlite3.IntegrityError:
                # 客户端幂等键重试：返回既有作业，不再创建。
                row = conn.execute(
                    "SELECT id,status FROM jobs WHERE idempotency_key=?",
                    (idempotency_key,),
                ).fetchone()
                return {
                    "job_id": row["id"], "scenario_commit_id": snap.commit_id,
                    "fingerprint": fingerprint, "deduped": True,
                    "job_status": row["status"],
                }
        return {
            "job_id": job_id, "scenario_commit_id": snap.commit_id,
            "fingerprint": fingerprint, "deduped": False,
        }

    # ---- 认领与执行（可重入）-----------------------------------------

    def acquire_job(self, owner: str | None = None) -> dict[str, Any] | None:
        """认领一个可运行作业：queued，或 running 但租约已过期（崩溃续跑）。"""
        owner = owner or f"worker-{os.getpid()}"
        now = self.clock.now()
        lease_to = (now + timedelta(seconds=self.lease_seconds)).isoformat(
            timespec="microseconds"
        )
        now_s = now.isoformat(timespec="microseconds")
        with self.repo.db.transaction() as conn:
            row = conn.execute(
                "SELECT * FROM jobs WHERE kind='forecast' AND ("
                "status='queued' OR "
                "(status='running' AND lease_expires_at IS NOT NULL "
                " AND lease_expires_at < ?)) "
                "ORDER BY created_at, rowid LIMIT 1",
                (now_s,),
            ).fetchone()
            if row is None:
                return None
            conn.execute(
                "UPDATE jobs SET status='running', lease_owner=?, "
                "lease_expires_at=?, attempts=attempts+1, updated_at=?, "
                "started_at=COALESCE(started_at, ?) WHERE id=?",
                (owner, lease_to, now_s, now_s, row["id"]),
            )
            return {"job_id": row["id"], "attempts": row["attempts"] + 1,
                    "payload": json_loads(row["payload"]),
                    "cursor": json_loads(row["cursor"])}

    def heartbeat(self, job_id: str, cursor: dict[str, Any] | None = None) -> None:
        lease_to = (self.clock.now() + timedelta(seconds=self.lease_seconds)) \
            .isoformat(timespec="microseconds")
        with self.repo.db.transaction() as conn:
            conn.execute(
                "UPDATE jobs SET lease_expires_at=?, updated_at=?, "
                "cursor=COALESCE(?, cursor) WHERE id=?",
                (lease_to, self._now(),
                 json_dumps(cursor) if cursor is not None else None, job_id),
            )

    def run_job(self, job_id: str, *, owner: str | None = None) -> dict[str, Any]:
        """执行（或续跑）一个作业，直到完成。

        queued 作业会先自认领（CLI ``run-job`` 直接执行场景）；running 作业
        允许同一进程重入，崩溃后由租约过期后的认领者续跑。
        """
        row = self.repo.get_job(job_id)
        if row["status"] not in ("queued", "running"):
            return {"job_id": job_id, "status": row["status"],
                    "result": json_loads(row["result"])}
        owner = owner or f"worker-{os.getpid()}"
        if row["status"] == "queued":
            with self.repo.db.transaction() as conn:
                conn.execute(
                    "UPDATE jobs SET status='running', lease_owner=?, "
                    "lease_expires_at=?, attempts=attempts+1, "
                    "started_at=?, updated_at=? WHERE id=? AND status='queued'",
                    (owner,
                     (self.clock.now() + timedelta(seconds=self.lease_seconds))
                     .isoformat(timespec="microseconds"),
                     self._now(), self._now(), job_id),
                )
        payload = json_loads(row["payload"])
        cursor = json_loads(row["cursor"]) or {"completed_areas": [], "results": {}}
        cursor.setdefault("completed_areas", [])
        cursor.setdefault("results", {})

        snap = resolve_commit(
            self.repo, payload["scenario_id"], payload["commit_id"]
        )
        # 续跑前重新计算指纹：若情景已前进，作业仍锚定原提交（不可变）。
        fingerprint = self.versions.forecast_fingerprint(snap)
        if fingerprint != payload["fingerprint"]:
            raise InvalidStateError("作业指纹与情景解析结果不符")

        params = self.repo.load_object(snap.parameters_version_id)
        mapping = self.repo.load_object(snap.mapping_version_id)
        engine = Forecaster(params, mapping)
        resolved = load_resolved_inputs(self.repo, snap)
        by_area, anchor, area_order = engine.plan(resolved)

        total = len(area_order)
        for idx, area in enumerate(area_order, start=1):
            if area in cursor["completed_areas"]:
                continue
            cursor["results"][area] = engine.forecast_area(
                area, by_area[area], anchor
            )
            cursor["completed_areas"].append(area)
            self.heartbeat(job_id, cursor)  # 检查点：已完成服务区已持久化
            if self.step_hook is not None:
                self.step_hook(job_id, idx, total)

        forecast_payload = engine.assemble(
            resolved, snap.parameters_version_id, snap.mapping_version_id,
            cursor["results"], anchor,
        )
        return self._finalize(job_id, snap, fingerprint, forecast_payload)

    def _finalize(
        self, job_id: str, snap: Snapshot, fingerprint: str,
        forecast_payload: dict[str, Any],
    ) -> dict[str, Any]:
        with self.repo.db.transaction() as conn:
            # 可能由另一个 worker 抢先完成（同指纹预测全局唯一）。
            existing = self.repo.find_forecast_by_fingerprint(conn, fingerprint)
            if existing is None:
                fvid, _oid, _ = self.repo.register_version(
                    conn, "forecast", forecast_payload,
                    note=f"情景 {snap.scenario_id}@{snap.commit_id}",
                    created_by="forecast-engine", now=self._now(),
                )
                conn.execute(
                    "INSERT INTO forecasts(version_id,fingerprint,created_at) "
                    "VALUES(?,?,?)",
                    (fvid, fingerprint, self._now()),
                )
                for ref in forecast_payload["inputs"]:
                    conn.execute(
                        "INSERT INTO forecast_inputs(forecast_version_id,"
                        "source_key,input_version_id) VALUES(?,?,?)",
                        (fvid, ref["source_key"], ref["version_id"]),
                    )
            else:
                fvid = existing["version_id"]

            run_id = self.idgen.new_id("run")
            try:
                conn.execute(
                    "INSERT INTO forecast_runs(id,forecast_version_id,"
                    "scenario_id,scenario_commit_id,job_id,fingerprint,"
                    "created_at) VALUES(?,?,?,?,?,?,?)",
                    (run_id, fvid, snap.scenario_id, snap.commit_id,
                     job_id, fingerprint, self._now()),
                )
            except sqlite3.IntegrityError:
                # 该提交上的运行已由并发 worker 登记：复用。
                row = conn.execute(
                    "SELECT id FROM forecast_runs "
                    "WHERE scenario_commit_id=? AND forecast_version_id=?",
                    (snap.commit_id, fvid),
                ).fetchone()
                run_id = row["id"]

            result = json_dumps({
                "run_id": run_id, "forecast_version_id": fvid,
                "scenario_commit_id": snap.commit_id,
                "fingerprint": fingerprint,
            })
            conn.execute(
                "UPDATE jobs SET status='succeeded', result=?, error=NULL, "
                "finished_at=?, updated_at=?, lease_owner=NULL, "
                "lease_expires_at=NULL WHERE id=?",
                (result, self._now(), self._now(), job_id),
            )

            # 自动核销：同情景下、使用了“更正后版本”的新预测完成时，
            # 把旧预测上受该更正影响的标记核销并指向新预测。
            new_inputs = [
                (r["source_key"], r["version_id"])
                for r in forecast_payload["inputs"]
            ]
            for _src, inp_vid in new_inputs:
                conn.execute(
                    "UPDATE invalidation_marks SET status='resolved', "
                    "resolved_at=?, resolution_forecast_version_id=? "
                    "WHERE id IN ("
                    "  SELECT m.id FROM invalidation_marks m "
                    "  JOIN corrections c ON c.id=m.correction_id "
                    "  JOIN forecast_runs fr ON fr.forecast_version_id="
                    "m.forecast_version_id "
                    "  WHERE m.status='affected' AND fr.scenario_id=? "
                    "  AND c.correction_version_id=?)",
                    (self._now(), fvid, snap.scenario_id, inp_vid),
                )
        return {"job_id": job_id, "status": "succeeded",
                "result": json_loads(result)}

    def fail_job(self, job_id: str, error: str) -> None:
        with self.repo.db.transaction() as conn:
            conn.execute(
                "UPDATE jobs SET status='failed', error=?, finished_at=?, "
                "updated_at=? WHERE id=?",
                (error, self._now(), self._now(), job_id),
            )

    def run_available(self, owner: str | None = None, limit: int = 100) -> int:
        """工作循环：认领并执行所有可运行作业，返回处理数量。"""
        processed = 0
        for _ in range(limit):
            claimed = self.acquire_job(owner)
            if claimed is None:
                break
            try:
                self.run_job(claimed["job_id"], owner=owner)
            except Exception as exc:  # noqa: BLE001 - 记录后继续处理下一作业
                self.fail_job(claimed["job_id"], f"{type(exc).__name__}: {exc}")
            processed += 1
        return processed

    def job_status(self, job_id: str) -> dict[str, Any]:
        row = self.repo.get_job(job_id)
        return {
            "job_id": job_id, "kind": row["kind"], "status": row["status"],
            "attempts": row["attempts"], "error": row["error"],
            "lease_owner": row["lease_owner"],
            "lease_expires_at": row["lease_expires_at"],
            "cursor": json_loads(row["cursor"]),
            "result": json_loads(row["result"]),
            "created_at": row["created_at"], "finished_at": row["finished_at"],
        }
