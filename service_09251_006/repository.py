"""仓储层：把不可变对象存储与 SQLite 元数据组合起来的查询接口。"""
from __future__ import annotations

import sqlite3
from typing import Any, Iterable

from .errors import NotFoundError
from .storage.database import Database, json_loads
from .storage.object_store import ObjectStore


class Repository:
    def __init__(self, db: Database, objects: ObjectStore) -> None:
        self.db = db
        self.objects = objects

    # ---- 版本登记 -----------------------------------------------------

    def register_version(
        self,
        conn: sqlite3.Connection,
        kind: str,
        payload: dict[str, Any],
        *,
        source_key: str | None = None,
        corrects_version_id: str | None = None,
        note: str | None = None,
        created_by: str | None = None,
        now: str,
    ) -> tuple[str, str, bool]:
        """登记版本。返回 (version_id, object_id, created_new)。

        相同 (kind, object_id) 重复登记时直接返回已有版本（幂等）。
        """
        object_id = self.objects.put(_OBJECT_KIND[kind], payload)
        row = conn.execute(
            "SELECT id FROM versions WHERE kind=? AND object_id=?",
            (kind, object_id),
        ).fetchone()
        if row is not None:
            return row["id"], object_id, False
        no = self.db.next_version_no(conn)
        version_id = f"v{no:06d}"
        conn.execute(
            "INSERT INTO versions(id,version_no,kind,object_id,source_key,"
            "corrects_version_id,note,created_by,created_at) "
            "VALUES(?,?,?,?,?,?,?,?,?)",
            (version_id, no, kind, object_id, source_key, corrects_version_id,
             note, created_by, now),
        )
        return version_id, object_id, True

    def get_version_row(self, version_id: str) -> sqlite3.Row:
        with self.db.session() as conn:
            row = conn.execute(
                "SELECT * FROM versions WHERE id=?", (version_id,)
            ).fetchone()
        if row is None:
            raise NotFoundError("版本不存在", version_id=version_id)
        return row

    def require_kind(self, version_id: str, kind: str) -> sqlite3.Row:
        row = self.get_version_row(version_id)
        if row["kind"] != kind:
            from .errors import ValidationError

            raise ValidationError(
                "版本类型不符", version_id=version_id,
                expect=kind, got=row["kind"],
            )
        return row

    def load_object(self, version_id: str) -> dict[str, Any]:
        row = self.get_version_row(version_id)
        return self.objects.get(row["object_id"])

    def list_versions(self, kind: str | None = None) -> list[sqlite3.Row]:
        sql = "SELECT * FROM versions"
        args: tuple[Any, ...] = ()
        if kind:
            sql += " WHERE kind=?"
            args = (kind,)
        sql += " ORDER BY version_no"
        with self.db.session() as conn:
            return list(conn.execute(sql, args))

    # ---- 提交链 -------------------------------------------------------

    def insert_commit(self, conn: sqlite3.Connection, fields: dict[str, Any]) -> None:
        cols = ",".join(fields)
        marks = ",".join("?" for _ in fields)
        conn.execute(
            f"INSERT INTO scenario_commits({cols}) VALUES({marks})",
            tuple(fields.values()),
        )

    def list_commits(self, scenario_id: str) -> list[sqlite3.Row]:
        with self.db.session() as conn:
            rows = list(conn.execute(
                "SELECT * FROM scenario_commits WHERE scenario_id=? ORDER BY seq",
                (scenario_id,),
            ))
        if not rows:
            raise NotFoundError("情景不存在", scenario_id=scenario_id)
        return rows

    def head_commit(self, scenario_id: str) -> sqlite3.Row:
        return self.list_commits(scenario_id)[-1]

    def commit_by_id(self, commit_id: str) -> sqlite3.Row:
        with self.db.session() as conn:
            row = conn.execute(
                "SELECT * FROM scenario_commits WHERE id=?", (commit_id,)
            ).fetchone()
        if row is None:
            raise NotFoundError("提交不存在", commit_id=commit_id)
        return row

    def commit_exists(self, conn: sqlite3.Connection, commit_id: str) -> bool:
        return conn.execute(
            "SELECT 1 FROM scenario_commits WHERE id=?", (commit_id,)
        ).fetchone() is not None

    # ---- 预测 ---------------------------------------------------------

    def find_forecast_by_fingerprint(
        self, conn: sqlite3.Connection, fingerprint: str
    ) -> sqlite3.Row | None:
        return conn.execute(
            "SELECT * FROM forecasts WHERE fingerprint=?", (fingerprint,)
        ).fetchone()

    def get_forecast(self, forecast_version_id: str) -> dict[str, Any]:
        self.require_kind(forecast_version_id, "forecast")
        with self.db.session() as conn:
            row = conn.execute(
                "SELECT * FROM forecasts WHERE version_id=?", (forecast_version_id,)
            ).fetchone()
            inputs = list(conn.execute(
                "SELECT source_key,input_version_id FROM forecast_inputs "
                "WHERE forecast_version_id=? ORDER BY source_key",
                (forecast_version_id,),
            ))
            runs = list(conn.execute(
                "SELECT id,scenario_id,scenario_commit_id,job_id,created_at "
                "FROM forecast_runs WHERE forecast_version_id=? ORDER BY created_at",
                (forecast_version_id,),
            ))
        if row is None:
            raise NotFoundError("预测元数据缺失", version_id=forecast_version_id)
        payload = self.load_object(forecast_version_id)
        return {
            "version_id": forecast_version_id,
            "fingerprint": row["fingerprint"],
            "created_at": row["created_at"],
            "runs": [dict(r) for r in runs],
            "inputs": [dict(r) for r in inputs],
            "payload": payload,
        }

    def list_forecasts_for_scenario(
        self, scenario_id: str
    ) -> list[sqlite3.Row]:
        with self.db.session() as conn:
            return list(conn.execute(
                "SELECT r.*, f.fingerprint FROM forecast_runs r "
                "JOIN forecasts f ON f.version_id=r.forecast_version_id "
                "WHERE r.scenario_id=? ORDER BY r.created_at",
                (scenario_id,),
            ))

    # ---- 作业 ---------------------------------------------------------

    def get_job(self, job_id: str) -> sqlite3.Row:
        with self.db.session() as conn:
            row = conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
        if row is None:
            raise NotFoundError("作业不存在", job_id=job_id)
        return row

    @staticmethod
    def job_payload(row: sqlite3.Row) -> dict[str, Any]:
        return json_loads(row["payload"])

    @staticmethod
    def job_cursor(row: sqlite3.Row) -> dict[str, Any]:
        return json_loads(row["cursor"]) or {}


_OBJECT_KIND = {
    "raw_input": "raw_batch",
    "parameters": "parameters",
    "region_map": "region_map",
    "forecast": "forecast",
    "decision": "decision",
}


def rows_to_dicts(rows: Iterable[sqlite3.Row]) -> list[dict[str, Any]]:
    return [dict(r) for r in rows]
