"""持久化层：SQLite 仓储。

设计要点：
- 所有表只增不改（状态字段的迁移除外），历史不可篡改；
- 多步写入通过 tx() 事务完成，失败整体回滚；
- 连接按线程隔离，WAL + busy_timeout 支撑并发派生与读写并行；
- 内容哈希唯一约束保证重复导入幂等。
"""
from __future__ import annotations

import os
import sqlite3
import threading
from contextlib import contextmanager

SCHEMA = """
PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS batches (
    batch_id          TEXT PRIMARY KEY,
    content_hash      TEXT NOT NULL UNIQUE,
    source            TEXT NOT NULL,
    imported_at       TEXT NOT NULL,
    record_count      INTEGER NOT NULL,
    corrects_batch_id TEXT,
    note              TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS batch_records (
    batch_id    TEXT NOT NULL REFERENCES batches(batch_id),
    station_id  TEXT NOT NULL,
    observed_at TEXT NOT NULL,
    day         TEXT NOT NULL,
    hour        INTEGER NOT NULL,
    vehicles    INTEGER NOT NULL,
    sessions    INTEGER NOT NULL,
    energy_kwh  REAL NOT NULL,
    PRIMARY KEY (batch_id, station_id, observed_at)
);
CREATE INDEX IF NOT EXISTS idx_records_station ON batch_records(batch_id, station_id);

CREATE TABLE IF NOT EXISTS corrections (
    correction_id       TEXT PRIMARY KEY,
    target_batch_id     TEXT NOT NULL REFERENCES batches(batch_id),
    correction_batch_id TEXT REFERENCES batches(batch_id),
    station_ids         TEXT,
    range_start         TEXT,
    range_end           TEXT,
    reason              TEXT NOT NULL,
    registered_at       TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS parameter_sets (
    param_id     TEXT PRIMARY KEY,
    payload      TEXT NOT NULL,
    content_hash TEXT NOT NULL UNIQUE,
    created_at   TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS region_mappings (
    mapping_id   TEXT PRIMARY KEY,
    entries      TEXT NOT NULL,
    content_hash TEXT NOT NULL UNIQUE,
    created_at   TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS versions (
    version_id        TEXT PRIMARY KEY,
    parent_version_id TEXT REFERENCES versions(version_id),
    parent_key        TEXT NOT NULL,
    seq               INTEGER NOT NULL,
    scenario          TEXT NOT NULL,
    param_id          TEXT NOT NULL REFERENCES parameter_sets(param_id),
    mapping_id        TEXT NOT NULL REFERENCES region_mappings(mapping_id),
    batch_ids         TEXT NOT NULL,
    status            TEXT NOT NULL,
    stale             INTEGER NOT NULL DEFAULT 0,
    stale_reason      TEXT,
    result_hash       TEXT,
    created_at        TEXT NOT NULL,
    signed_at         TEXT,
    signed_by         TEXT,
    revoked_at        TEXT,
    revoked_by        TEXT,
    revoke_reason     TEXT,
    UNIQUE (parent_key, seq)
);
CREATE INDEX IF NOT EXISTS idx_versions_parent ON versions(parent_key);

CREATE TABLE IF NOT EXISTS results (
    version_id   TEXT NOT NULL REFERENCES versions(version_id),
    service_area TEXT NOT NULL,
    hour         INTEGER NOT NULL,
    days         INTEGER NOT NULL,
    mean_kwh     REAL NOT NULL,
    std_kwh      REAL NOT NULL,
    ci_low       REAL NOT NULL,
    ci_high      REAL NOT NULL,
    is_peak      INTEGER NOT NULL,
    PRIMARY KEY (version_id, service_area, hour)
);

CREATE TABLE IF NOT EXISTS decisions (
    decision_id  TEXT PRIMARY KEY,
    version_id   TEXT NOT NULL REFERENCES versions(version_id),
    service_area TEXT NOT NULL,
    action       TEXT NOT NULL,
    units        INTEGER NOT NULL,
    decided_by   TEXT NOT NULL,
    decided_at   TEXT NOT NULL,
    note         TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_decisions_version ON decisions(version_id);

CREATE TABLE IF NOT EXISTS jobs (
    job_id     TEXT PRIMARY KEY,
    kind       TEXT NOT NULL,
    version_id TEXT NOT NULL REFERENCES versions(version_id),
    status     TEXT NOT NULL,
    total      INTEGER NOT NULL,
    done       INTEGER NOT NULL DEFAULT 0,
    error      TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
-- 同一版本同一时间至多一个活动任务，保证重复发起幂等。
CREATE UNIQUE INDEX IF NOT EXISTS idx_jobs_active_version
    ON jobs(version_id) WHERE status IN ('PENDING', 'RUNNING');

CREATE TABLE IF NOT EXISTS job_steps (
    job_id      TEXT NOT NULL REFERENCES jobs(job_id),
    step_key    TEXT NOT NULL,
    finished_at TEXT NOT NULL,
    PRIMARY KEY (job_id, step_key)
);
"""


class Repository:
    """SQLite 仓储：所有方法在调用方事务（若有）内执行。"""

    def __init__(self, path: str):
        self.path = path
        if path != ":memory:":
            os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        self._local = threading.local()
        self._conn().executescript(SCHEMA)

    # ---- 连接与事务 -------------------------------------------------

    def _conn(self) -> sqlite3.Connection:
        conn = getattr(self._local, "conn", None)
        if conn is None:
            conn = sqlite3.connect(self.path, isolation_level=None, check_same_thread=False)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA busy_timeout=10000")
            conn.execute("PRAGMA foreign_keys=ON")
            self._local.conn = conn
            self._local.depth = 0
        return conn

    @contextmanager
    def tx(self):
        """写事务（BEGIN IMMEDIATE）。嵌套调用并入外层事务。"""
        conn = self._conn()
        if self._local.depth > 0:
            self._local.depth += 1
            try:
                yield conn
            finally:
                self._local.depth -= 1
            return
        conn.execute("BEGIN IMMEDIATE")
        self._local.depth = 1
        try:
            yield conn
            conn.execute("COMMIT")
        except BaseException:
            conn.execute("ROLLBACK")
            raise
        finally:
            self._local.depth = 0

    def close(self) -> None:
        conn = getattr(self._local, "conn", None)
        if conn is not None:
            conn.close()
            self._local.conn = None
            self._local.depth = 0

    # ---- 批次与记录 -------------------------------------------------

    def insert_batch(self, *, batch_id, content_hash, source, imported_at,
                     record_count, corrects_batch_id, note) -> None:
        self._conn().execute(
            "INSERT INTO batches (batch_id, content_hash, source, imported_at,"
            " record_count, corrects_batch_id, note) VALUES (?,?,?,?,?,?,?)",
            (batch_id, content_hash, source, imported_at, record_count,
             corrects_batch_id, note),
        )

    def batch_by_hash(self, content_hash: str):
        row = self._conn().execute(
            "SELECT * FROM batches WHERE content_hash=?", (content_hash,)).fetchone()
        return dict(row) if row else None

    def get_batch(self, batch_id: str):
        row = self._conn().execute(
            "SELECT * FROM batches WHERE batch_id=?", (batch_id,)).fetchone()
        return dict(row) if row else None

    def list_batches(self) -> list[dict]:
        rows = self._conn().execute(
            "SELECT * FROM batches ORDER BY imported_at, batch_id").fetchall()
        return [dict(r) for r in rows]

    def insert_records(self, batch_id: str, records) -> None:
        self._conn().executemany(
            "INSERT INTO batch_records (batch_id, station_id, observed_at, day, hour,"
            " vehicles, sessions, energy_kwh) VALUES (?,?,?,?,?,?,?,?)",
            [
                (batch_id, r["station_id"], r["observed_at"], r["day"], r["hour"],
                 r["vehicles"], r["sessions"], r["energy_kwh"])
                for r in records
            ],
        )

    def records_for_batches(self, batch_ids) -> list[dict]:
        if not batch_ids:
            return []
        marks = ",".join("?" for _ in batch_ids)
        rows = self._conn().execute(
            f"SELECT * FROM batch_records WHERE batch_id IN ({marks})"
            " ORDER BY station_id, observed_at",
            tuple(batch_ids),
        ).fetchall()
        return [dict(r) for r in rows]

    def batch_stations(self, batch_id: str) -> set[str]:
        rows = self._conn().execute(
            "SELECT DISTINCT station_id FROM batch_records WHERE batch_id=?",
            (batch_id,)).fetchall()
        return {r["station_id"] for r in rows}

    def batch_stations_in_range(self, batch_id: str, start, end) -> set[str]:
        sql = "SELECT DISTINCT station_id FROM batch_records WHERE batch_id=?"
        args: list = [batch_id]
        if start:
            sql += " AND observed_at>=?"
            args.append(start)
        if end:
            sql += " AND observed_at<=?"
            args.append(end)
        rows = self._conn().execute(sql, args).fetchall()
        return {r["station_id"] for r in rows}

    # ---- 更正 -------------------------------------------------------

    def insert_correction(self, *, correction_id, target_batch_id, correction_batch_id,
                          station_ids, range_start, range_end, reason, registered_at) -> None:
        self._conn().execute(
            "INSERT INTO corrections (correction_id, target_batch_id, correction_batch_id,"
            " station_ids, range_start, range_end, reason, registered_at)"
            " VALUES (?,?,?,?,?,?,?,?)",
            (correction_id, target_batch_id, correction_batch_id, station_ids,
             range_start, range_end, reason, registered_at),
        )

    def get_correction(self, correction_id: str):
        row = self._conn().execute(
            "SELECT * FROM corrections WHERE correction_id=?", (correction_id,)).fetchone()
        return dict(row) if row else None

    def list_corrections(self) -> list[dict]:
        rows = self._conn().execute(
            "SELECT * FROM corrections ORDER BY registered_at, correction_id").fetchall()
        return [dict(r) for r in rows]

    # ---- 参数与映射 -------------------------------------------------

    def insert_params(self, *, param_id, payload, content_hash, created_at) -> None:
        self._conn().execute(
            "INSERT INTO parameter_sets (param_id, payload, content_hash, created_at)"
            " VALUES (?,?,?,?)",
            (param_id, payload, content_hash, created_at),
        )

    def params_by_hash(self, content_hash: str):
        row = self._conn().execute(
            "SELECT * FROM parameter_sets WHERE content_hash=?", (content_hash,)).fetchone()
        return dict(row) if row else None

    def get_params(self, param_id: str):
        row = self._conn().execute(
            "SELECT * FROM parameter_sets WHERE param_id=?", (param_id,)).fetchone()
        return dict(row) if row else None

    def insert_mapping(self, *, mapping_id, entries, content_hash, created_at) -> None:
        self._conn().execute(
            "INSERT INTO region_mappings (mapping_id, entries, content_hash, created_at)"
            " VALUES (?,?,?,?)",
            (mapping_id, entries, content_hash, created_at),
        )

    def mapping_by_hash(self, content_hash: str):
        row = self._conn().execute(
            "SELECT * FROM region_mappings WHERE content_hash=?", (content_hash,)).fetchone()
        return dict(row) if row else None

    def get_mapping(self, mapping_id: str):
        row = self._conn().execute(
            "SELECT * FROM region_mappings WHERE mapping_id=?", (mapping_id,)).fetchone()
        return dict(row) if row else None

    # ---- 版本 -------------------------------------------------------

    def insert_version(self, *, version_id, parent_version_id, parent_key, seq, scenario,
                       param_id, mapping_id, batch_ids, status, stale, stale_reason,
                       created_at) -> None:
        self._conn().execute(
            "INSERT INTO versions (version_id, parent_version_id, parent_key, seq, scenario,"
            " param_id, mapping_id, batch_ids, status, stale, stale_reason, created_at)"
            " VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            (version_id, parent_version_id, parent_key, seq, scenario, param_id,
             mapping_id, batch_ids, status, stale, stale_reason, created_at),
        )

    def get_version(self, version_id: str):
        row = self._conn().execute(
            "SELECT * FROM versions WHERE version_id=?", (version_id,)).fetchone()
        return dict(row) if row else None

    def list_versions(self) -> list[dict]:
        rows = self._conn().execute(
            "SELECT * FROM versions ORDER BY created_at, version_id").fetchall()
        return [dict(r) for r in rows]

    def children_of(self, version_id: str) -> list[dict]:
        rows = self._conn().execute(
            "SELECT * FROM versions WHERE parent_version_id=? ORDER BY seq",
            (version_id,)).fetchall()
        return [dict(r) for r in rows]

    def next_seq(self, parent_key: str) -> int:
        row = self._conn().execute(
            "SELECT COALESCE(MAX(seq),0)+1 AS seq FROM versions WHERE parent_key=?",
            (parent_key,)).fetchone()
        return int(row["seq"])

    def set_computed(self, version_id: str, result_hash: str) -> int:
        cur = self._conn().execute(
            "UPDATE versions SET status='COMPUTED', result_hash=?"
            " WHERE version_id=? AND status='DRAFT'",
            (result_hash, version_id),
        )
        return cur.rowcount

    def set_signed(self, version_id: str, signed_by: str, signed_at: str) -> int:
        cur = self._conn().execute(
            "UPDATE versions SET status='SIGNED', signed_by=?, signed_at=?"
            " WHERE version_id=? AND status='COMPUTED'",
            (signed_by, signed_at, version_id),
        )
        return cur.rowcount

    def set_revoked(self, version_id: str, revoked_by: str, revoked_at: str, reason: str) -> int:
        cur = self._conn().execute(
            "UPDATE versions SET status='REVOKED', revoked_by=?, revoked_at=?, revoke_reason=?"
            " WHERE version_id=? AND status IN ('COMPUTED','SIGNED')",
            (revoked_by, revoked_at, reason, version_id),
        )
        return cur.rowcount

    def mark_stale(self, version_id: str, reason: str) -> int:
        cur = self._conn().execute(
            "UPDATE versions SET stale=1, stale_reason=? WHERE version_id=? AND stale=0",
            (reason, version_id),
        )
        return cur.rowcount

    # ---- 结果 -------------------------------------------------------

    def insert_results(self, version_id: str, rows) -> None:
        self._conn().executemany(
            "INSERT OR IGNORE INTO results (version_id, service_area, hour, days,"
            " mean_kwh, std_kwh, ci_low, ci_high, is_peak) VALUES (?,?,?,?,?,?,?,?,?)",
            [
                (version_id, r["service_area"], r["hour"], r["days"], r["mean_kwh"],
                 r["std_kwh"], r["ci_low"], r["ci_high"], 1 if r["is_peak"] else 0)
                for r in rows
            ],
        )

    def get_results(self, version_id: str) -> list[dict]:
        rows = self._conn().execute(
            "SELECT * FROM results WHERE version_id=? ORDER BY service_area, hour",
            (version_id,)).fetchall()
        return [dict(r) for r in rows]

    def result_areas(self, version_id: str) -> list[str]:
        rows = self._conn().execute(
            "SELECT DISTINCT service_area FROM results WHERE version_id=? ORDER BY 1",
            (version_id,)).fetchall()
        return [r["service_area"] for r in rows]

    # ---- 调度决策 ---------------------------------------------------

    def insert_decision(self, *, decision_id, version_id, service_area, action,
                        units, decided_by, decided_at, note) -> None:
        self._conn().execute(
            "INSERT INTO decisions (decision_id, version_id, service_area, action,"
            " units, decided_by, decided_at, note) VALUES (?,?,?,?,?,?,?,?)",
            (decision_id, version_id, service_area, action, units,
             decided_by, decided_at, note),
        )

    def get_decision(self, decision_id: str):
        row = self._conn().execute(
            "SELECT * FROM decisions WHERE decision_id=?", (decision_id,)).fetchone()
        return dict(row) if row else None

    def list_decisions(self, version_id=None) -> list[dict]:
        if version_id is None:
            rows = self._conn().execute(
                "SELECT * FROM decisions ORDER BY decided_at, decision_id").fetchall()
        else:
            rows = self._conn().execute(
                "SELECT * FROM decisions WHERE version_id=? ORDER BY decided_at, decision_id",
                (version_id,)).fetchall()
        return [dict(r) for r in rows]

    # ---- 任务 -------------------------------------------------------

    def insert_job(self, *, job_id, kind, version_id, status, total,
                   created_at, updated_at) -> None:
        self._conn().execute(
            "INSERT INTO jobs (job_id, kind, version_id, status, total, done,"
            " created_at, updated_at) VALUES (?,?,?,?,?,0,?,?)",
            (job_id, kind, version_id, status, total, created_at, updated_at),
        )

    def get_job(self, job_id: str):
        row = self._conn().execute(
            "SELECT * FROM jobs WHERE job_id=?", (job_id,)).fetchone()
        return dict(row) if row else None

    def active_job_for_version(self, version_id: str):
        row = self._conn().execute(
            "SELECT * FROM jobs WHERE version_id=? AND status IN ('PENDING','RUNNING')"
            " ORDER BY created_at DESC LIMIT 1",
            (version_id,)).fetchone()
        return dict(row) if row else None

    def set_job_status(self, job_id: str, status: str, updated_at: str, error=None) -> None:
        self._conn().execute(
            "UPDATE jobs SET status=?, updated_at=?, error=? WHERE job_id=?",
            (status, updated_at, error, job_id),
        )

    def bump_job_done(self, job_id: str, updated_at: str) -> None:
        self._conn().execute(
            "UPDATE jobs SET done=done+1, updated_at=? WHERE job_id=?",
            (updated_at, job_id),
        )

    def insert_step(self, job_id: str, step_key: str, finished_at: str) -> None:
        self._conn().execute(
            "INSERT OR IGNORE INTO job_steps (job_id, step_key, finished_at) VALUES (?,?,?)",
            (job_id, step_key, finished_at),
        )

    def steps_done(self, job_id: str) -> set[str]:
        rows = self._conn().execute(
            "SELECT step_key FROM job_steps WHERE job_id=?", (job_id,)).fetchall()
        return {r["step_key"] for r in rows}
