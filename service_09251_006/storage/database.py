"""SQLite 元数据库：登记表、情景提交链、作业、失效标记、签署事件。

设计原则：
* 元数据只保存 *指针*（指向对象存储中的不可变内容对象）与 *追加事件*；
* 唯一索引承担并发去重（重复批次、并发派生）；
* 任何状态变化（签署、撤销、失效、核销）都是插入新行，不覆盖历史。
"""
from __future__ import annotations

import json
import os
import sqlite3
from contextlib import contextmanager
from typing import Any, Iterator

SCHEMA_VERSION = 1

SCHEMA = """
PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

-- 所有已登记的不可变版本（输入/参数/映射/预测/决策）
CREATE TABLE IF NOT EXISTS versions (
    id TEXT PRIMARY KEY,
    version_no INTEGER NOT NULL,
    kind TEXT NOT NULL CHECK (kind IN
        ('raw_input','parameters','region_map','forecast','decision')),
    object_id TEXT NOT NULL,
    source_key TEXT,
    corrects_version_id TEXT,
    note TEXT,
    created_by TEXT,
    created_at TEXT NOT NULL,
    UNIQUE(kind, object_id)
);

-- 情景提交链：每个提交只叠加一次变化，沿 parent 链解析快照
CREATE TABLE IF NOT EXISTS scenario_commits (
    id TEXT PRIMARY KEY,
    scenario_id TEXT NOT NULL,
    seq INTEGER NOT NULL,
    parent_commit_id TEXT,
    change_type TEXT NOT NULL CHECK (change_type IN
        ('root','add_input','replace_input','set_parameters','set_mapping')),
    input_version_id TEXT,
    replaces_version_id TEXT,
    parameters_version_id TEXT,
    mapping_version_id TEXT,
    message TEXT,
    created_by TEXT,
    created_at TEXT NOT NULL,
    UNIQUE(scenario_id, seq)
);

-- 并发/重复派生去重：同一情景、同一父提交上的相同变化只能成功一次
CREATE UNIQUE INDEX IF NOT EXISTS ux_commit_derivation
ON scenario_commits (
    scenario_id,
    COALESCE(parent_commit_id, ''),
    change_type,
    COALESCE(input_version_id, ''),
    COALESCE(replaces_version_id, ''),
    COALESCE(parameters_version_id, ''),
    COALESCE(mapping_version_id, '')
);

CREATE TABLE IF NOT EXISTS forecasts (
    version_id TEXT PRIMARY KEY REFERENCES versions(id),
    -- 由输入/参数/映射版本与引擎版本确定性派生；同内容预测全局唯一
    fingerprint TEXT NOT NULL UNIQUE,
    created_at TEXT NOT NULL
);

-- 每次“在某情景提交上请求预测”的运行记录（同一预测内容可在多处复用）
CREATE TABLE IF NOT EXISTS forecast_runs (
    id TEXT PRIMARY KEY,
    forecast_version_id TEXT NOT NULL REFERENCES forecasts(version_id),
    scenario_id TEXT NOT NULL,
    scenario_commit_id TEXT NOT NULL,
    job_id TEXT,
    fingerprint TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE(scenario_commit_id, forecast_version_id)
);
CREATE INDEX IF NOT EXISTS ix_runs_scenario ON forecast_runs(scenario_id);

-- 预测实际使用的输入版本（按数据源解析后），用于更正影响传播
CREATE TABLE IF NOT EXISTS forecast_inputs (
    forecast_version_id TEXT NOT NULL,
    source_key TEXT NOT NULL,
    input_version_id TEXT NOT NULL,
    PRIMARY KEY (forecast_version_id, source_key)
);
CREATE INDEX IF NOT EXISTS ix_forecast_inputs_version
ON forecast_inputs(input_version_id);

CREATE TABLE IF NOT EXISTS corrections (
    id TEXT PRIMARY KEY,
    correction_version_id TEXT NOT NULL UNIQUE,
    original_version_id TEXT NOT NULL,
    reason TEXT,
    created_by TEXT,
    created_at TEXT NOT NULL
);

-- 追加式失效标记：受影响 -> 已核销（重算后）
CREATE TABLE IF NOT EXISTS invalidation_marks (
    id TEXT PRIMARY KEY,
    correction_id TEXT NOT NULL,
    forecast_version_id TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('affected','resolved')),
    marked_at TEXT NOT NULL,
    resolved_at TEXT,
    resolution_forecast_version_id TEXT,
    UNIQUE(correction_id, forecast_version_id)
);
CREATE INDEX IF NOT EXISTS ix_marks_forecast ON invalidation_marks(forecast_version_id);

-- 签署/撤销为追加事件，每预测的最新事件即当前状态
CREATE TABLE IF NOT EXISTS adoption_events (
    id TEXT PRIMARY KEY,
    forecast_version_id TEXT NOT NULL,
    decision_version_id TEXT,
    action TEXT NOT NULL CHECK (action IN ('adopted','revoked')),
    actor TEXT,
    reason TEXT,
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_adoption_forecast ON adoption_events(forecast_version_id);

-- 可重入长作业：租约 + 游标检查点
CREATE TABLE IF NOT EXISTS jobs (
    id TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN
        ('queued','running','succeeded','failed')),
    idempotency_key TEXT UNIQUE,
    payload TEXT NOT NULL,
    cursor TEXT,
    result TEXT,
    error TEXT,
    attempts INTEGER NOT NULL DEFAULT 0,
    lease_owner TEXT,
    lease_expires_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    started_at TEXT,
    finished_at TEXT
);
CREATE INDEX IF NOT EXISTS ix_jobs_status ON jobs(status);
"""


class Database:
    def __init__(self, path: str) -> None:
        self.path = os.path.abspath(path)
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        self._init_schema()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(
            self.path, timeout=30, isolation_level=None, check_same_thread=False
        )
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA busy_timeout=30000")
        conn.execute("PRAGMA journal_mode=WAL")
        return conn

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            yield conn
            conn.commit()
        except BaseException:
            conn.rollback()
            raise
        finally:
            conn.close()

    @contextmanager
    def session(self) -> Iterator[sqlite3.Connection]:
        """自动提交的短连接（只读或单语句写入）。"""
        conn = self._connect()
        try:
            yield conn
        finally:
            conn.close()

    def _init_schema(self) -> None:
        conn = self._connect()
        try:
            conn.executescript(SCHEMA)
            row = conn.execute(
                "SELECT value FROM meta WHERE key='schema_version'"
            ).fetchone()
            if row is None:
                conn.execute(
                    "INSERT INTO meta(key, value) VALUES('schema_version', ?)",
                    (str(SCHEMA_VERSION),),
                )
        finally:
            conn.close()

    # ---- 版本号分配（原子计数）---------------------------------------

    def next_version_no(self, conn: sqlite3.Connection) -> int:
        conn.execute(
            "INSERT INTO meta(key,value) VALUES('version_seq','0') "
            "ON CONFLICT(key) DO NOTHING"
        )
        row = conn.execute(
            "UPDATE meta SET value=CAST(CAST(value AS INTEGER)+1 AS TEXT) "
            "WHERE key='version_seq' RETURNING value"
        ).fetchone()
        return int(row["value"])


def json_dumps(data: Any) -> str:
    return json.dumps(data, sort_keys=True, ensure_ascii=False, separators=(",", ":"))


def json_loads(text: str | None) -> Any:
    return json.loads(text) if text is not None else None
