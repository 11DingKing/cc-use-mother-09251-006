"""应用装配：把存储、服务与可替换端口组装为一个应用对象。

运行数据目录通过环境变量 ``SERVICE_09251_DATA_DIR`` 指定；缺省使用
``~/.local/share/service_09251_006``，保证运行数据不写入源码目录。
"""
from __future__ import annotations

import os

from .clock import Clock, IdGenerator, SystemClock, Uuid4IdGenerator
from .repository import Repository
from .services.forecasting import ForecastService
from .services.versioning import VersionService
from .storage.database import Database
from .storage.object_store import ObjectStore

DEFAULT_DATA_ENV = "SERVICE_09251_DATA_DIR"


def default_data_dir() -> str:
    custom = os.environ.get(DEFAULT_DATA_ENV)
    if custom:
        return os.path.abspath(custom)
    base = os.environ.get("XDG_DATA_HOME") or os.path.expanduser("~/.local/share")
    return os.path.join(base, "service_09251_006")


class Application:
    """服务定位器：API 与 CLI 共用同一组服务。"""

    def __init__(
        self,
        data_dir: str | None = None,
        *,
        clock: Clock | None = None,
        idgen: IdGenerator | None = None,
        lease_seconds: float | None = None,
        step_hook=None,
    ) -> None:
        self.data_dir = os.path.abspath(data_dir or default_data_dir())
        os.makedirs(self.data_dir, exist_ok=True)
        self.clock = clock or SystemClock()
        self.idgen = idgen or Uuid4IdGenerator()

        self.db = Database(os.path.join(self.data_dir, "meta.db"))
        self.objects = ObjectStore(os.path.join(self.data_dir, "objects"))
        self.repo = Repository(self.db, self.objects)
        self.versions = VersionService(self.repo, self.clock, self.idgen)
        self.forecasts = ForecastService(
            self.repo, self.versions, self.clock, self.idgen,
            lease_seconds=float(lease_seconds or os.environ.get(
                "SERVICE_09251_LEASE_SECONDS", "30")),
            step_hook=step_hook,
        )

    # ---- 查询便捷方法 -------------------------------------------------

    def list_scenarios(self, limit: int = 200) -> list[dict[str, object]]:
        with self.db.session() as conn:
            rows = conn.execute(
                "SELECT scenario_id, COUNT(*) AS commits, MAX(seq) AS head_seq, "
                "MAX(created_at) AS last_activity "
                "FROM scenario_commits GROUP BY scenario_id "
                "ORDER BY last_activity DESC LIMIT ?",
                (limit,),
            ).fetchall()
        return [dict(r) for r in rows]

    def list_corrections(self) -> list[dict[str, object]]:
        with self.db.session() as conn:
            rows = conn.execute(
                "SELECT c.*, "
                "SUM(CASE WHEN m.status='affected' THEN 1 ELSE 0 END) "
                "  AS affected_count, "
                "SUM(CASE WHEN m.status='resolved' THEN 1 ELSE 0 END) "
                "  AS resolved_count "
                "FROM corrections c LEFT JOIN invalidation_marks m "
                "ON m.correction_id=c.id GROUP BY c.id ORDER BY c.created_at",
            ).fetchall()
        return [dict(r) for r in rows]

    def invalidations(self, forecast_version_id: str | None = None) -> list[dict]:
        sql = ("SELECT m.*, c.original_version_id, c.correction_version_id "
               "FROM invalidation_marks m JOIN corrections c "
               "ON c.id=m.correction_id")
        args: tuple = ()
        if forecast_version_id:
            sql += " WHERE m.forecast_version_id=?"
            args = (forecast_version_id,)
        sql += " ORDER BY m.marked_at"
        with self.db.session() as conn:
            return [dict(r) for r in conn.execute(sql, args)]
