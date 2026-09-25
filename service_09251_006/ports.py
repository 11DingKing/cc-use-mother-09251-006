"""可替换端口：时间、标识等外部依赖的注入点。

生产环境使用真实时钟与随机标识；测试注入固定时钟与序列标识，
保证状态变化可以稳定复现。
"""
from __future__ import annotations

import itertools
import threading
import uuid
from datetime import datetime, timedelta, timezone


def to_iso(moment: datetime) -> str:
    """统一转换为带时区的 ISO 8601 文本（UTC，Z 结尾）。"""
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=timezone.utc)
    return moment.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def parse_iso(text: str) -> datetime:
    """解析 ISO 8601 文本；裸时间按 UTC 处理。"""
    moment = datetime.fromisoformat(str(text).replace("Z", "+00:00"))
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=timezone.utc)
    return moment


class Clock:
    """时间端口。"""

    def now(self) -> datetime:
        return datetime.now(timezone.utc)

    def now_iso(self) -> str:
        return to_iso(self.now())


class FixedClock(Clock):
    """测试时钟：从固定起点按步长推进，调用可复现。"""

    def __init__(self, start: str = "2026-09-25T00:00:00Z", step_seconds: float = 1.0):
        self._current = parse_iso(start)
        self._step = timedelta(seconds=step_seconds)
        self._lock = threading.Lock()

    def now(self) -> datetime:
        with self._lock:
            moment = self._current
            self._current = self._current + self._step
            return moment


class IdGenerator:
    """标识端口。"""

    def new_id(self, prefix: str) -> str:
        raise NotImplementedError


class UuidIds(IdGenerator):
    """生产标识：随机且足够短，便于人工引用。"""

    def new_id(self, prefix: str) -> str:
        return f"{prefix}_{uuid.uuid4().hex[:12]}"


class SequentialIds(IdGenerator):
    """测试标识：确定性递增。"""

    def __init__(self) -> None:
        self._counter = itertools.count(1)
        self._lock = threading.Lock()

    def new_id(self, prefix: str) -> str:
        with self._lock:
            return f"{prefix}_{next(self._counter):06d}"
