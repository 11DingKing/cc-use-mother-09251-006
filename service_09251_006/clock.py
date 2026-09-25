"""可替换的时间与标识端口，保证状态变化可稳定复现。

业务代码不直接调用 ``datetime.now`` / ``uuid.uuid4``，而是通过这里的
:class:`Clock` 与 :class:`IdGenerator` 注入；测试中可冻结时间、使用确定序列。
"""
from __future__ import annotations

import threading
import uuid
from datetime import datetime, timezone
from typing import Protocol


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def now_iso() -> str:
    return utcnow().isoformat(timespec="microseconds")


class Clock(Protocol):
    def now(self) -> datetime: ...


class SystemClock:
    """墙钟，内部持有锁以便测试中可被替换为冻结时钟。"""

    def now(self) -> datetime:
        return utcnow()


class FrozenClock:
    def __init__(self, moment: datetime | str) -> None:
        if isinstance(moment, str):
            moment = datetime.fromisoformat(moment)
        if moment.tzinfo is None:
            moment = moment.replace(tzinfo=timezone.utc)
        self._moment = moment

    def now(self) -> datetime:
        return self._moment


class IdGenerator(Protocol):
    def new_id(self, prefix: str) -> str: ...


class Uuid4IdGenerator:
    def __init__(self) -> None:
        self._lock = threading.Lock()

    def new_id(self, prefix: str) -> str:
        with self._lock:
            return f"{prefix}_{uuid.uuid4().hex}"


class SequenceIdGenerator:
    """测试用：``prefix_000001`` 形式的确定 ID。"""

    def __init__(self) -> None:
        self._counters: dict[str, int] = {}
        self._lock = threading.Lock()

    def new_id(self, prefix: str) -> str:
        with self._lock:
            n = self._counters.get(prefix, 0) + 1
            self._counters[prefix] = n
            return f"{prefix}_{n:06d}"
