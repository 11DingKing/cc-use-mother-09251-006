"""领域模型：版本状态机、任务状态与领域错误。

所有持久化对象不可变：状态迁移只追加标记（如 stale、签署、撤销），
已固化的输入、参数、映射与计算结果永不改写。
"""
from __future__ import annotations

from enum import Enum


class VersionStatus(str, Enum):
    DRAFT = "DRAFT"        # 已创建，尚未计算
    COMPUTED = "COMPUTED"  # 结果已固化，可签署
    SIGNED = "SIGNED"      # 已签署，可被调度决策采用
    REVOKED = "REVOKED"    # 已撤销（终态），历史保留


class JobStatus(str, Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    DONE = "DONE"
    FAILED = "FAILED"


ACTIVE_JOB_STATUSES = (JobStatus.PENDING.value, JobStatus.RUNNING.value)


class DomainError(Exception):
    """领域错误基类；code 供 API 映射状态码。"""

    code = "domain_error"

    def __init__(self, message: str, *, details: dict | None = None):
        super().__init__(message)
        self.message = message
        self.details = dict(details or {})


class NotFoundError(DomainError):
    code = "not_found"


class ValidationError(DomainError):
    code = "validation"


class ConflictError(DomainError):
    """状态机冲突：非法迁移或重复占用。"""
    code = "conflict"


class NotSignedError(ConflictError):
    """调度决策只能采用已签署版本。"""
    code = "version_not_signed"


class StaleVersionError(ConflictError):
    """版本受上游数据更正影响，禁止再被采用。"""
    code = "version_stale"


class JobInterrupted(Exception):
    """长任务在检查点之间被中断；进度已持久化，可随时续跑。"""
