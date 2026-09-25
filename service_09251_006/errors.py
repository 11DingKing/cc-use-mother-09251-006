"""领域与应用层抛出的错误，携带稳定的机器可读代码。"""
from __future__ import annotations


class VersionError(Exception):
    """所有业务错误的基类。"""

    code = "error"

    def __init__(self, message: str, **details: object) -> None:
        super().__init__(message)
        self.message = message
        self.details = details

    def to_dict(self) -> dict[str, object]:
        data: dict[str, object] = {"code": self.code, "message": self.message}
        if self.details:
            data["details"] = self.details
        return data


class NotFoundError(VersionError):
    code = "not_found"


class ConflictError(VersionError):
    code = "conflict"


class ValidationError(VersionError):
    code = "validation_error"


class InvalidStateError(VersionError):
    code = "invalid_state"


class LineageError(VersionError):
    code = "lineage_violation"
