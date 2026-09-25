"""规范化 JSON 序列化与内容寻址。

所有不可变对象都以 *规范化 JSON* 落盘：键按字典序排序、无多余空白、
UTF-8 不转义。对象地址（哈希）独立于内存中键的插入顺序，保证
``dict`` 插入顺序不同的两次提交得到相同内容 ID。
"""
from __future__ import annotations

import hashlib
import json
from typing import Any, Iterable

CONTENT_ALGO = "sha256"


def canonical_dumps(obj: Any) -> bytes:
    """把可 JSON 化对象序列化为确定性字节串。"""
    return json.dumps(
        obj,
        sort_keys=True,
        ensure_ascii=False,
        separators=(",", ":"),
        allow_nan=False,
        default=_json_default,
    ).encode("utf-8")


def _json_default(value: Any) -> Any:  # pragma: no cover - 防御
    raise TypeError(f"不可序列化的对象类型: {type(value).__name__}")


def content_hash(payload: dict[str, Any]) -> str:
    """计算内容对象的十六进制哈希。"""
    return hashlib.sha256(canonical_dumps(payload)).hexdigest()


def short_hash(payload: dict[str, Any], length: int = 12) -> str:
    return content_hash(payload)[:length]


def hash_manifest(parts: Iterable[tuple[str, str]]) -> str:
    """由若干 ``(名称, 哈希)`` 对计算清单哈希（顺序敏感）。"""
    h = hashlib.sha256()
    for name, digest in parts:
        h.update(name.encode("utf-8"))
        h.update(b"\x00")
        h.update(digest.encode("ascii"))
        h.update(b"\x00")
    return h.hexdigest()
