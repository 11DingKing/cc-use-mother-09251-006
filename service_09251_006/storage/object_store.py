"""只追加、内容寻址的不可变对象存储（WORM）。

对象以其内容哈希命名，原子落盘（临时文件 + ``os.replace``，并对最终
路径使用 ``O_CREAT|O_EXCL`` 风格的存在性检查）。相同内容重复写入是
幂等的：返回同一对象 ID，不产生第二份数据，也不允许通过同 ID 写入
不同内容——任何冲突都会抛出 :class:`IntegrityError`。
"""
from __future__ import annotations

import hashlib
import json
import os
import tempfile
from typing import Any

from ..errors import ConflictError
from ..hashing import canonical_dumps, content_hash

# 对象类型 -> 单字符目录前缀，便于人工浏览 objects 目录。
_KIND_PREFIX = {
    "raw_batch": "rb",
    "parameters": "pa",
    "region_map": "rm",
    "forecast": "fc",
    "decision": "dc",
}


class ObjectStore:
    def __init__(self, root: str) -> None:
        self.root = os.path.abspath(root)
        os.makedirs(self.root, exist_ok=True)

    # ---- 路径布局 -----------------------------------------------------

    def _relpath(self, object_id: str) -> str:
        kind, digest = object_id.split("-", 1)
        prefix = _KIND_PREFIX.get(kind, "ob")
        return os.path.join(prefix, digest[:2], f"{object_id}.json")

    def path_for(self, object_id: str) -> str:
        return os.path.join(self.root, self._relpath(object_id))

    # ---- 写入 ---------------------------------------------------------

    def put(self, kind: str, payload: dict[str, Any]) -> str:
        """写入不可变对象，返回对象 ID（``<kind>-<hash>``）。

        相同载荷幂等；相同 ID 不同内容冲突。
        """
        body = dict(payload)
        body.setdefault("_kind", kind)
        data = canonical_dumps(body)
        digest = hashlib.sha256(data).hexdigest()
        object_id = f"{kind}-{digest}"
        path = self.path_for(object_id)
        os.makedirs(os.path.dirname(path), exist_ok=True)

        if os.path.exists(path):
            with open(path, "rb") as fh:
                existing = fh.read()
            if existing != data:
                raise ConflictError(
                    "对象 ID 相同但内容不一致，存储可能已损坏",
                    object_id=object_id,
                )
            return object_id  # 幂等：重复批次/重复计算

        # 同目录临时文件 + 原子替换，崩溃时不留半成品。
        fd, tmp = tempfile.mkstemp(prefix=".tmp-", dir=os.path.dirname(path))
        try:
            with os.fdopen(fd, "wb") as fh:
                fh.write(data)
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp, path)
        except BaseException:
            _unlink_quiet(tmp)
            raise
        return object_id

    # ---- 读取 ---------------------------------------------------------

    def exists(self, object_id: str) -> bool:
        return os.path.exists(self.path_for(object_id))

    def get(self, object_id: str) -> dict[str, Any]:
        path = self.path_for(object_id)
        try:
            with open(path, "rb") as fh:
                raw = fh.read()
            body = json.loads(raw.decode("utf-8"))
        except FileNotFoundError:
            from ..errors import NotFoundError

            raise NotFoundError("对象不存在", object_id=object_id) from None
        except (json.JSONDecodeError, UnicodeDecodeError):
            from ..errors import LineageError

            raise LineageError(
                "对象文件无法解析，历史数据疑似被篡改或损坏",
                object_id=object_id,
            ) from None
        # 读取即校验：检测静默损坏/被外部篡改。
        stored_id = object_id
        kind, digest = stored_id.split("-", 1)
        actual = content_hash(body)
        if actual != digest:
            from ..errors import LineageError

            raise LineageError(
                "对象内容与哈希不符，历史数据疑似被篡改",
                object_id=object_id,
            )
        if body.get("_kind") != kind:
            from ..errors import LineageError

            raise LineageError(
                "对象类型标记与 ID 前缀不符", object_id=object_id
            )
        return body

    def list_ids(self, kind: str) -> list[str]:
        """列出某类型全部对象 ID（管理/核验命令使用）。"""
        prefix = _KIND_PREFIX.get(kind, "ob")
        base = os.path.join(self.root, prefix)
        found: list[str] = []
        if not os.path.isdir(base):
            return found
        for dirpath, _dirs, files in os.walk(base):
            for name in files:
                if name.endswith(".json"):
                    found.append(name[:-5])
        return sorted(found)


def _unlink_quiet(path: str) -> None:
    try:
        os.unlink(path)
    except OSError:
        pass


__all__ = ["ObjectStore"]
