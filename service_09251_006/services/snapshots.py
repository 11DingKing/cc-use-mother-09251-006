"""情景快照：沿不可变提交链解析出某一时刻生效的输入/参数/映射版本集合。"""
from __future__ import annotations

import sqlite3
from dataclasses import dataclass, field

from ..errors import InvalidStateError, NotFoundError, ValidationError
from ..repository import Repository


@dataclass
class Snapshot:
    scenario_id: str
    commit_id: str
    seq: int
    inputs: dict[str, str] = field(default_factory=dict)  # source_key -> version_id
    parameters_version_id: str | None = None
    mapping_version_id: str | None = None

    def require_complete(self) -> None:
        if not self.inputs:
            raise InvalidStateError("情景尚未包含任何输入数据，无法预测")
        if self.parameters_version_id is None:
            raise InvalidStateError("情景尚未设置预测参数，无法预测")
        if self.mapping_version_id is None:
            raise InvalidStateError("情景尚未设置区域映射，无法预测")


def resolve_commit(
    repo: Repository, scenario_id: str, commit_id: str | None = None
) -> Snapshot:
    """从 root 沿链应用变更，得到指定提交（默认 HEAD）的快照。"""
    commits = repo.list_commits(scenario_id)
    if commit_id is not None and not any(c["id"] == commit_id for c in commits):
        raise NotFoundError("提交不属于该情景", scenario_id=scenario_id, commit_id=commit_id)
    snap = Snapshot(scenario_id=scenario_id, commit_id="", seq=-1)
    for c in commits:
        _apply_commit(repo, snap, c)
        if commit_id is not None and c["id"] == commit_id:
            snap.commit_id = c["id"]
            snap.seq = c["seq"]
            return snap
    head = commits[-1]
    if commit_id is None:
        snap.commit_id = head["id"]
        snap.seq = head["seq"]
    return snap


def _apply_commit(repo: Repository, snap: Snapshot, c: sqlite3.Row) -> None:
    kind = c["change_type"]
    if kind == "root":
        if c["parameters_version_id"]:
            snap.parameters_version_id = c["parameters_version_id"]
        if c["mapping_version_id"]:
            snap.mapping_version_id = c["mapping_version_id"]
        return
    if kind == "add_input":
        vid = c["input_version_id"]
        row = repo.require_kind(vid, "raw_input")
        key = row["source_key"]
        if key in snap.inputs:
            raise InvalidStateError(
                "数据源已存在，应使用 replace_input", source_key=key
            )
        snap.inputs[key] = vid
    elif kind == "replace_input":
        new_id, old_id = c["input_version_id"], c["replaces_version_id"]
        row = repo.require_kind(new_id, "raw_input")
        key = row["source_key"]
        if snap.inputs.get(key) != old_id:
            raise InvalidStateError(
                "被替换版本不是该数据源的当前版本",
                source_key=key,
                expect_current=snap.inputs.get(key),
                given=old_id,
            )
        snap.inputs[key] = new_id
    elif kind == "set_parameters":
        repo.require_kind(c["parameters_version_id"], "parameters")
        snap.parameters_version_id = c["parameters_version_id"]
    elif kind == "set_mapping":
        repo.require_kind(c["mapping_version_id"], "region_map")
        snap.mapping_version_id = c["mapping_version_id"]
    else:  # pragma: no cover - 受 CHECK 约束保护
        raise ValidationError("未知提交类型", change_type=kind)


def load_resolved_inputs(
    repo: Repository, snap: Snapshot
) -> list[dict[str, object]]:
    """读取快照中每个数据源的原始批次内容，供预测引擎使用。"""
    resolved: list[dict[str, object]] = []
    for key in sorted(snap.inputs):
        vid = snap.inputs[key]
        row = repo.get_version_row(vid)
        payload = repo.load_object(vid)
        resolved.append({
            "source_key": key,
            "source": payload["source"],
            "service_area": payload["service_area"],
            "version_id": vid,
            "records": payload["records"],
        })
    return resolved
