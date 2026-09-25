"""应用服务：不可变版本登记、情景派生、签署撤销、版本比较、谱系核验。"""
from __future__ import annotations

import sqlite3
from typing import Any

from ..clock import Clock, IdGenerator
from ..errors import (
    ConflictError,
    InvalidStateError,
    NotFoundError,
    ValidationError,
)
from ..forecast import ENGINE_VERSION
from ..hashing import hash_manifest
from ..models import (
    validate_decision,
    validate_parameters,
    validate_raw_batch,
    validate_region_map,
)
from ..repository import Repository
from .snapshots import Snapshot, resolve_commit

# 原始输入的数据源逻辑键 = 数据类别 + 服务区。
# 同一键在一个情景中同时只有一个生效版本，沿提交链可替换。


class VersionService:
    def __init__(
        self, repo: Repository, clock: Clock, idgen: IdGenerator,
        engine_version: str = ENGINE_VERSION,
    ) -> None:
        self.repo = repo
        self.clock = clock
        self.idgen = idgen
        self.engine_version = engine_version

    def _now(self) -> str:
        return self.clock.now().isoformat(timespec="microseconds")

    # ---- 输入/参数/映射导入 ------------------------------------------

    def import_raw_batch(
        self, doc: dict[str, Any], *, note: str | None = None,
        created_by: str | None = None,
    ) -> dict[str, Any]:
        normalized = validate_raw_batch(doc)
        source_key = f"{normalized['source']}:{normalized['service_area']}"
        with self.repo.db.transaction() as conn:
            vid, oid, created = self.repo.register_version(
                conn, "raw_input", normalized, source_key=source_key,
                note=note, created_by=created_by, now=self._now(),
            )
        return {
            "version_id": vid, "object_id": oid, "created_new": created,
            "kind": "raw_input", "source_key": source_key,
        }

    def import_parameters(self, doc: dict[str, Any], **kw: Any) -> dict[str, Any]:
        return self._import_simple("parameters", validate_parameters(doc), **kw)

    def import_region_map(self, doc: dict[str, Any], **kw: Any) -> dict[str, Any]:
        return self._import_simple("region_map", validate_region_map(doc), **kw)

    def _import_simple(
        self, kind: str, payload: dict[str, Any], *, note: str | None = None,
        created_by: str | None = None,
    ) -> dict[str, Any]:
        with self.repo.db.transaction() as conn:
            vid, oid, created = self.repo.register_version(
                conn, kind, payload, note=note, created_by=created_by,
                now=self._now(),
            )
        return {
            "version_id": vid, "object_id": oid, "created_new": created,
            "kind": kind,
        }

    # ---- 更正（绝不改写历史）-----------------------------------------

    def correct_raw_batch(
        self, original_version_id: str, corrected_doc: dict[str, Any],
        *, reason: str | None = None, created_by: str | None = None,
    ) -> dict[str, Any]:
        """登记一条更正：新版本 + corrections 记录 + 下游失效标记。

        原始对象与版本保持不变；受影响预测在 :mod:`forecasting` 中被标记。
        """
        from .forecasting import mark_correction_impact_for_original

        original = self.repo.require_kind(original_version_id, "raw_input")
        original_key = original["source_key"]
        normalized = validate_raw_batch(corrected_doc)
        new_key = f"{normalized['source']}:{normalized['service_area']}"
        if new_key != original_key:
            raise ValidationError(
                "更正批次与原始批次不属于同一数据源",
                original_source_key=original_key, corrected_source_key=new_key,
            )
        if self.repo.objects.get(original["object_id"]) == {
            **normalized, "_kind": "raw_batch"
        }:
            raise ValidationError("更正内容与原始版本完全相同，无需更正")

        with self.repo.db.transaction() as conn:
            existing_corr = conn.execute(
                "SELECT id,correction_version_id FROM corrections "
                "WHERE original_version_id=? AND correction_version_id IN "
                "(SELECT id FROM versions WHERE kind='raw_input' AND object_id=?)",
                (original_version_id,
                 self.repo.objects.put("raw_batch", normalized)),
            ).fetchone()
            if existing_corr is not None:
                corr_id = existing_corr["id"]
                new_vid = existing_corr["correction_version_id"]
                oid = self.repo.get_version_row(new_vid)["object_id"]
            else:
                new_vid, oid, created = self.repo.register_version(
                    conn, "raw_input", normalized, source_key=new_key,
                    corrects_version_id=original_version_id,
                    note=f"更正 {original_version_id}" + (f": {reason}" if reason else ""),
                    created_by=created_by, now=self._now(),
                )
                corr_id = self.idgen.new_id("cor")
                conn.execute(
                    "INSERT INTO corrections(id,correction_version_id,"
                    "original_version_id,reason,created_by,created_at) "
                    "VALUES(?,?,?,?,?,?)",
                    (corr_id, new_vid, original_version_id, reason,
                     created_by, self._now()),
                )
            affected = mark_correction_impact_for_original(
                conn, corr_id, original_version_id, self._now(), self.idgen
            )
        return {
            "correction_id": corr_id,
            "original_version_id": original_version_id,
            "corrected_version_id": new_vid,
            "object_id": oid,
            "affected_forecasts": affected,
        }

    # ---- 情景与提交链 -------------------------------------------------

    def create_scenario(
        self, *, parameters_version_id: str | None = None,
        mapping_version_id: str | None = None, message: str = "创建情景",
        created_by: str | None = None,
    ) -> dict[str, Any]:
        if parameters_version_id is not None:
            self.repo.require_kind(parameters_version_id, "parameters")
        if mapping_version_id is not None:
            self.repo.require_kind(mapping_version_id, "region_map")
        scenario_id = self.idgen.new_id("scn")
        commit_id = self.idgen.new_id("cmt")
        with self.repo.db.transaction() as conn:
            self.repo.insert_commit(conn, {
                "id": commit_id, "scenario_id": scenario_id, "seq": 0,
                "parent_commit_id": None, "change_type": "root",
                "input_version_id": None, "replaces_version_id": None,
                "parameters_version_id": parameters_version_id,
                "mapping_version_id": mapping_version_id,
                "message": message, "created_by": created_by,
                "created_at": self._now(),
            })
        return {"scenario_id": scenario_id, "root_commit_id": commit_id, "seq": 0}

    def _derive(
        self, scenario_id: str, change_type: str, fields: dict[str, Any],
        message: str, created_by: str | None,
        validate: Any = None, base_commit_id: str | None = None,
    ) -> dict[str, Any]:
        with self.repo.db.transaction() as conn:
            # IMMEDIATE 锁保证并发派生串行化提交；快照基于已稳定的已提交状态。
            head = conn.execute(
                "SELECT * FROM scenario_commits WHERE scenario_id=? "
                "ORDER BY seq DESC LIMIT 1",
                (scenario_id,),
            ).fetchone()
            if head is None:
                raise NotFoundError("情景不存在", scenario_id=scenario_id)
            # 乐观并发控制：并发派生同一父提交时只有一个成功。
            if base_commit_id is not None and base_commit_id != head["id"]:
                raise ConflictError(
                    "派生所基于的提交已不是情景 HEAD，请刷新后重试",
                    base_commit_id=base_commit_id, head_commit_id=head["id"],
                )
            if validate is not None:
                validate(conn, head)
            seq = head["seq"] + 1
            commit_id = self.idgen.new_id("cmt")
            row = {
                "id": commit_id, "scenario_id": scenario_id, "seq": seq,
                "parent_commit_id": head["id"], "change_type": change_type,
                "input_version_id": None, "replaces_version_id": None,
                "parameters_version_id": None, "mapping_version_id": None,
                "message": message, "created_by": created_by,
                "created_at": self._now(),
            }
            row.update(fields)
            try:
                self.repo.insert_commit(conn, row)
            except sqlite3.IntegrityError as exc:
                raise ConflictError(
                    "相同派生已存在（可能由并发请求提交）",
                    scenario_id=scenario_id, change_type=change_type,
                ) from exc
        return {"scenario_id": scenario_id, "commit_id": commit_id, "seq": seq}

    def add_input(
        self, scenario_id: str, input_version_id: str, *,
        message: str | None = None, created_by: str | None = None,
        base_commit_id: str | None = None,
    ) -> dict[str, Any]:
        row = self.repo.require_kind(input_version_id, "raw_input")
        key = row["source_key"]

        def validate(conn: sqlite3.Connection, head: sqlite3.Row) -> None:
            snap = resolve_commit(self.repo, scenario_id, head["id"])
            if key in snap.inputs:
                raise ConflictError(
                    "数据源已在情景中，请使用 replace_input 替换",
                    source_key=key,
                )

        return self._derive(
            scenario_id, "add_input", {"input_version_id": input_version_id},
            message or f"加入输入 {key}@{input_version_id}", created_by, validate,
            base_commit_id=base_commit_id,
        )

    def replace_input(
        self, scenario_id: str, old_version_id: str, new_version_id: str, *,
        message: str | None = None, created_by: str | None = None,
        base_commit_id: str | None = None,
    ) -> dict[str, Any]:
        old = self.repo.require_kind(old_version_id, "raw_input")
        new = self.repo.require_kind(new_version_id, "raw_input")
        if old["source_key"] != new["source_key"]:
            raise ValidationError(
                "替换版本必须属于同一数据源",
                old_source_key=old["source_key"], new_source_key=new["source_key"],
            )
        if old_version_id == new_version_id:
            raise ValidationError("新旧版本相同，无需替换")

        def validate(conn: sqlite3.Connection, head: sqlite3.Row) -> None:
            snap = resolve_commit(self.repo, scenario_id, head["id"])
            if snap.inputs.get(old["source_key"]) != old_version_id:
                raise ConflictError(
                    "被替换版本不是该数据源当前生效版本",
                    source_key=old["source_key"],
                    current=snap.inputs.get(old["source_key"]),
                )

        return self._derive(
            scenario_id, "replace_input",
            {"input_version_id": new_version_id,
             "replaces_version_id": old_version_id},
            message or f"替换输入 {old['source_key']}: "
                       f"{old_version_id} -> {new_version_id}",
            created_by, validate, base_commit_id=base_commit_id,
        )

    def set_parameters(
        self, scenario_id: str, parameters_version_id: str, *,
        message: str | None = None, created_by: str | None = None,
        base_commit_id: str | None = None,
    ) -> dict[str, Any]:
        self.repo.require_kind(parameters_version_id, "parameters")

        def validate(conn: sqlite3.Connection, head: sqlite3.Row) -> None:
            snap = resolve_commit(self.repo, scenario_id, head["id"])
            if snap.parameters_version_id == parameters_version_id:
                raise ConflictError("该参数版本已是当前生效版本，无变化")

        return self._derive(
            scenario_id, "set_parameters",
            {"parameters_version_id": parameters_version_id},
            message or f"设置参数 {parameters_version_id}", created_by, validate,
            base_commit_id=base_commit_id,
        )

    def set_mapping(
        self, scenario_id: str, mapping_version_id: str, *,
        message: str | None = None, created_by: str | None = None,
        base_commit_id: str | None = None,
    ) -> dict[str, Any]:
        self.repo.require_kind(mapping_version_id, "region_map")

        def validate(conn: sqlite3.Connection, head: sqlite3.Row) -> None:
            snap = resolve_commit(self.repo, scenario_id, head["id"])
            if snap.mapping_version_id == mapping_version_id:
                raise ConflictError("该映射版本已是当前生效版本，无变化")

        return self._derive(
            scenario_id, "set_mapping",
            {"mapping_version_id": mapping_version_id},
            message or f"设置区域映射 {mapping_version_id}", created_by, validate,
            base_commit_id=base_commit_id,
        )

    def scenario_detail(self, scenario_id: str) -> dict[str, Any]:
        commits = self.repo.list_commits(scenario_id)
        snap = resolve_commit(self.repo, scenario_id)
        return {
            "scenario_id": scenario_id,
            "head_commit_id": snap.commit_id,
            "seq": snap.seq,
            "inputs": [
                {"source_key": k, "version_id": v}
                for k, v in sorted(snap.inputs.items())
            ],
            "parameters_version_id": snap.parameters_version_id,
            "mapping_version_id": snap.mapping_version_id,
            "commits": [
                {
                    "id": c["id"], "seq": c["seq"],
                    "parent_commit_id": c["parent_commit_id"],
                    "change_type": c["change_type"],
                    "input_version_id": c["input_version_id"],
                    "replaces_version_id": c["replaces_version_id"],
                    "parameters_version_id": c["parameters_version_id"],
                    "mapping_version_id": c["mapping_version_id"],
                    "message": c["message"], "created_by": c["created_by"],
                    "created_at": c["created_at"],
                }
                for c in commits
            ],
        }

    def snapshot(self, scenario_id: str, commit_id: str | None = None) -> Snapshot:
        return resolve_commit(self.repo, scenario_id, commit_id)

    # ---- 指纹 ---------------------------------------------------------

    def forecast_fingerprint(self, snap: Snapshot) -> str:
        parts: list[tuple[str, str]] = [("engine", self.engine_version)]
        parts.append(("parameters",
                      self.repo.get_version_row(snap.parameters_version_id)["object_id"]))
        parts.append(("mapping",
                      self.repo.get_version_row(snap.mapping_version_id)["object_id"]))
        for key in sorted(snap.inputs):
            parts.append((key, self.repo.get_version_row(snap.inputs[key])["object_id"]))
        return "fp-" + hash_manifest(parts)[:32]

    # ---- 签署 / 撤销 --------------------------------------------------

    def adopt(
        self, forecast_version_id: str, decision_doc: dict[str, Any] | None = None,
        *, actor: str | None = None, reason: str | None = None,
    ) -> dict[str, Any]:
        self.repo.require_kind(forecast_version_id, "forecast")
        decision_vid = None
        payload: dict[str, Any] | None = None
        if decision_doc is not None:
            payload = validate_decision(decision_doc)
        with self.repo.db.transaction() as conn:
            status = _adoption_status(conn, forecast_version_id)
            if status == "adopted":
                raise InvalidStateError(
                    "预测已被采用且未撤销，不能重复采用",
                    forecast_version_id=forecast_version_id,
                )
            if payload is not None:
                decision_vid, _oid, _ = self.repo.register_version(
                    conn, "decision", payload, note=f"采用 {forecast_version_id}",
                    created_by=actor, now=self._now(),
                )
            event_id = self.idgen.new_id("evt")
            conn.execute(
                "INSERT INTO adoption_events(id,forecast_version_id,"
                "decision_version_id,action,actor,reason,created_at) "
                "VALUES(?,?,?,?,?,?,?)",
                (event_id, forecast_version_id, decision_vid, "adopted",
                 actor, reason, self._now()),
            )
        return {
            "event_id": event_id, "forecast_version_id": forecast_version_id,
            "action": "adopted", "decision_version_id": decision_vid,
        }

    def revoke(
        self, forecast_version_id: str, *, actor: str | None = None,
        reason: str | None = None,
    ) -> dict[str, Any]:
        self.repo.require_kind(forecast_version_id, "forecast")
        with self.repo.db.transaction() as conn:
            status = _adoption_status(conn, forecast_version_id)
            if status != "adopted":
                raise InvalidStateError(
                    "预测当前未被采用，无法撤销",
                    forecast_version_id=forecast_version_id, current=status,
                )
            event_id = self.idgen.new_id("evt")
            conn.execute(
                "INSERT INTO adoption_events(id,forecast_version_id,"
                "decision_version_id,action,actor,reason,created_at) "
                "VALUES(?,?,NULL,?,?,?,?)",
                (event_id, forecast_version_id, "revoked", actor, reason,
                 self._now()),
            )
        return {
            "event_id": event_id, "forecast_version_id": forecast_version_id,
            "action": "revoked",
        }

    def adoption_status(self, forecast_version_id: str) -> dict[str, Any]:
        with self.repo.db.session() as conn:
            events = list(conn.execute(
                "SELECT * FROM adoption_events WHERE forecast_version_id=? "
                "ORDER BY created_at, rowid",
                (forecast_version_id,),
            ))
        latest = events[-1] if events else None
        return {
            "forecast_version_id": forecast_version_id,
            "status": latest["action"] if latest else "never_adopted",
            "decision_version_id": latest["decision_version_id"] if latest else None,
            "events": [dict(e) for e in events],
        }

    # ---- 比较 ---------------------------------------------------------

    def compare_forecasts(
        self, left_version_id: str, right_version_id: str
    ) -> dict[str, Any]:
        left = self.repo.get_forecast(left_version_id)
        right = self.repo.get_forecast(right_version_id)
        lp, rp = left["payload"], right["payload"]
        result: dict[str, Any] = {
            "left": left_version_id, "right": right_version_id,
            "inputs_changed": _diff_inputs(left["inputs"], right["inputs"]),
            "parameters_changed": lp["parameters_version"] != rp["parameters_version"],
            "mapping_changed": lp["mapping_version"] != rp["mapping_version"],
            "service_areas": _compare_scopes(lp.get("service_areas", {}),
                                             rp.get("service_areas", {})),
            "deploy_regions": _compare_scopes(lp.get("deploy_regions", {}),
                                              rp.get("deploy_regions", {})),
        }
        result["left_status"] = self.adoption_status(left_version_id)["status"]
        result["right_status"] = self.adoption_status(right_version_id)["status"]
        return result

    # ---- 谱系核验 -----------------------------------------------------

    def verify_lineage(self, forecast_version_id: str) -> dict[str, Any]:
        """对单个预测做端到端核验：指针完整、哈希未改、可被原始版本精确复算。"""
        from ..forecast import Forecaster
        from ..hashing import canonical_dumps

        self.repo.require_kind(forecast_version_id, "forecast")
        with self.repo.db.session() as conn:
            frow = conn.execute(
                "SELECT * FROM forecasts WHERE version_id=?",
                (forecast_version_id,),
            ).fetchone()
            input_rows = list(conn.execute(
                "SELECT source_key,input_version_id FROM forecast_inputs "
                "WHERE forecast_version_id=? ORDER BY source_key",
                (forecast_version_id,),
            ))
            open_mark_count = conn.execute(
                "SELECT COUNT(*) AS n FROM invalidation_marks "
                "WHERE forecast_version_id=? AND status='affected'",
                (forecast_version_id,),
            ).fetchone()["n"]
        if frow is None:
            raise NotFoundError("预测元数据缺失", version_id=forecast_version_id)

        checks: list[dict[str, Any]] = []

        def check(name: str, ok: bool, detail: str = "") -> None:
            checks.append({"name": name, "ok": bool(ok), "detail": detail})

        object_id = self.repo.get_version_row(forecast_version_id)["object_id"]

        # 0. 预测对象自身可读且哈希未改（被篡改时给出失败报告而非抛错）。
        try:
            payload = self.repo.load_object(forecast_version_id)
            check("预测对象存在且哈希一致", True)
        except Exception as exc:  # noqa: BLE001 - 任何读取/哈希失败都是核验失败
            check("预测对象存在且哈希一致", False, f"{type(exc).__name__}: {exc}")
            return {
                "forecast_version_id": forecast_version_id,
                "object_id": object_id, "fingerprint": frow["fingerprint"],
                "engine_version": self.engine_version, "ok": False,
                "checks": checks,
            }

        # 1. 所有输入/参数/映射指针存在且对象哈希校验通过（objects.get 内校验）。
        resolved: list[dict[str, Any]] = []
        inputs_ok = True
        for ref in input_rows:
            try:
                body = self.repo.load_object(ref["input_version_id"])
                resolved.append({
                    "source_key": ref["source_key"], "source": body["source"],
                    "service_area": body["service_area"],
                    "version_id": ref["input_version_id"], "records": body["records"],
                })
            except Exception as exc:  # noqa: BLE001
                inputs_ok = False
                check(f"输入 {ref['source_key']} 可读", False, str(exc))
        check("全部输入版本存在且哈希一致", inputs_ok)

        params_ok = True
        try:
            params = self.repo.load_object(payload["parameters_version"])
        except Exception as exc:  # noqa: BLE001
            params_ok = False
            check("参数版本可读", False, str(exc))
        check("参数版本存在且哈希一致", params_ok)

        mapping_ok = True
        try:
            mapping = self.repo.load_object(payload["mapping_version"])
        except Exception as exc:  # noqa: BLE001
            mapping_ok = False
            check("映射版本可读", False, str(exc))
        check("区域映射版本存在且哈希一致", mapping_ok)

        # 2. 指纹一致。
        snap = Snapshot(
            scenario_id="(verify)", commit_id="(verify)", seq=-1,
            inputs={ref["source_key"]: ref["input_version_id"] for ref in input_rows},
            parameters_version_id=payload["parameters_version"],
            mapping_version_id=payload["mapping_version"],
        )
        fp_ok = False
        try:
            fp_ok = self.forecast_fingerprint(snap) == frow["fingerprint"]
        except NotFoundError as exc:
            check("内容指纹与登记一致", False, str(exc.message))
        else:
            check("内容指纹与登记一致", fp_ok, f"登记 {frow['fingerprint']}")

        # 3. 用登记的原始数据重新计算，字节级比对。
        recompute_ok = False
        detail = ""
        if inputs_ok and params_ok and mapping_ok:
            engine = Forecaster(params, mapping)
            recomputed = engine.run(
                resolved, payload["parameters_version"], payload["mapping_version"]
            )
            stored = {k: v for k, v in payload.items() if not k.startswith("_")}
            recompute_ok = canonical_dumps(recomputed) == canonical_dumps(stored)
            if not recompute_ok:
                detail = "复算结果与存储结果不一致"
        check("使用登记输入可精确复算预测", recompute_ok, detail)

        # 4. 失效状态。
        check("不存在未处理的失效标记", open_mark_count == 0,
              f"未处理标记 {open_mark_count} 条")

        ok = all(c["ok"] for c in checks)
        return {
            "forecast_version_id": forecast_version_id,
            "object_id": object_id,
            "fingerprint": frow["fingerprint"],
            "engine_version": self.engine_version,
            "ok": ok,
            "checks": checks,
        }


def _adoption_status(conn: sqlite3.Connection, forecast_version_id: str) -> str | None:
    row = conn.execute(
        "SELECT action FROM adoption_events WHERE forecast_version_id=? "
        "ORDER BY created_at DESC, rowid DESC LIMIT 1",
        (forecast_version_id,),
    ).fetchone()
    return row["action"] if row else None


def _diff_inputs(left: list[dict[str, Any]], right: list[dict[str, Any]]) -> dict[str, Any]:
    lm = {r["source_key"]: r["input_version_id"] for r in left}
    rm = {r["source_key"]: r["input_version_id"] for r in right}
    changed = {k: {"left": lm[k], "right": rm[k]}
               for k in sorted(set(lm) & set(rm)) if lm[k] != rm[k]}
    return {
        "only_left": {k: lm[k] for k in sorted(set(lm) - set(rm))},
        "only_right": {k: rm[k] for k in sorted(set(rm) - set(lm))},
        "changed": changed,
    }


def _compare_scopes(left: dict[str, Any], right: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for key in sorted(set(left) | set(right)):
        if key not in left or key not in right:
            out[key] = {"present": "left" if key in left else "right"}
            continue
        lp_, rp_ = left[key]["peak"], right[key]["peak"]
        out[key] = {
            "present": "both",
            "left_peak": lp_, "right_peak": rp_,
            "delta_point": round(rp_["point"] - lp_["point"], 6),
            "delta_ci_low": round(rp_["ci_low"] - lp_["ci_low"], 6),
            "delta_ci_high": round(rp_["ci_high"] - lp_["ci_high"], 6),
            "timestamp_changed": lp_["timestamp"] != rp_["timestamp"],
        }
    return out
