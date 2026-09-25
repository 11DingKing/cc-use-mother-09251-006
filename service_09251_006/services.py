"""应用服务：导入、派生、计算、签署、撤销、更正影响、比较与谱系核验。

所有用例入口都收敛在 ForecastService；接口边界（HTTP API、CLI）只做
参数搬运与序列化，不含业务规则。
"""
from __future__ import annotations

import json
import sqlite3

from .domain import (
    ConflictError,
    JobStatus,
    NotFoundError,
    NotSignedError,
    StaleVersionError,
    ValidationError,
    VersionStatus,
)
from .forecast import ForecastParams, canonical_json, results_hash, sha256_text
from .jobs import ComputeRunner
from .ports import Clock, IdGenerator, UuidIds, parse_iso, to_iso
from .storage import Repository


def _as_count(value, field: str) -> int:
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise ValidationError(f"{field} 必须是数字") from exc
    if number < 0 or not number.is_integer():
        raise ValidationError(f"{field} 必须是非负整数")
    return int(number)


def _normalize_record(raw) -> dict:
    if not isinstance(raw, dict):
        raise ValidationError("每条记录必须是 JSON 对象")
    station = str(raw.get("station_id") or "").strip()
    if not station:
        raise ValidationError("记录缺少 station_id")
    observed_raw = raw.get("observed_at")
    if not observed_raw:
        raise ValidationError(f"站点 {station} 的记录缺少 observed_at")
    try:
        moment = parse_iso(str(observed_raw))
    except ValueError as exc:
        raise ValidationError(f"非法时间格式: {observed_raw}") from exc
    try:
        energy = float(raw.get("energy_kwh", 0.0))
    except (TypeError, ValueError) as exc:
        raise ValidationError(f"站点 {station} 的 energy_kwh 必须是数字") from exc
    if energy < 0:
        raise ValidationError(f"站点 {station} 的 energy_kwh 不能为负")
    observed = to_iso(moment)
    return {
        "station_id": station,
        "observed_at": observed,
        "day": observed[:10],
        "hour": moment.hour,
        "vehicles": _as_count(raw.get("vehicles", 0), "vehicles"),
        "sessions": _as_count(raw.get("sessions", 0), "sessions"),
        "energy_kwh": energy,
    }


def _canonical_records(records) -> list[dict]:
    """规范化并按 (站点, 时间) 排序去重，得到批次的规范记录集。"""
    canon: dict[tuple[str, str], dict] = {}
    for raw in records:
        rec = _normalize_record(raw)
        canon[(rec["station_id"], rec["observed_at"])] = rec
    return [canon[key] for key in sorted(canon)]


def _hashable_record(rec: dict) -> dict:
    """哈希规范形式：只含输入字段，派生字段（day/hour）不参与。"""
    return {
        "station_id": rec["station_id"],
        "observed_at": rec["observed_at"],
        "vehicles": rec["vehicles"],
        "sessions": rec["sessions"],
        "energy_kwh": rec["energy_kwh"],
    }


def batch_content_hash(source: str, records) -> str:
    canon = [_hashable_record(r) for r in records]
    return sha256_text(canonical_json({"source": source, "records": canon}))


class ForecastService:
    """充电需求预测版本管理的应用服务门面。"""

    def __init__(self, repo: Repository, *, clock=None, ids: IdGenerator | None = None):
        self.repo = repo
        self.clock = clock or Clock()
        self.ids = ids or UuidIds()
        self.runner = ComputeRunner(repo, self.clock)

    # ------------------------------------------------------------------
    # 导入与更正
    # ------------------------------------------------------------------

    def import_batch(self, *, source, records, note="", corrects=None,
                     correction_reason="", correction_stations=None,
                     correction_range=None) -> dict:
        """导入原始数据批次；按内容哈希幂等，重复导入返回既有批次。

        corrects 指向被更正的历史批次时，同时登记更正并标记受影响的下游版本。
        """
        if not source or not str(source).strip():
            raise ValidationError("缺少 source")
        if not isinstance(records, list) or not records:
            raise ValidationError("records 必须是非空数组")
        source = str(source).strip()
        canon = _canonical_records(records)
        content_hash = batch_content_hash(source, canon)
        existing = self.repo.batch_by_hash(content_hash)
        if existing:
            return {"batch": self._batch_view(existing), "created": False, "correction": None}
        if corrects is not None and self.repo.get_batch(corrects) is None:
            raise NotFoundError(f"被更正的批次不存在: {corrects}")

        batch_id = self.ids.new_id("bat")
        now = self.clock.now_iso()
        correction_view = None
        with self.repo.tx():
            self.repo.insert_batch(
                batch_id=batch_id, content_hash=content_hash, source=source,
                imported_at=now, record_count=len(canon),
                corrects_batch_id=corrects, note=str(note or ""))
            self.repo.insert_records(batch_id, canon)
            if corrects is not None:
                correction_view = self._register_correction_locked(
                    target_batch_id=corrects, correction_batch_id=batch_id,
                    stations=correction_stations, time_range=correction_range,
                    reason=correction_reason or "原始数据更正")
        return {
            "batch": self._batch_view(self.repo.get_batch(batch_id)),
            "created": True,
            "correction": correction_view,
        }

    def register_correction(self, *, target_batch_id, correction_batch_id=None,
                            stations=None, time_range=None, reason="") -> dict:
        """为已导入的批次登记更正（更正数据此前已单独导入的情况）。"""
        if self.repo.get_batch(target_batch_id) is None:
            raise NotFoundError(f"被更正的批次不存在: {target_batch_id}")
        if correction_batch_id is not None and self.repo.get_batch(correction_batch_id) is None:
            raise NotFoundError(f"更正批次不存在: {correction_batch_id}")
        with self.repo.tx():
            return self._register_correction_locked(
                target_batch_id=target_batch_id,
                correction_batch_id=correction_batch_id,
                stations=stations, time_range=time_range,
                reason=reason or "原始数据更正")

    def _register_correction_locked(self, *, target_batch_id, correction_batch_id,
                                    stations, time_range, reason) -> dict:
        range_start = range_end = None
        if time_range:
            range_start, range_end = self._validate_range(time_range)
        station_ids = None
        if stations:
            station_ids = sorted({str(s).strip() for s in stations if str(s).strip()})
            if not station_ids:
                raise ValidationError("correction_stations 不能为空数组")
        correction_id = self.ids.new_id("cor")
        self.repo.insert_correction(
            correction_id=correction_id, target_batch_id=target_batch_id,
            correction_batch_id=correction_batch_id,
            station_ids=json.dumps(station_ids) if station_ids else None,
            range_start=range_start, range_end=range_end,
            reason=str(reason), registered_at=self.clock.now_iso())
        impacted = self._mark_impacted_locked(correction_id)
        return {
            "correction_id": correction_id,
            "target_batch_id": target_batch_id,
            "correction_batch_id": correction_batch_id,
            "station_ids": station_ids,
            "range_start": range_start,
            "range_end": range_end,
            "reason": str(reason),
            "impacted_versions": impacted,
        }

    @staticmethod
    def _validate_range(time_range) -> tuple:
        if not isinstance(time_range, (list, tuple)) or len(time_range) != 2:
            raise ValidationError("correction_range 必须是 [start, end]")
        start, end = time_range
        start_iso = to_iso(parse_iso(str(start))) if start else None
        end_iso = to_iso(parse_iso(str(end))) if end else None
        if start_iso and end_iso and start_iso > end_iso:
            raise ValidationError("correction_range 起点不能晚于终点")
        return start_iso, end_iso

    def _correction_stations(self, correction: dict) -> set[str]:
        """计算更正实际波及的站点集合（站点过滤与时间范围取交）。"""
        target = correction["target_batch_id"]
        if correction["station_ids"]:
            stations = set(json.loads(correction["station_ids"]))
        else:
            stations = set(self.repo.batch_stations(target))
        if correction["range_start"] or correction["range_end"]:
            stations &= self.repo.batch_stations_in_range(
                target, correction["range_start"], correction["range_end"])
        return stations

    def _mark_impacted_locked(self, correction_id: str) -> list[str]:
        """标记受更正影响的下游版本；只追加 stale 标记，历史内容不变。"""
        correction = self.repo.get_correction(correction_id)
        target = correction["target_batch_id"]
        affected = self._correction_stations(correction)
        impacted = []
        for version in self.repo.list_versions():
            if version["stale"]:
                continue
            if target not in json.loads(version["batch_ids"]):
                continue
            mapping = self.repo.get_mapping(version["mapping_id"])
            used = set(json.loads(mapping["entries"])) & self.repo.batch_stations(target)
            hit = affected & used
            if hit:
                reason = (f"批次 {target} 被更正（correction {correction_id}），"
                          f"涉及站点 {', '.join(sorted(hit))}")
                self.repo.mark_stale(version["version_id"], reason)
                impacted.append(version["version_id"])
        return impacted

    def _staleness_reasons(self, batch_ids, mapping_id: str) -> list[str]:
        """版本创建时检查：输入批次是否已被既有更正波及。"""
        mapping = self.repo.get_mapping(mapping_id)
        mapped = set(json.loads(mapping["entries"]))
        reasons = []
        for correction in self.repo.list_corrections():
            target = correction["target_batch_id"]
            if target not in batch_ids:
                continue
            used = mapped & self.repo.batch_stations(target)
            hit = self._correction_stations(correction) & used
            if hit:
                reasons.append(f"批次 {target} 存在更正（correction "
                               f"{correction['correction_id']}），涉及站点 {', '.join(sorted(hit))}")
        return reasons

    # ------------------------------------------------------------------
    # 参数与区域映射
    # ------------------------------------------------------------------

    def create_parameter_set(self, payload) -> dict:
        params = ForecastParams.from_payload(payload)
        content = canonical_json(params.canonical())
        digest = sha256_text(content)
        existing = self.repo.params_by_hash(digest)
        if existing:
            return {"parameter_set": self._params_view(existing), "created": False}
        param_id = self.ids.new_id("par")
        self.repo.insert_params(param_id=param_id, payload=content,
                                content_hash=digest, created_at=self.clock.now_iso())
        return {"parameter_set": self._params_view(self.repo.get_params(param_id)),
                "created": True}

    def create_region_mapping(self, entries) -> dict:
        if not isinstance(entries, dict) or not entries:
            raise ValidationError("区域映射必须是非空对象 {站点: 服务区}")
        clean = {}
        for station, area in entries.items():
            station, area = str(station).strip(), str(area).strip()
            if not station or not area:
                raise ValidationError("站点与服务区名称不能为空")
            clean[station] = area
        content = canonical_json(clean)
        digest = sha256_text(content)
        existing = self.repo.mapping_by_hash(digest)
        if existing:
            return {"region_mapping": self._mapping_view(existing), "created": False}
        mapping_id = self.ids.new_id("map")
        self.repo.insert_mapping(mapping_id=mapping_id, entries=content,
                                 content_hash=digest, created_at=self.clock.now_iso())
        return {"region_mapping": self._mapping_view(self.repo.get_mapping(mapping_id)),
                "created": True}

    # ------------------------------------------------------------------
    # 版本与派生
    # ------------------------------------------------------------------

    def create_version(self, *, batch_ids, param_id, mapping_id, scenario,
                       parent_version_id=None) -> dict:
        batch_ids = [str(b) for b in (batch_ids or [])]
        if not batch_ids:
            raise ValidationError("版本至少需要一个数据批次")
        for batch_id in batch_ids:
            if self.repo.get_batch(batch_id) is None:
                raise NotFoundError(f"批次不存在: {batch_id}")
        if self.repo.get_params(param_id) is None:
            raise NotFoundError(f"参数集不存在: {param_id}")
        if self.repo.get_mapping(mapping_id) is None:
            raise NotFoundError(f"区域映射不存在: {mapping_id}")
        scenario = str(scenario or "").strip()
        if not scenario:
            raise ValidationError("scenario 不能为空")
        parent_key = ""
        if parent_version_id is not None:
            if self.repo.get_version(parent_version_id) is None:
                raise NotFoundError(f"父版本不存在: {parent_version_id}")
            parent_key = parent_version_id

        with self.repo.tx():
            seq = self.repo.next_seq(parent_key)
            reasons = self._staleness_reasons(batch_ids, mapping_id)
            version_id = self.ids.new_id("ver")
            self.repo.insert_version(
                version_id=version_id, parent_version_id=parent_version_id,
                parent_key=parent_key, seq=seq, scenario=scenario,
                param_id=param_id, mapping_id=mapping_id,
                batch_ids=json.dumps(batch_ids), status=VersionStatus.DRAFT.value,
                stale=1 if reasons else 0, stale_reason="；".join(reasons) or None,
                created_at=self.clock.now_iso())
        return self.get_version(version_id)

    def derive_version(self, parent_version_id: str, *, scenario,
                       param_id=None, mapping_id=None, batch_ids=None) -> dict:
        """从任一版本派生情景：未指定的输入沿父版本继承。"""
        parent = self.repo.get_version(parent_version_id)
        if parent is None:
            raise NotFoundError(f"父版本不存在: {parent_version_id}")
        return self.create_version(
            batch_ids=batch_ids if batch_ids is not None else json.loads(parent["batch_ids"]),
            param_id=param_id or parent["param_id"],
            mapping_id=mapping_id or parent["mapping_id"],
            scenario=scenario, parent_version_id=parent_version_id)

    # ------------------------------------------------------------------
    # 计算（长任务，可重入）
    # ------------------------------------------------------------------

    def start_compute(self, version_id: str) -> dict:
        """发起计算任务；重复发起返回既有活动任务，保证幂等。"""
        version = self._require_version(version_id)
        if version["status"] != VersionStatus.DRAFT.value:
            raise ConflictError(
                f"版本状态为 {version['status']}，结果不可变；如需重算请派生新版本",
                details={"version_id": version_id})
        active = self.repo.active_job_for_version(version_id)
        if active:
            return {"job": ComputeRunner._view(active), "created": False}
        plan = self._compute_plan(version)
        job_id = self.ids.new_id("job")
        now = self.clock.now_iso()
        try:
            with self.repo.tx():
                self.repo.insert_job(job_id=job_id, kind="compute",
                                     version_id=version_id,
                                     status=JobStatus.PENDING.value,
                                     total=len(plan), created_at=now, updated_at=now)
        except sqlite3.IntegrityError:
            # 并发发起：唯一索引兜底，返回先创建的任务。
            return {"job": ComputeRunner._view(self.repo.active_job_for_version(version_id)),
                    "created": False}
        return {"job": ComputeRunner._view(self.repo.get_job(job_id)), "created": True}

    def run_compute(self, job_id: str, should_stop=None) -> dict:
        """执行或续跑计算任务；中断后再次调用即可从断点继续。"""
        return self.runner.run(job_id, should_stop=should_stop)

    def get_job(self, job_id: str) -> dict:
        job = self.repo.get_job(job_id)
        if job is None:
            raise NotFoundError(f"任务不存在: {job_id}")
        return ComputeRunner._view(job)

    def _compute_plan(self, version: dict) -> list[str]:
        mapping = json.loads(self.repo.get_mapping(version["mapping_id"])["entries"])
        records = self.repo.records_for_batches(json.loads(version["batch_ids"]))
        return sorted({mapping[r["station_id"]] for r in records
                       if r["station_id"] in mapping})

    # ------------------------------------------------------------------
    # 签署、撤销与调度决策
    # ------------------------------------------------------------------

    def sign_version(self, version_id: str, *, signed_by) -> dict:
        signed_by = str(signed_by or "").strip()
        if not signed_by:
            raise ValidationError("签署人不能为空")
        with self.repo.tx():
            version = self._require_version(version_id)
            if version["status"] != VersionStatus.COMPUTED.value:
                raise ConflictError(
                    f"仅 COMPUTED 版本可签署，当前状态 {version['status']}",
                    details={"version_id": version_id})
            if not self.repo.set_signed(version_id, signed_by, self.clock.now_iso()):
                raise ConflictError("版本状态已变化，签署失败",
                                    details={"version_id": version_id})
        return self.get_version(version_id)

    def revoke_version(self, version_id: str, *, revoked_by, reason) -> dict:
        revoked_by = str(revoked_by or "").strip()
        reason = str(reason or "").strip()
        if not revoked_by or not reason:
            raise ValidationError("撤销必须填写操作人与原因")
        with self.repo.tx():
            version = self._require_version(version_id)
            if version["status"] not in (VersionStatus.COMPUTED.value,
                                         VersionStatus.SIGNED.value):
                raise ConflictError(
                    f"仅 COMPUTED/SIGNED 版本可撤销，当前状态 {version['status']}",
                    details={"version_id": version_id})
            if not self.repo.set_revoked(version_id, revoked_by,
                                         self.clock.now_iso(), reason):
                raise ConflictError("版本状态已变化，撤销失败",
                                    details={"version_id": version_id})
        return self.get_version(version_id)

    def add_decision(self, version_id: str, *, service_area, action, units,
                     decided_by, note="") -> dict:
        """把被采用的预测与调度决策关联；仅已签署且未受更正影响的版本可被采用。"""
        version = self._require_version(version_id)
        if version["status"] != VersionStatus.SIGNED.value:
            raise NotSignedError(
                f"仅已签署版本可被调度决策采用，当前状态 {version['status']}",
                details={"version_id": version_id})
        if version["stale"]:
            raise StaleVersionError(
                "版本受原始数据更正影响，禁止采用；请基于更正后数据派生新版本",
                details={"version_id": version_id, "stale_reason": version["stale_reason"]})
        service_area = str(service_area or "").strip()
        if service_area not in self.repo.result_areas(version_id):
            raise ValidationError(f"服务区 {service_area or '(空)'} 不在版本结果中")
        action = str(action or "").strip()
        if not action:
            raise ValidationError("action 不能为空")
        units = _as_count(units, "units")
        if units <= 0:
            raise ValidationError("units 必须是正整数")
        decided_by = str(decided_by or "").strip()
        if not decided_by:
            raise ValidationError("decided_by 不能为空")
        decision_id = self.ids.new_id("dec")
        self.repo.insert_decision(
            decision_id=decision_id, version_id=version_id, service_area=service_area,
            action=action, units=units, decided_by=decided_by,
            decided_at=self.clock.now_iso(), note=str(note or ""))
        return self._decision_view(self.repo.get_decision(decision_id))

    def list_decisions(self, version_id=None) -> list[dict]:
        return [self._decision_view(row) for row in self.repo.list_decisions(version_id)]

    # ------------------------------------------------------------------
    # 比较与谱系核验
    # ------------------------------------------------------------------

    def compare_versions(self, a_id: str, b_id: str) -> dict:
        """比较两个版本在各服务区的峰值与置信区间。"""
        va = self._require_version(a_id)
        vb = self._require_version(b_id)
        rows_a = self.repo.get_results(a_id)
        rows_b = self.repo.get_results(b_id)
        if not rows_a or not rows_b:
            raise ConflictError("两个版本都必须已完成计算才能比较",
                                details={"a": a_id, "b": b_id})
        peaks_a = {r["service_area"]: r for r in rows_a if r["is_peak"]}
        peaks_b = {r["service_area"]: r for r in rows_b if r["is_peak"]}
        areas = []
        for area in sorted(set(peaks_a) | set(peaks_b)):
            pa, pb = peaks_a.get(area), peaks_b.get(area)
            entry = {"service_area": area,
                     "a": self._peak_view(pa), "b": self._peak_view(pb)}
            if pa and pb:
                entry["delta_mean_kwh"] = round(pb["mean_kwh"] - pa["mean_kwh"], 6)
                entry["delta_ci_width_kwh"] = round(
                    (pb["ci_high"] - pb["ci_low"]) - (pa["ci_high"] - pa["ci_low"]), 6)
            areas.append(entry)
        return {"a": self._version_brief(va), "b": self._version_brief(vb),
                "areas": areas}

    def verify_lineage(self, version_id: str) -> dict:
        """谱系核验：沿父链向上，逐项校验输入存在性、内容哈希与 stale 传播。"""
        version = self._require_version(version_id)
        chain = []
        current = version
        while current is not None:
            chain.append(current)
            parent_id = current["parent_version_id"]
            current = self.repo.get_version(parent_id) if parent_id else None

        checks: list[dict] = []
        ok = True

        def check(name, passed, detail=""):
            nonlocal ok
            checks.append({"check": name, "ok": bool(passed), "detail": detail})
            ok = ok and passed

        for item in chain:
            vid = item["version_id"]
            params = self.repo.get_params(item["param_id"])
            mapping = self.repo.get_mapping(item["mapping_id"])
            batch_ids = json.loads(item["batch_ids"])
            batches = {bid: self.repo.get_batch(bid) for bid in batch_ids}
            missing = [bid for bid, row in batches.items() if row is None]
            check(f"{vid}:inputs_exist",
                  params is not None and mapping is not None and not missing,
                  f"缺失: {missing}" if missing else "")
            if params is not None:
                check(f"{vid}:params_integrity",
                      sha256_text(params["payload"]) == params["content_hash"])
            if mapping is not None:
                check(f"{vid}:mapping_integrity",
                      sha256_text(mapping["entries"]) == mapping["content_hash"])
            for bid, batch in batches.items():
                if batch is None:
                    continue
                records = self.repo.records_for_batches([bid])
                canon = [{
                    "station_id": r["station_id"], "observed_at": r["observed_at"],
                    "vehicles": r["vehicles"], "sessions": r["sessions"],
                    "energy_kwh": r["energy_kwh"],
                } for r in records]
                digest = batch_content_hash(batch["source"], canon)
                check(f"{vid}:batch_integrity:{bid}",
                      digest == batch["content_hash"] and len(canon) == batch["record_count"])
            if item["result_hash"]:
                digest = self._results_digest(item)
                check(f"{vid}:results_integrity", digest == item["result_hash"])
            expected = self._staleness_reasons(batch_ids, item["mapping_id"])
            check(f"{vid}:staleness_marked", not expected or item["stale"],
                  "存在未标记的更正影响" if expected and not item["stale"] else "")

        return {"version_id": version_id, "ok": ok,
                "chain": [self._version_brief(v) for v in chain], "checks": checks}

    def _results_digest(self, version: dict) -> str:
        rows = [{
            "service_area": r["service_area"], "hour": r["hour"], "days": r["days"],
            "mean_kwh": r["mean_kwh"], "std_kwh": r["std_kwh"],
            "ci_low": r["ci_low"], "ci_high": r["ci_high"],
            "is_peak": bool(r["is_peak"]),
        } for r in self.repo.get_results(version["version_id"])]
        return results_hash(rows)

    # ------------------------------------------------------------------
    # 查询
    # ------------------------------------------------------------------

    def get_version(self, version_id: str) -> dict:
        return self._version_view(self._require_version(version_id))

    def list_versions(self) -> list[dict]:
        return [self._version_view(row) for row in self.repo.list_versions()]

    def get_results(self, version_id: str) -> dict:
        self._require_version(version_id)
        rows = []
        for row in self.repo.get_results(version_id):
            rows.append({
                "service_area": row["service_area"], "hour": row["hour"],
                "days": row["days"], "mean_kwh": row["mean_kwh"],
                "std_kwh": row["std_kwh"], "ci_low": row["ci_low"],
                "ci_high": row["ci_high"], "is_peak": bool(row["is_peak"]),
            })
        return {"version_id": version_id, "rows": rows}

    def get_batch(self, batch_id: str) -> dict:
        batch = self.repo.get_batch(batch_id)
        if batch is None:
            raise NotFoundError(f"批次不存在: {batch_id}")
        return self._batch_view(batch)

    def list_batches(self) -> list[dict]:
        return [self._batch_view(row) for row in self.repo.list_batches()]

    # ------------------------------------------------------------------
    # 视图与工具
    # ------------------------------------------------------------------

    def _require_version(self, version_id: str) -> dict:
        version = self.repo.get_version(version_id)
        if version is None:
            raise NotFoundError(f"版本不存在: {version_id}")
        return version

    @staticmethod
    def _batch_view(row: dict) -> dict:
        return {
            "batch_id": row["batch_id"], "source": row["source"],
            "content_hash": row["content_hash"], "record_count": row["record_count"],
            "imported_at": row["imported_at"],
            "corrects_batch_id": row["corrects_batch_id"], "note": row["note"],
        }

    @staticmethod
    def _params_view(row: dict) -> dict:
        return {"param_id": row["param_id"], "payload": json.loads(row["payload"]),
                "content_hash": row["content_hash"], "created_at": row["created_at"]}

    @staticmethod
    def _mapping_view(row: dict) -> dict:
        return {"mapping_id": row["mapping_id"], "entries": json.loads(row["entries"]),
                "content_hash": row["content_hash"], "created_at": row["created_at"]}

    @staticmethod
    def _version_view(row: dict) -> dict:
        return {
            "version_id": row["version_id"],
            "parent_version_id": row["parent_version_id"],
            "seq": row["seq"], "scenario": row["scenario"],
            "param_id": row["param_id"], "mapping_id": row["mapping_id"],
            "batch_ids": json.loads(row["batch_ids"]),
            "status": row["status"], "stale": bool(row["stale"]),
            "stale_reason": row["stale_reason"], "result_hash": row["result_hash"],
            "created_at": row["created_at"],
            "signed_by": row["signed_by"], "signed_at": row["signed_at"],
            "revoked_by": row["revoked_by"], "revoked_at": row["revoked_at"],
            "revoke_reason": row["revoke_reason"],
        }

    @classmethod
    def _version_brief(cls, row: dict) -> dict:
        view = cls._version_view(row)
        return {key: view[key] for key in
                ("version_id", "parent_version_id", "seq", "scenario", "status",
                 "stale", "param_id", "mapping_id", "result_hash")}

    @staticmethod
    def _peak_view(row) -> dict | None:
        if row is None:
            return None
        return {"peak_hour": row["hour"], "mean_kwh": row["mean_kwh"],
                "ci_low": row["ci_low"], "ci_high": row["ci_high"],
                "days": row["days"]}

    @staticmethod
    def _decision_view(row: dict) -> dict:
        return {
            "decision_id": row["decision_id"], "version_id": row["version_id"],
            "service_area": row["service_area"], "action": row["action"],
            "units": row["units"], "decided_by": row["decided_by"],
            "decided_at": row["decided_at"], "note": row["note"],
        }
