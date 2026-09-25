"""HTTP API 边界：基于标准库的 REST 接口。

只做参数搬运与序列化；业务规则全部在 ForecastService。
错误统一为 {"error": {"code", "message", "details"}}。
"""
from __future__ import annotations

import json
import re
import threading
import traceback
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from .domain import DomainError, JobInterrupted

ERROR_STATUS = {
    "validation": 400,
    "not_found": 404,
    "conflict": 409,
    "version_not_signed": 409,
    "version_stale": 409,
}


# ---------------------------------------------------------------------------
# 路由处理：每个函数接收 (service, path_params, query, body)，返回 (status, payload)
# ---------------------------------------------------------------------------

def _create_batch(service, path, query, body):
    return 200, service.import_batch(
        source=body.get("source"), records=body.get("records"),
        note=body.get("note", ""), corrects=body.get("corrects"),
        correction_reason=body.get("correction_reason", ""),
        correction_stations=body.get("correction_stations"),
        correction_range=body.get("correction_range"))


def _list_batches(service, path, query, body):
    return 200, {"batches": service.list_batches()}


def _get_batch(service, path, query, body):
    return 200, {"batch": service.get_batch(path["id"])}


def _create_correction(service, path, query, body):
    return 200, {"correction": service.register_correction(
        target_batch_id=body.get("target_batch_id"),
        correction_batch_id=body.get("correction_batch_id"),
        stations=body.get("stations"), time_range=body.get("time_range"),
        reason=body.get("reason", ""))}


def _create_params(service, path, query, body):
    return 200, service.create_parameter_set(body.get("params", body))


def _create_mapping(service, path, query, body):
    return 200, service.create_region_mapping(body.get("entries", body))


def _create_version(service, path, query, body):
    return 200, {"version": service.create_version(
        batch_ids=body.get("batch_ids"), param_id=body.get("param_id"),
        mapping_id=body.get("mapping_id"), scenario=body.get("scenario"))}


def _list_versions(service, path, query, body):
    return 200, {"versions": service.list_versions()}


def _get_version(service, path, query, body):
    return 200, {"version": service.get_version(path["id"])}


def _derive(service, path, query, body):
    return 200, {"version": service.derive_version(
        path["id"], scenario=body.get("scenario"), param_id=body.get("param_id"),
        mapping_id=body.get("mapping_id"), batch_ids=body.get("batch_ids"))}


def _compute(service, path, query, body):
    started = service.start_compute(path["id"])
    job = started["job"]
    wait = _truthy(body.get("wait")) or _truthy(_first(query, "wait"))
    if job["status"] in ("PENDING", "RUNNING"):
        if wait:
            try:
                service.run_compute(job["job_id"])
            except JobInterrupted:
                pass  # 任务可续跑，返回当前进度
            job = service.get_job(job["job_id"])
        else:
            _spawn_background(service, job["job_id"])
    return 202, {"job": job, "created": started["created"]}


def _sign(service, path, query, body):
    return 200, {"version": service.sign_version(path["id"], signed_by=body.get("signed_by"))}


def _revoke(service, path, query, body):
    return 200, {"version": service.revoke_version(
        path["id"], revoked_by=body.get("revoked_by"), reason=body.get("reason"))}


def _results(service, path, query, body):
    return 200, service.get_results(path["id"])


def _lineage(service, path, query, body):
    return 200, service.verify_lineage(path["id"])


def _decisions_of(service, path, query, body):
    return 200, {"decisions": service.list_decisions(path["id"])}


def _decide(service, path, query, body):
    return 200, {"decision": service.add_decision(
        body.get("version_id"), service_area=body.get("service_area"),
        action=body.get("action"), units=body.get("units"),
        decided_by=body.get("decided_by"), note=body.get("note", ""))}


def _compare(service, path, query, body):
    return 200, service.compare_versions(_first(query, "a"), _first(query, "b"))


def _get_job(service, path, query, body):
    return 200, {"job": service.get_job(path["id"])}


def _resume_job(service, path, query, body):
    try:
        job = service.run_compute(path["id"])
    except JobInterrupted:
        job = service.get_job(path["id"])
    return 202, {"job": job}


def _health(service, path, query, body):
    return 200, {"status": "ok"}


ROUTES = [
    ("GET", r"/api/health", _health),
    ("POST", r"/api/batches", _create_batch),
    ("GET", r"/api/batches", _list_batches),
    ("GET", r"/api/batches/(?P<id>[^/]+)", _get_batch),
    ("POST", r"/api/corrections", _create_correction),
    ("POST", r"/api/parameter-sets", _create_params),
    ("POST", r"/api/region-mappings", _create_mapping),
    ("POST", r"/api/versions", _create_version),
    ("GET", r"/api/versions", _list_versions),
    ("GET", r"/api/versions/(?P<id>[^/]+)", _get_version),
    ("POST", r"/api/versions/(?P<id>[^/]+)/derive", _derive),
    ("POST", r"/api/versions/(?P<id>[^/]+)/compute", _compute),
    ("POST", r"/api/versions/(?P<id>[^/]+)/sign", _sign),
    ("POST", r"/api/versions/(?P<id>[^/]+)/revoke", _revoke),
    ("GET", r"/api/versions/(?P<id>[^/]+)/results", _results),
    ("GET", r"/api/versions/(?P<id>[^/]+)/lineage", _lineage),
    ("GET", r"/api/versions/(?P<id>[^/]+)/decisions", _decisions_of),
    ("POST", r"/api/decisions", _decide),
    ("GET", r"/api/compare", _compare),
    ("GET", r"/api/jobs/(?P<id>[^/]+)", _get_job),
    ("POST", r"/api/jobs/(?P<id>[^/]+)/resume", _resume_job),
]
COMPILED_ROUTES = [(method, re.compile(pattern), fn) for method, pattern, fn in ROUTES]

_BACKGROUND_THREADS: list[threading.Thread] = []


def _truthy(value) -> bool:
    return str(value or "").strip().lower() in ("1", "true", "yes", "on")


def _first(query: dict, key: str):
    values = query.get(key)
    return values[0] if values else None


def _spawn_background(service, job_id: str) -> None:
    def _run():
        try:
            service.run_compute(job_id)
        except Exception:  # 后台线程只推进任务，状态落库可查
            traceback.print_exc()

    thread = threading.Thread(target=_run, name=f"compute-{job_id}", daemon=True)
    _BACKGROUND_THREADS.append(thread)
    thread.start()


class ApiHandler(BaseHTTPRequestHandler):
    """请求处理器；service 挂在 server 实例上。"""

    server_version = "ForecastVersioning/1.0"
    protocol_version = "HTTP/1.1"

    def do_GET(self):
        self._dispatch("GET")

    def do_POST(self):
        self._dispatch("POST")

    def log_message(self, *args):  # 静默访问日志
        pass

    def _dispatch(self, method: str) -> None:
        parsed = urlparse(self.path)
        query = parse_qs(parsed.query)
        for route_method, pattern, handler in COMPILED_ROUTES:
            match = pattern.fullmatch(parsed.path)
            if route_method == method and match:
                self._handle(handler, match.groupdict(), query)
                return
        self._send(404, {"error": {"code": "not_found", "message": "资源不存在", "details": {}}})

    def _handle(self, handler, path_params: dict, query: dict) -> None:
        try:
            body = self._read_body()
            status, payload = handler(self.server.service, path_params, query, body)
            self._send(status, payload)
        except DomainError as exc:
            self._send(ERROR_STATUS.get(exc.code, 400),
                       {"error": {"code": exc.code, "message": exc.message,
                                  "details": exc.details}})
        except Exception:
            traceback.print_exc()
            self._send(500, {"error": {"code": "internal",
                                       "message": "内部错误", "details": {}}})

    def _read_body(self) -> dict:
        length = int(self.headers.get("Content-Length") or 0)
        if length == 0:
            return {}
        raw = self.rfile.read(length)
        try:
            body = json.loads(raw.decode("utf-8"))
        except (ValueError, UnicodeDecodeError):
            from .domain import ValidationError
            raise ValidationError("请求体必须是合法 JSON")
        if not isinstance(body, dict):
            from .domain import ValidationError
            raise ValidationError("请求体必须是 JSON 对象")
        return body

    def _send(self, status: int, payload: dict) -> None:
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)


def make_server(service, host: str = "127.0.0.1", port: int = 8080) -> ThreadingHTTPServer:
    """构建 HTTP 服务；service 为 ForecastService 实例。"""
    server = ThreadingHTTPServer((host, port), ApiHandler)
    server.service = service
    server.daemon_threads = True
    return server
