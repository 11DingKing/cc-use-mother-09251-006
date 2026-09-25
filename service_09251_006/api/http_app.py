"""HTTP API（标准库实现，无第三方依赖）。

路由概览：
    POST   /api/inputs/raw                 导入原始批次（重复批次幂等）
    POST   /api/parameters                 导入参数
    POST   /api/region-maps                导入区域映射
    POST   /api/versions/{vid}/corrections 原始数据更正
    GET    /api/versions[?kind=]           版本清单
    GET    /api/versions/{vid}             版本内容与元数据

    POST   /api/scenarios                  创建情景
    GET    /api/scenarios                  情景清单
    GET    /api/scenarios/{sid}            情景详情与提交链
    POST   /api/scenarios/{sid}/derive     派生（add/replace/set_*）
    POST   /api/scenarios/{sid}/forecasts  提交预测作业（?wait=1 同步执行）
    GET    /api/scenarios/{sid}/forecasts  情景下的预测运行

    GET    /api/jobs/{jid}                 作业状态（含检查点游标）
    POST   /api/jobs/{jid}/run             执行/续跑作业
    POST   /api/jobs/run-available         排空队列（工作循环）

    GET    /api/forecasts/{fvid}           预测内容
    GET    /api/forecasts/compare?left=&right=  比较峰值与置信区间
    GET    /api/forecasts/{fvid}/verify    谱系核验
    POST   /api/forecasts/{fvid}/adopt     采用并关联调度决策
    POST   /api/forecasts/{fvid}/revoke    撤销
    GET    /api/forecasts/{fvid}/adoption  签署事件历史

    GET    /api/corrections                更正与影响计数
    GET    /api/invalidations[?forecast=]  失效标记
    GET    /api/health
"""
from __future__ import annotations

import json
import re
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Callable
from urllib.parse import parse_qs, urlsplit

from ..container import Application
from ..errors import (
    ConflictError,
    InvalidStateError,
    LineageError,
    NotFoundError,
    ValidationError,
    VersionError,
)

_STATUS = {
    NotFoundError: 404,
    ValidationError: 422,
    ConflictError: 409,
    InvalidStateError: 409,
    LineageError: 409,
}


def make_handler(app: Application) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        server_version = "VersionedForecast/1.0"

        def log_message(self, fmt: str, *args: Any) -> None:  # 安静日志
            app_path = getattr(self.server, "log_sink", None)
            if app_path is not None:
                app_path("%s - %s", self.address_string(), fmt % args)

        # ---- 框架 -----------------------------------------------------

        def _send_json(self, status: int, body: Any) -> None:
            data = json.dumps(body, ensure_ascii=False, indent=2).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def _read_json(self) -> dict[str, Any]:
            length = int(self.headers.get("Content-Length") or 0)
            if length == 0:
                return {}
            raw = self.rfile.read(length)
            try:
                doc = json.loads(raw.decode("utf-8"))
            except (json.JSONDecodeError, UnicodeDecodeError) as exc:
                raise ValidationError(f"请求体不是合法 JSON: {exc}")
            if not isinstance(doc, dict):
                raise ValidationError("请求体必须是 JSON 对象")
            return doc

        def _handle(self, fn: Callable[[], Any]) -> None:
            try:
                result = fn()
            except VersionError as exc:
                self._send_json(_STATUS.get(type(exc), 400), {"error": exc.to_dict()})
            except Exception as exc:  # noqa: BLE001
                self._send_json(500, {"error": {
                    "code": "internal", "message": f"{type(exc).__name__}: {exc}"}})
            else:
                self._send_json(200, result)

        def _actor(self, body: dict[str, Any]) -> str | None:
            return body.get("actor") or self.headers.get("X-Actor")

        # ---- 路由 -----------------------------------------------------

        def do_GET(self) -> None:  # noqa: N802
            parts = urlsplit(self.path)
            p, q = parts.path, parse_qs(parts.query)
            self._handle(lambda: self._route_get(p, q))

        def do_POST(self) -> None:  # noqa: N802
            parts = urlsplit(self.path)
            self._handle(lambda: self._route_post(parts.path, self._read_json()))

        def _route_get(self, p: str, q: dict[str, list[str]]) -> Any:
            v = app.versions
            if p == "/api/health":
                return {"status": "ok", "data_dir": app.data_dir}
            if p == "/api/versions":
                kind = q.get("kind", [None])[0]
                rows = app.repo.list_versions(kind)
                return {"versions": [dict(r) for r in rows]}
            if p == "/api/scenarios":
                return {"scenarios": app.list_scenarios()}
            if p == "/api/corrections":
                return {"corrections": app.list_corrections()}
            if p == "/api/invalidations":
                return {"invalidations": app.invalidations(
                    q.get("forecast", [None])[0])}
            if p == "/api/forecasts/compare":
                left = _need(q, "left"); right = _need(q, "right")
                return v.compare_forecasts(left, right)

            m = re.fullmatch(r"/api/versions/(v\d+)", p)
            if m:
                vid = m.group(1)
                row = app.repo.get_version_row(vid)
                return {"metadata": dict(row), "content": app.repo.load_object(vid)}

            m = re.fullmatch(r"/api/scenarios/(\w+)", p)
            if m:
                return v.scenario_detail(m.group(1))

            m = re.fullmatch(r"/api/scenarios/(\w+)/forecasts", p)
            if m:
                rows = app.repo.list_forecasts_for_scenario(m.group(1))
                return {"runs": [dict(r) for r in rows]}

            m = re.fullmatch(r"/api/jobs/(\w+)", p)
            if m:
                return app.forecasts.job_status(m.group(1))

            m = re.fullmatch(r"/api/forecasts/(v\d+)", p)
            if m:
                return app.repo.get_forecast(m.group(1))

            m = re.fullmatch(r"/api/forecasts/(v\d+)/verify", p)
            if m:
                return v.verify_lineage(m.group(1))

            m = re.fullmatch(r"/api/forecasts/(v\d+)/adoption", p)
            if m:
                return v.adoption_status(m.group(1))

            raise NotFoundError("未知路径", path=p)

        def _route_post(self, p: str, body: dict[str, Any]) -> Any:
            v, f = app.versions, app.forecasts

            if p == "/api/inputs/raw":
                return v.import_raw_batch(
                    body, note=body.get("note"), created_by=self._actor(body))
            if p == "/api/parameters":
                return v.import_parameters(
                    body, note=body.get("note"), created_by=self._actor(body))
            if p == "/api/region-maps":
                return v.import_region_map(
                    body, note=body.get("note"), created_by=self._actor(body))
            if p == "/api/scenarios":
                return v.create_scenario(
                    parameters_version_id=body.get("parameters_version_id"),
                    mapping_version_id=body.get("mapping_version_id"),
                    message=body.get("message", "创建情景"),
                    created_by=self._actor(body))
            if p == "/api/jobs/run-available":
                n = f.run_available(owner=body.get("owner"))
                return {"processed": n}

            m = re.fullmatch(r"/api/versions/(v\d+)/corrections", p)
            if m:
                doc = {k: body[k] for k in
                       ("source", "service_area", "records", "unit") if k in body}
                return v.correct_raw_batch(
                    m.group(1), doc, reason=body.get("reason"),
                    created_by=self._actor(body))

            m = re.fullmatch(r"/api/scenarios/(\w+)/derive", p)
            if m:
                return self._derive(m.group(1), body)

            m = re.fullmatch(r"/api/scenarios/(\w+)/forecasts", p)
            if m:
                submitted = f.submit_forecast(
                    m.group(1), commit_id=body.get("commit_id"),
                    idempotency_key=body.get("idempotency_key"),
                    created_by=self._actor(body))
                if body.get("wait", False) and not submitted.get("deduped"):
                    out = f.run_job(submitted["job_id"])
                    return {"submission": submitted, "run": out}
                return submitted

            m = re.fullmatch(r"/api/jobs/(\w+)/run", p)
            if m:
                return f.run_job(m.group(1), owner=body.get("owner"))

            m = re.fullmatch(r"/api/forecasts/(v\d+)/adopt", p)
            if m:
                return v.adopt(
                    m.group(1), body.get("decision"),
                    actor=self._actor(body), reason=body.get("reason"))

            m = re.fullmatch(r"/api/forecasts/(v\d+)/revoke", p)
            if m:
                return v.revoke(
                    m.group(1), actor=self._actor(body),
                    reason=body.get("reason"))

            raise NotFoundError("未知路径", path=p)

        def _derive(self, sid: str, body: dict[str, Any]) -> Any:
            action = body.get("action")
            v = app.versions
            actor = self._actor(body)
            msg = body.get("message")
            base = body.get("base_commit_id")
            if action == "add_input":
                return v.add_input(sid, body["input_version_id"],
                                   message=msg, created_by=actor,
                                   base_commit_id=base)
            if action == "replace_input":
                return v.replace_input(
                    sid, body["old_version_id"], body["new_version_id"],
                    message=msg, created_by=actor, base_commit_id=base)
            if action == "set_parameters":
                return v.set_parameters(sid, body["parameters_version_id"],
                                        message=msg, created_by=actor,
                                        base_commit_id=base)
            if action == "set_mapping":
                return v.set_mapping(sid, body["mapping_version_id"],
                                     message=msg, created_by=actor,
                                     base_commit_id=base)
            raise ValidationError(
                "action 必须是 add_input/replace_input/set_parameters/set_mapping",
                action=action)

    return Handler


def _need(q: dict[str, list[str]], key: str) -> str:
    vals = q.get(key)
    if not vals or not vals[0]:
        raise ValidationError(f"缺少查询参数 {key}")
    return vals[0]


def create_server(
    app: Application, host: str = "127.0.0.1", port: int = 8080,
) -> ThreadingHTTPServer:
    server = ThreadingHTTPServer((host, port), make_handler(app))
    server.allow_reuse_address = True
    return server


def serve(app: Application, host: str = "127.0.0.1", port: int = 8080) -> None:
    httpd = create_server(app, host, port)
    print(f"充电需求预测版本管理服务已启动: http://{host}:{port}  数据目录={app.data_dir}")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        httpd.server_close()
