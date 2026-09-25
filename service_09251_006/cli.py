"""离线管理命令：导入、计算、签署、撤销、谱系核验与工作循环。

示例::

    python -m service_09251_006.cli import-raw --file batch.json --actor zhang
    python -m service_09251_006.cli scenario-create --params v000001 --mapping v000002
    python -m service_09251_006.cli derive scn_xxx add-input v000003
    python -m service_09251_006.cli forecast scn_xxx --wait
    python -m service_09251_006.cli verify v000007
    python -m service_09251_006.cli adopt v000007 --decision deploy.json
    python -m service_09251_006.cli revoke v000007 --reason 参数修订
    python -m service_09251_006.cli worker          # 排空队列（重启后续跑）
"""
from __future__ import annotations

import argparse
import json
import sys
from typing import Any

from .container import Application, DEFAULT_DATA_ENV
from .errors import VersionError


def _load_doc(path: str | None) -> dict[str, Any]:
    raw = open(path, "r", encoding="utf-8").read() if path else sys.stdin.read()
    doc = json.loads(raw)
    if not isinstance(doc, dict):
        raise SystemExit("JSON 文档必须是对象")
    return doc


def _emit(obj: Any) -> None:
    json.dump(obj, sys.stdout, ensure_ascii=False, indent=2)
    sys.stdout.write("\n")


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="service_09251_006",
                                description="充电需求预测版本管理离线命令")
    p.add_argument("--data-dir", default=None,
                   help=f"数据目录（默认环境变量 {DEFAULT_DATA_ENV}）")
    sub = p.add_subparsers(dest="cmd", required=True)

    sp = sub.add_parser("import-raw", help="导入原始车流/充电需求批次")
    sp.add_argument("--file", help="JSON 文件，缺省读 stdin")
    sp.add_argument("--actor"); sp.add_argument("--note")

    sp = sub.add_parser("import-params", help="导入预测参数")
    sp.add_argument("--file"); sp.add_argument("--actor"); sp.add_argument("--note")

    sp = sub.add_parser("import-mapping", help="导入区域映射")
    sp.add_argument("--file"); sp.add_argument("--actor"); sp.add_argument("--note")

    sp = sub.add_parser("correct", help="登记原始数据更正（历史不变，标记下游）")
    sp.add_argument("version_id"); sp.add_argument("--file", required=True)
    sp.add_argument("--reason"); sp.add_argument("--actor")

    sp = sub.add_parser("list-versions", help="列出版本")
    sp.add_argument("--kind", choices=[
        "raw_input", "parameters", "region_map", "forecast", "decision"])

    sp = sub.add_parser("show-version", help="查看版本内容")
    sp.add_argument("version_id")

    sp = sub.add_parser("scenario-create", help="创建情景")
    sp.add_argument("--params"); sp.add_argument("--mapping")
    sp.add_argument("--message"); sp.add_argument("--actor")

    sp = sub.add_parser("scenario-show", help="查看情景与提交链")
    sp.add_argument("scenario_id")

    sp = sub.add_parser("list-scenarios", help="列出情景")

    sp = sub.add_parser("derive", help="从当前 HEAD 派生提交")
    sp.add_argument("scenario_id")
    sp.add_argument("action", choices=[
        "add-input", "replace-input", "set-params", "set-mapping"])
    sp.add_argument("versions", nargs="+", help="add/set: 一个版本；replace: 旧 新")
    sp.add_argument("--message"); sp.add_argument("--actor")
    sp.add_argument("--base", help="所基于的父提交 ID（乐观并发控制）")

    sp = sub.add_parser("forecast", help="提交预测并可选同步执行")
    sp.add_argument("scenario_id")
    sp.add_argument("--commit", help="指定提交，缺省 HEAD")
    sp.add_argument("--wait", action="store_true", help="提交后在本进程执行")
    sp.add_argument("--idempotency-key"); sp.add_argument("--actor")

    sp = sub.add_parser("job-status", help="查看作业状态与检查点")
    sp.add_argument("job_id")

    sp = sub.add_parser("run-job", help="执行/续跑指定作业")
    sp.add_argument("job_id"); sp.add_argument("--owner")

    sp = sub.add_parser("worker", help="认领并执行全部可运行作业（重启续跑）")
    sp.add_argument("--owner"); sp.add_argument("--limit", type=int, default=100)

    sp = sub.add_parser("compare", help="比较两版预测的峰值与置信区间")
    sp.add_argument("left"); sp.add_argument("right")

    sp = sub.add_parser("verify", help="谱系核验（哈希+精确复算+失效状态）")
    sp.add_argument("version_id")

    sp = sub.add_parser("adopt", help="采用预测并关联调度决策")
    sp.add_argument("version_id"); sp.add_argument("--decision",
                    help="调度决策 JSON 文件")
    sp.add_argument("--actor"); sp.add_argument("--reason")

    sp = sub.add_parser("revoke", help="撤销采用")
    sp.add_argument("version_id"); sp.add_argument("--actor"); sp.add_argument("--reason")

    sp = sub.add_parser("adoption", help="查看签署/撤销事件")
    sp.add_argument("version_id")

    sp = sub.add_parser("list-corrections", help="更正与影响计数")
    sp = sub.add_parser("list-invalidations", help="失效标记清单")
    sp.add_argument("forecast_id", nargs="?")

    sp = sub.add_parser("serve", help="启动 HTTP 服务")
    sp.add_argument("--host", default="127.0.0.1")
    sp.add_argument("--port", type=int, default=8080)
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    app = Application(args.data_dir)
    v, f = app.versions, app.forecasts
    try:
        c = args.cmd
        if c == "import-raw":
            _emit(v.import_raw_batch(_load_doc(args.file), note=args.note,
                                     created_by=args.actor))
        elif c == "import-params":
            _emit(v.import_parameters(_load_doc(args.file), note=args.note,
                                      created_by=args.actor))
        elif c == "import-mapping":
            _emit(v.import_region_map(_load_doc(args.file), note=args.note,
                                      created_by=args.actor))
        elif c == "correct":
            _emit(v.correct_raw_batch(args.version_id, _load_doc(args.file),
                                      reason=args.reason, created_by=args.actor))
        elif c == "list-versions":
            _emit({"versions": [dict(r) for r in app.repo.list_versions(args.kind)]})
        elif c == "show-version":
            row = app.repo.get_version_row(args.version_id)
            _emit({"metadata": dict(row), "content": app.repo.load_object(args.version_id)})
        elif c == "scenario-create":
            _emit(v.create_scenario(parameters_version_id=args.params,
                                    mapping_version_id=args.mapping,
                                    message=args.message, created_by=args.actor))
        elif c == "scenario-show":
            _emit(v.scenario_detail(args.scenario_id))
        elif c == "list-scenarios":
            _emit({"scenarios": app.list_scenarios()})
        elif c == "derive":
            _emit(_derive(v, args))
        elif c == "forecast":
            submitted = f.submit_forecast(
                args.scenario_id, commit_id=args.commit,
                idempotency_key=args.idempotency_key, created_by=args.actor)
            if args.wait and not submitted.get("deduped"):
                _emit({"submission": submitted,
                       "run": f.run_job(submitted["job_id"])})
            else:
                _emit(submitted)
        elif c == "job-status":
            _emit(f.job_status(args.job_id))
        elif c == "run-job":
            _emit(f.run_job(args.job_id, owner=args.owner))
        elif c == "worker":
            _emit({"processed": f.run_available(owner=args.owner,
                                                limit=args.limit)})
        elif c == "compare":
            _emit(v.compare_forecasts(args.left, args.right))
        elif c == "verify":
            result = v.verify_lineage(args.version_id)
            _emit(result)
            return 0 if result["ok"] else 2
        elif c == "adopt":
            decision = _load_doc(args.decision) if args.decision else None
            _emit(v.adopt(args.version_id, decision, actor=args.actor,
                          reason=args.reason))
        elif c == "revoke":
            _emit(v.revoke(args.version_id, actor=args.actor, reason=args.reason))
        elif c == "adoption":
            _emit(v.adoption_status(args.version_id))
        elif c == "list-corrections":
            _emit({"corrections": app.list_corrections()})
        elif c == "list-invalidations":
            _emit({"invalidations": app.invalidations(args.forecast_id)})
        elif c == "serve":
            from .api.http_app import serve
            serve(app, host=args.host, port=args.port)
    except VersionError as exc:
        json.dump({"error": exc.to_dict()}, sys.stderr, ensure_ascii=False, indent=2)
        sys.stderr.write("\n")
        return 1
    return 0


def _derive(v: Any, args: argparse.Namespace) -> Any:
    sid, actor, msg, base = (
        args.scenario_id, args.actor, args.message, getattr(args, "base", None))
    if args.action == "add-input":
        return v.add_input(sid, args.versions[0], message=msg, created_by=actor,
                           base_commit_id=base)
    if args.action == "replace-input":
        if len(args.versions) != 2:
            raise SystemExit("replace-input 需要: <旧版本> <新版本>")
        return v.replace_input(sid, args.versions[0], args.versions[1],
                               message=msg, created_by=actor, base_commit_id=base)
    if args.action == "set-params":
        return v.set_parameters(sid, args.versions[0], message=msg, created_by=actor,
                                base_commit_id=base)
    return v.set_mapping(sid, args.versions[0], message=msg, created_by=actor,
                         base_commit_id=base)


if __name__ == "__main__":
    raise SystemExit(main())
