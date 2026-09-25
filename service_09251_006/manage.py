"""离线管理命令：导入、计算、签署、撤销、谱系核验等。

用法：
    python3 -m service_09251_006.manage [--db PATH] <命令> [参数]

数据库路径优先级：--db 选项 > 环境变量 S09251_006_DB >
~/.local/share/service_09251_006/app.db（运行数据不写入源码目录）。
"""
from __future__ import annotations

import argparse
import json
import os
import sys

from .domain import DomainError, JobInterrupted
from .services import ForecastService
from .storage import Repository

ENV_DB = "S09251_006_DB"
DEFAULT_DB = os.path.join(
    os.path.expanduser("~"), ".local", "share", "service_09251_006", "app.db")


def _default_db() -> str:
    return os.environ.get(ENV_DB) or DEFAULT_DB


def _load_json_arg(text: str):
    """参数值既可以是 JSON 文本，也可以是 @文件路径 或文件路径。"""
    if text.startswith("@"):
        with open(text[1:], "r", encoding="utf-8") as fh:
            return json.load(fh)
    if os.path.exists(text):
        with open(text, "r", encoding="utf-8") as fh:
            return json.load(fh)
    return json.loads(text)


def _print(payload) -> None:
    print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=False))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="manage", description="充电需求预测版本管理：离线管理命令")
    parser.add_argument("--db", default=None, help="SQLite 数据库路径")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("init-db", help="初始化数据库（建库建表）")

    serve = sub.add_parser("serve", help="启动 HTTP API 服务")
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8080)

    imp = sub.add_parser("import-batch", help="导入原始数据批次（幂等）")
    imp.add_argument("file", help="JSON 文件：{source, records[], note?}")
    imp.add_argument("--corrects", default=None, help="被更正的历史批次 ID")
    imp.add_argument("--correction-reason", default="")
    imp.add_argument("--correction-stations", default=None,
                     help="逗号分隔的站点列表，缺省为整批")
    imp.add_argument("--correction-range-start", default=None)
    imp.add_argument("--correction-range-end", default=None)

    corr = sub.add_parser("register-correction", help="为已导入批次登记更正")
    corr.add_argument("--target", required=True, help="被更正批次 ID")
    corr.add_argument("--correction-batch", default=None, help="更正数据批次 ID")
    corr.add_argument("--stations", default=None, help="逗号分隔站点，缺省为整批")
    corr.add_argument("--range-start", default=None)
    corr.add_argument("--range-end", default=None)
    corr.add_argument("--reason", default="")

    params = sub.add_parser("create-params", help="创建参数集（JSON 或 @文件）")
    params.add_argument("payload")

    mapping = sub.add_parser("create-mapping", help="创建区域映射（JSON 或 @文件）")
    mapping.add_argument("payload")

    version = sub.add_parser("create-version", help="创建根版本")
    version.add_argument("--batches", required=True, help="逗号分隔批次 ID")
    version.add_argument("--params", required=True)
    version.add_argument("--mapping", required=True)
    version.add_argument("--scenario", required=True)

    derive = sub.add_parser("derive", help="从任一版本派生情景")
    derive.add_argument("parent")
    derive.add_argument("--scenario", required=True)
    derive.add_argument("--params", default=None)
    derive.add_argument("--mapping", default=None)
    derive.add_argument("--batches", default=None, help="逗号分隔批次 ID")

    compute = sub.add_parser("compute", help="发起并执行计算（可重入，中断后续跑）")
    compute.add_argument("version")

    resume = sub.add_parser("resume-job", help="续跑中断的计算任务")
    resume.add_argument("job")

    sign = sub.add_parser("sign", help="签署已计算版本")
    sign.add_argument("version")
    sign.add_argument("--by", required=True, dest="signed_by")

    revoke = sub.add_parser("revoke", help="撤销版本（保留历史）")
    revoke.add_argument("version")
    revoke.add_argument("--by", required=True, dest="revoked_by")
    revoke.add_argument("--reason", required=True)

    compare = sub.add_parser("compare", help="比较两版本的服务区峰值与置信区间")
    compare.add_argument("a")
    compare.add_argument("b")

    lineage = sub.add_parser("verify-lineage", help="谱系核验")
    lineage.add_argument("version")

    decide = sub.add_parser("decide", help="把已签署版本与调度决策关联")
    decide.add_argument("version")
    decide.add_argument("--area", required=True)
    decide.add_argument("--action", required=True)
    decide.add_argument("--units", type=int, required=True)
    decide.add_argument("--by", required=True, dest="decided_by")
    decide.add_argument("--note", default="")

    show = sub.add_parser("show-version", help="查看版本详情")
    show.add_argument("version")

    results = sub.add_parser("show-results", help="查看版本计算结果")
    results.add_argument("version")

    job = sub.add_parser("show-job", help="查看任务状态")
    job.add_argument("job")

    decisions = sub.add_parser("list-decisions", help="列出调度决策")
    decisions.add_argument("--version", default=None)

    sub.add_parser("list-versions", help="列出版本")
    sub.add_parser("list-batches", help="列出批次")
    return parser


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    db_path = args.db or _default_db()
    service = ForecastService(Repository(db_path))
    try:
        return _run(service, args)
    except DomainError as exc:
        _print({"error": {"code": exc.code, "message": exc.message,
                          "details": exc.details}})
        return 2


def _run(service: ForecastService, args) -> int:
    cmd = args.command

    if cmd == "init-db":
        _print({"db": service.repo.path, "status": "ready"})
    elif cmd == "serve":
        from .api import make_server
        server = make_server(service, args.host, args.port)
        print(f"API 服务已启动: http://{args.host}:{args.port}/api/health",
              file=sys.stderr)
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            server.shutdown()
    elif cmd == "import-batch":
        payload = _load_json_arg(args.file)
        stations = (args.correction_stations.split(",")
                    if args.correction_stations else None)
        time_range = None
        if args.correction_range_start or args.correction_range_end:
            time_range = [args.correction_range_start, args.correction_range_end]
        _print(service.import_batch(
            source=payload.get("source"), records=payload.get("records"),
            note=payload.get("note", ""), corrects=args.corrects,
            correction_reason=args.correction_reason,
            correction_stations=stations, correction_range=time_range))
    elif cmd == "register-correction":
        stations = args.stations.split(",") if args.stations else None
        time_range = None
        if args.range_start or args.range_end:
            time_range = [args.range_start, args.range_end]
        _print({"correction": service.register_correction(
            target_batch_id=args.target, correction_batch_id=args.correction_batch,
            stations=stations, time_range=time_range, reason=args.reason)})
    elif cmd == "create-params":
        _print(service.create_parameter_set(_load_json_arg(args.payload)))
    elif cmd == "create-mapping":
        _print(service.create_region_mapping(_load_json_arg(args.payload)))
    elif cmd == "create-version":
        _print({"version": service.create_version(
            batch_ids=args.batches.split(","), param_id=args.params,
            mapping_id=args.mapping, scenario=args.scenario)})
    elif cmd == "derive":
        _print({"version": service.derive_version(
            args.parent, scenario=args.scenario, param_id=args.params,
            mapping_id=args.mapping,
            batch_ids=args.batches.split(",") if args.batches else None)})
    elif cmd == "compute":
        started = service.start_compute(args.version)
        try:
            service.run_compute(started["job"]["job_id"])
        except JobInterrupted:
            print("任务中断，可用 resume-job 续跑", file=sys.stderr)
        _print({"job": service.get_job(started["job"]["job_id"]),
                "created": started["created"]})
    elif cmd == "resume-job":
        try:
            service.run_compute(args.job)
        except JobInterrupted:
            print("任务再次中断，可继续 resume-job", file=sys.stderr)
        _print({"job": service.get_job(args.job)})
    elif cmd == "sign":
        _print({"version": service.sign_version(args.version, signed_by=args.signed_by)})
    elif cmd == "revoke":
        _print({"version": service.revoke_version(
            args.version, revoked_by=args.revoked_by, reason=args.reason)})
    elif cmd == "compare":
        _print(service.compare_versions(args.a, args.b))
    elif cmd == "verify-lineage":
        _print(service.verify_lineage(args.version))
    elif cmd == "decide":
        _print({"decision": service.add_decision(
            args.version, service_area=args.area, action=args.action,
            units=args.units, decided_by=args.decided_by, note=args.note)})
    elif cmd == "show-version":
        _print({"version": service.get_version(args.version)})
    elif cmd == "show-results":
        _print(service.get_results(args.version))
    elif cmd == "show-job":
        _print({"job": service.get_job(args.job)})
    elif cmd == "list-decisions":
        _print({"decisions": service.list_decisions(args.version)})
    elif cmd == "list-versions":
        _print({"versions": service.list_versions()})
    elif cmd == "list-batches":
        _print({"batches": service.list_batches()})
    return 0


if __name__ == "__main__":
    sys.exit(main())
