"""充电需求预测版本管理的服务端包入口。"""
from .services import ForecastService
from .storage import Repository

PROJECT_CODE = "service_09251_006"


def project_info() -> dict[str, str]:
    """返回稳定的项目标识。"""
    return {"code": PROJECT_CODE, "title": "充电需求预测版本管理"}


def build_service(db_path: str, **kwargs) -> ForecastService:
    """便捷构造：给定数据库路径返回应用服务。"""
    return ForecastService(Repository(db_path), **kwargs)
