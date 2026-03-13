"""
主服务入口（收口示例）

说明：
- 通过 AppContext 统一初始化
- 保留最小可运行结构
- 具体路由可按你现有主线继续补充
"""
from __future__ import annotations

import logging

from app_context import build_app_context

logger = logging.getLogger(__name__)

CTX = build_app_context()

db = CTX["db"]
fetcher = CTX["fetcher"]
factor_engine = CTX["factor_engine"]
backtest_engine = CTX["backtest_engine"]
point_in_time_builder = CTX.get("point_in_time_builder")
model_engine = CTX.get("model_engine")
portfolio_engine = CTX.get("portfolio_engine")
agent_v1 = CTX.get("agent_v1")

logger.info("server context initialized")


def get_runtime_context():
    return {
        "db": db,
        "fetcher": fetcher,
        "factor_engine": factor_engine,
        "backtest_engine": backtest_engine,
        "point_in_time_builder": point_in_time_builder,
        "model_engine": model_engine,
        "portfolio_engine": portfolio_engine,
        "agent_v1": agent_v1,
    }


if __name__ == "__main__":
    logger.info("server bootstrap ready")
    print("server bootstrap ready")
