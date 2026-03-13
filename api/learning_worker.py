"""
后台学习 / 定时任务入口（收口示例）

说明：
- 通过 AppContext 统一初始化
- 减少与 server.py 的重复逻辑
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

logger.info("learning_worker context initialized")


def run_once():
    logger.info("learning_worker run_once started")
    return {
        "db": db is not None,
        "fetcher": fetcher is not None,
        "factor_engine": factor_engine is not None,
        "backtest_engine": backtest_engine is not None,
        "point_in_time_builder": point_in_time_builder is not None,
        "model_engine": model_engine is not None,
        "portfolio_engine": portfolio_engine is not None,
        "agent_v1": agent_v1 is not None,
    }


if __name__ == "__main__":
    print(run_once())
