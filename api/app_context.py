"""
应用上下文装配模块

用途：
- 统一装配主线服务
- 减少 server.py / learning_worker.py 重复初始化
- 为 Agent V1 提供可选挂接点
"""
from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


class AppContext(dict):
    """简单上下文字典对象。"""
    pass


def build_app_context():
    from cache_db import CacheDB
    from data_fetcher import TushareDataFetcher
    from factor_engine import FactorEngine
    from backtest_engine import BacktestEngine

    context = AppContext()

    db = CacheDB()
    fetcher = TushareDataFetcher(db=db)
    factor_engine = FactorEngine()
    backtest_engine = BacktestEngine()

    context["db"] = db
    context["fetcher"] = fetcher
    context["factor_engine"] = factor_engine
    context["backtest_engine"] = backtest_engine

    # 以下组件允许按你主线实际情况逐步补齐
    try:
        from point_in_time import PointInTimeDataBuilder
        context["point_in_time_builder"] = PointInTimeDataBuilder(db=db)
    except Exception as exc:
        logger.warning("point_in_time_builder init skipped: %s", exc)
        context["point_in_time_builder"] = None

    try:
        from model_engine import ModelEngine, PortfolioEngine
        context["model_engine"] = ModelEngine(db=db)
        context["portfolio_engine"] = PortfolioEngine()
    except Exception as exc:
        logger.warning("model_engine / portfolio_engine init skipped: %s", exc)
        context["model_engine"] = None
        context["portfolio_engine"] = None

    # Agent V1 可选挂接
    try:
        from agent_v1_entry import AgentV1Entry
        context["agent_v1"] = AgentV1Entry()
    except Exception as exc:
        logger.info("agent_v1 entry not attached: %s", exc)
        context["agent_v1"] = None

    return context
