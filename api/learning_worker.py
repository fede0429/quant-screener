"""
Standalone daily learning worker.
"""
import logging
import os
import threading
from zoneinfo import ZoneInfo

from apscheduler.schedulers.blocking import BlockingScheduler

from cache_db import CacheDB
from data_fetcher import TushareDataFetcher
from factor_engine import FactorEngine
from backtest_engine import BacktestEngine
from learning_engine import DailyLearningEngine
from model_engine import ModelEngine, PortfolioEngine
from monitor_engine import MonitorEngine
from portfolio_advisor import PortfolioAdvisor
from portfolio_lab import PortfolioLab

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

job_lock = threading.Lock()


def _env_flag(name: str, default: bool) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _env_int(name: str, default: int) -> int:
    value = os.environ.get(name)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError:
        return default


def _env_horizons(name: str, default: list[int]) -> list[int]:
    raw = os.environ.get(name)
    if not raw:
        return default
    result = []
    for item in raw.split(","):
        item = item.strip()
        if not item:
            continue
        try:
            result.append(int(item))
        except ValueError:
            continue
    return result or default


def _env_float(name: str, default: float) -> float:
    value = os.environ.get(name)
    if value is None:
        return default
    try:
        return float(value)
    except ValueError:
        return default


def _env_items(name: str, default: list[str]) -> list[str]:
    raw = os.environ.get(name)
    if not raw:
        return list(default)
    items = [item.strip() for item in raw.split(",") if item.strip()]
    return items or list(default)


def _env_int_list(name: str, default: list[int]) -> list[int]:
    items = _env_items(name, [str(item) for item in default])
    result = []
    for item in items:
        try:
            result.append(int(item))
        except ValueError:
            continue
    return result or list(default)


def _env_float_list(name: str, default: list[float]) -> list[float]:
    items = _env_items(name, [str(item) for item in default])
    result = []
    for item in items:
        try:
            result.append(float(item))
        except ValueError:
            continue
    return result or list(default)


def _ensure_fetcher_connected(db: CacheDB, fetcher: TushareDataFetcher):
    if fetcher.is_connected():
        return
    token = os.environ.get("TUSHARE_TOKEN", "").strip() or db.get_setting("tushare_token")
    if not token:
        raise RuntimeError("No Tushare token configured for learning worker.")
    result = fetcher.set_token(token)
    if not result.get("success"):
        raise RuntimeError(result.get("error", "Failed to connect to Tushare Pro"))


def run_job():
    if not job_lock.acquire(blocking=False):
        logger.warning("Learning worker skipped because another job is still running.")
        return

    db = None
    try:
        db_path = os.environ.get("CACHE_DB_PATH", "/app/data/cache.db")
        db = CacheDB(db_path)
        fetcher = TushareDataFetcher(db)
        factor_engine = FactorEngine()
        backtest_engine = BacktestEngine()
        learning_engine = DailyLearningEngine(db=db, factor_engine=factor_engine)
        model_engine = ModelEngine(db=db)
        portfolio_engine = PortfolioEngine()
        monitor_engine = MonitorEngine(
            db=db,
            factor_engine=factor_engine,
            model_engine=model_engine,
            portfolio_engine=portfolio_engine,
            learning_engine=learning_engine,
        )
        portfolio_lab = PortfolioLab(backtest_engine=backtest_engine)
        portfolio_advisor = PortfolioAdvisor(
            db=db,
            factor_engine=factor_engine,
            model_engine=model_engine,
            portfolio_engine=portfolio_engine,
            learning_engine=learning_engine,
            portfolio_lab=portfolio_lab,
        )
        learning_top_n = _env_int("LEARNING_TOP_N", 20)
        learning_horizons = _env_horizons("LEARNING_LABEL_HORIZONS", [5, 20, 60])
        learning_horizon = _env_int("LEARNING_HORIZON", 20)

        if _env_flag("LEARNING_REFRESH_BEFORE_RUN", True):
            _ensure_fetcher_connected(db, fetcher)
            logger.info("Running incremental data refresh before daily learning...")
            fetcher.refresh_all(full_refresh=False)

        result = learning_engine.run_daily_cycle(
            top_n=learning_top_n,
            horizons=learning_horizons,
            learning_horizon=learning_horizon,
            lookback_runs=_env_int("LEARNING_LOOKBACK_RUNS", 60),
            min_labeled_rows=_env_int("LEARNING_MIN_LABELED_ROWS", 200),
            auto_apply=_env_flag("LEARNING_AUTO_APPLY", True),
        )
        logger.info(
            "Daily learning finished for %s with active weights %s",
            result["run_date"],
            result["active_weights"],
        )

        if _env_flag("MODEL_MONITOR_PERSIST", True):
            try:
                monitor_payload = monitor_engine.build_live_payload(
                    horizon_days=learning_horizon,
                    top_n=_env_int("MODEL_MONITOR_TOP_N", learning_top_n),
                    model_weight=_env_float("MODEL_MONITOR_WEIGHT", 0.35),
                    neutralize_by=os.environ.get("MODEL_MONITOR_NEUTRALIZE_BY", "sector"),
                    max_position_weight=_env_float("MODEL_MONITOR_MAX_POSITION_WEIGHT", 0.05),
                    max_sector_weight=_env_float("MODEL_MONITOR_MAX_SECTOR_WEIGHT", 0.25),
                    max_industry_weight=_env_float("MODEL_MONITOR_MAX_INDUSTRY_WEIGHT", 0.15),
                    max_positions_per_sector=_env_int("MODEL_MONITOR_MAX_POSITIONS_PER_SECTOR", 4),
                    max_positions_per_industry=_env_int("MODEL_MONITOR_MAX_POSITIONS_PER_INDUSTRY", 2),
                    persist=True,
                )
                logger.info(
                    "Archived model monitor report for %s with model %s",
                    monitor_payload.get("report_date"),
                    monitor_payload.get("model", {}).get("model_id"),
                )
            except Exception:
                logger.exception("Daily model monitor archive failed")

        if _env_flag("PORTFOLIO_PROFILE_AUTO_OPTIMIZE", True):
            try:
                profile_result = portfolio_advisor.optimize_profile(
                    optimize_for=os.environ.get("PORTFOLIO_OPTIMIZE_FOR", "alpha_turnover"),
                    start_date=os.environ.get("PORTFOLIO_OPTIMIZE_START_DATE") or "2025-10-01",
                    end_date=os.environ.get("PORTFOLIO_OPTIMIZE_END_DATE"),
                    top_results=_env_int("PORTFOLIO_OPTIMIZE_TOP_RESULTS", 5),
                    max_combinations=_env_int("PORTFOLIO_OPTIMIZE_MAX_COMBINATIONS", 20),
                    min_improvement=_env_float("PORTFOLIO_OPTIMIZE_MIN_IMPROVEMENT", 0.25),
                    force_activate=_env_flag("PORTFOLIO_OPTIMIZE_FORCE_ACTIVATE", False),
                    grid={
                        "frequency": _env_items("PORTFOLIO_FREQUENCY_VALUES", ["monthly"]),
                        "top_n": _env_int_list("PORTFOLIO_TOP_N_VALUES", [30, 40]),
                        "portfolio_top_n": _env_int_list("PORTFOLIO_PORTFOLIO_TOP_N_VALUES", [15, 20]),
                        "use_model": [True],
                        "model_horizon": [learning_horizon],
                        "model_weight": _env_float_list("PORTFOLIO_MODEL_WEIGHT_VALUES", [0.35]),
                        "neutralize_by": _env_items("PORTFOLIO_NEUTRALIZE_BY_VALUES", ["sector"]),
                        "max_position_weight": _env_float_list("PORTFOLIO_MAX_POSITION_WEIGHT_VALUES", [0.05]),
                        "max_sector_weight": _env_float_list("PORTFOLIO_MAX_SECTOR_WEIGHT_VALUES", [0.25]),
                        "max_industry_weight": _env_float_list("PORTFOLIO_MAX_INDUSTRY_WEIGHT_VALUES", [0.15]),
                        "max_positions_per_sector": _env_int_list("PORTFOLIO_MAX_POSITIONS_PER_SECTOR_VALUES", [4]),
                        "max_positions_per_industry": _env_int_list("PORTFOLIO_MAX_POSITIONS_PER_INDUSTRY_VALUES", [2]),
                        "transaction_cost_bps": _env_float_list("PORTFOLIO_TRANSACTION_COST_BPS_VALUES", [10.0]),
                        "rebalance_buffer": _env_int_list("PORTFOLIO_REBALANCE_BUFFER_VALUES", [0, 5]),
                        "max_new_positions": _env_int_list("PORTFOLIO_MAX_NEW_POSITIONS_VALUES", [4, 8]),
                        "min_holding_periods": _env_int_list("PORTFOLIO_MIN_HOLDING_PERIODS_VALUES", [0, 1]),
                    },
                )
                logger.info(
                    "Portfolio profile optimization finished. active=%s profile=%s",
                    profile_result.get("activation_applied"),
                    (profile_result.get("active_profile") or {}).get("profile_id"),
                )
            except Exception:
                logger.exception("Daily portfolio profile optimization failed")

        if _env_flag("PORTFOLIO_SIGNAL_PERSIST", True):
            try:
                signal_payload = portfolio_advisor.run_signal(persist=True)
                logger.info(
                    "Archived portfolio signal report for %s using profile %s",
                    signal_payload.get("signal_date"),
                    (signal_payload.get("profile") or {}).get("profile_id"),
                )
            except Exception:
                logger.exception("Daily portfolio signal archive failed")
    except Exception:
        logger.exception("Learning worker job failed")
    finally:
        if db is not None:
            db.close()
        job_lock.release()


def main():
    timezone_name = os.environ.get("LEARNING_TIMEZONE", "Asia/Shanghai")
    scheduler = BlockingScheduler(timezone=ZoneInfo(timezone_name))
    hour = _env_int("LEARNING_RUN_HOUR", 18)
    minute = _env_int("LEARNING_RUN_MINUTE", 10)

    scheduler.add_job(
        run_job,
        trigger="cron",
        hour=hour,
        minute=minute,
        id="daily_learning",
        replace_existing=True,
        max_instances=1,
    )

    if _env_flag("LEARNING_RUN_ON_START", False):
        run_job()

    logger.info(
        "Learning worker started. Schedule: %02d:%02d %s",
        hour,
        minute,
        timezone_name,
    )
    scheduler.start()


if __name__ == "__main__":
    main()
