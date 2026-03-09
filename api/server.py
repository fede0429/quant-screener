"""
FastAPI server for the quant screener.
"""
import json
import logging
import os
import threading
import time
from contextlib import asynccontextmanager
from typing import Optional

import numpy as np
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from backtest_engine import BacktestEngine
from cache_db import CacheDB
from data_fetcher import TushareDataFetcher
from factor_engine import FactorEngine
from learning_engine import DEFAULT_WEIGHTS, DailyLearningEngine
from model_engine import DEFAULT_FEATURE_COLUMNS, ModelEngine, PortfolioEngine
from monitor_engine import MonitorEngine
from point_in_time import PointInTimeDataBuilder
from portfolio_advisor import PortfolioAdvisor
from portfolio_lab import PortfolioLab

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

db: CacheDB = None
fetcher: TushareDataFetcher = None
factor_engine: FactorEngine = None
backtest_engine: BacktestEngine = None
learning_engine: DailyLearningEngine = None
model_engine: ModelEngine = None
portfolio_engine: PortfolioEngine = None
monitor_engine: MonitorEngine = None
portfolio_lab: PortfolioLab = None
portfolio_advisor: PortfolioAdvisor = None

refresh_state = {
    "running": False,
    "progress": "",
    "step": "",
    "pct": 0,
    "error": None,
    "last_result": None,
    "start_time": None,
    "end_time": None,
}
refresh_lock = threading.Lock()


def _ensure_fetcher_connected():
    if fetcher.is_connected():
        return

    token = os.environ.get("TUSHARE_TOKEN", "").strip() or db.get_setting("tushare_token")
    if not token:
        raise RuntimeError("Please configure a Tushare Pro token first.")

    result = fetcher.set_token(token)
    if not result.get("success"):
        raise RuntimeError(result.get("error", "Failed to connect to Tushare Pro"))


def _run_refresh_background(full: bool):
    global refresh_state
    try:
        refresh_state["running"] = True
        refresh_state["error"] = None
        refresh_state["last_result"] = None
        refresh_state["start_time"] = time.time()
        refresh_state["end_time"] = None

        _ensure_fetcher_connected()

        refresh_state["step"] = "fetch_stock_list"
        refresh_state["progress"] = "Loading stock universe..."
        refresh_state["pct"] = 1
        stats = {}
        stats["stocks"] = fetcher.refresh_all_step_stocks()
        total_stocks = stats["stocks"].get("count", 0)

        if full:
            refresh_state["step"] = "fetch_prices"
            refresh_state["progress"] = f"Fetching {total_stocks} stocks of price history..."
            refresh_state["pct"] = 2

            def price_cb(done, total, records):
                pct = 2 + int(done / max(total, 1) * 55)
                refresh_state["pct"] = min(pct, 57)
                refresh_state["progress"] = f"Prices: {done}/{total} ({records} rows)"

            stats["prices"] = fetcher.fetch_all_prices_with_callback(days=750, callback=price_cb)
        else:
            refresh_state["step"] = "incremental_prices"
            refresh_state["progress"] = "Refreshing latest prices..."
            refresh_state["pct"] = 10
            stats["prices"] = fetcher._fetch_incremental_prices()
            refresh_state["pct"] = 57

        if full:
            refresh_state["step"] = "financials"
            refresh_state["progress"] = f"Fetching financials for {total_stocks} stocks..."
            refresh_state["pct"] = 58

            def fin_cb(done, total):
                pct = 58 + int(done / max(total, 1) * 32)
                refresh_state["pct"] = min(pct, 90)
                refresh_state["progress"] = f"Financials: {done}/{total}"

            stats["financials"] = fetcher.fetch_financials_with_callback(callback=fin_cb)
        else:
            refresh_state["pct"] = 90

        refresh_state["step"] = "indicators"
        refresh_state["progress"] = "Fetching valuation indicators..."
        refresh_state["pct"] = 91
        stats["indicators"] = fetcher._fetch_indicators(
            days=750 if full else 180,
            full_refresh=full,
        )
        refresh_state["pct"] = 95

        refresh_state["step"] = "benchmark"
        refresh_state["progress"] = "Fetching CSI300 benchmark..."
        refresh_state["pct"] = 96
        stats["benchmark"] = fetcher._fetch_benchmark(days=750 if full else 60)

        from datetime import datetime

        db.save_setting("last_refresh", datetime.now().isoformat())
        elapsed = round(time.time() - refresh_state["start_time"], 1)
        refresh_state["pct"] = 100
        refresh_state["step"] = "done"
        refresh_state["progress"] = f"Refresh completed in {elapsed}s"
        refresh_state["last_result"] = {
            "success": True,
            "elapsed_seconds": elapsed,
            "stats": stats,
        }
        refresh_state["end_time"] = time.time()
        logger.info("Data refresh completed in %ss", elapsed)

    except Exception as exc:
        logger.exception("Data refresh failed")
        refresh_state["error"] = str(exc)
        refresh_state["progress"] = f"Refresh failed: {exc}"
        refresh_state["end_time"] = time.time()
    finally:
        refresh_state["running"] = False


@asynccontextmanager
async def lifespan(app):
    global db, fetcher, factor_engine, backtest_engine, learning_engine, model_engine, portfolio_engine, monitor_engine, portfolio_lab, portfolio_advisor
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

    env_token = os.environ.get("TUSHARE_TOKEN", "").strip()
    if env_token:
        result = fetcher.set_token(env_token)
        if result.get("success"):
            logger.info("Loaded Tushare token from environment")
        else:
            logger.warning("Failed to validate Tushare token: %s", result.get("error"))

    logger.info("Backend service started")
    yield
    db.close()
    logger.info("Backend service stopped")


app = FastAPI(title="A-share Quant Screener API", version="1.7.0", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


class TokenRequest(BaseModel):
    token: str



class ScreenerRequest(BaseModel):
    weights: dict = Field(default_factory=lambda: dict(DEFAULT_WEIGHTS))
    filters: dict = Field(default_factory=dict)
    sectors: list = Field(default_factory=list)
    use_model: bool = Field(default=True)
    model_horizon: int = Field(default=20, ge=1, le=250)
    model_weight: float = Field(default=0.35, ge=0.0, le=1.0)
    build_portfolio: bool = Field(default=True)
    portfolio_top_n: int = Field(default=20, ge=1, le=100)
    neutralize_by: str = Field(default="sector")
    max_position_weight: float = Field(default=0.05, gt=0.0, le=1.0)
    max_sector_weight: float = Field(default=0.25, gt=0.0, le=1.0)
    max_industry_weight: float = Field(default=0.15, gt=0.0, le=1.0)
    max_positions_per_sector: int = Field(default=4, ge=1, le=50)
    max_positions_per_industry: int = Field(default=2, ge=1, le=50)
    current_holdings: list = Field(default_factory=list)
    rebalance_buffer: int = Field(default=0, ge=0, le=250)
    max_new_positions: Optional[int] = Field(default=None, ge=0, le=100)
    min_holding_periods: int = Field(default=0, ge=0, le=12)


class BacktestRequest(BaseModel):

    weights: dict = Field(default_factory=lambda: dict(DEFAULT_WEIGHTS))
    filters: dict = Field(default_factory=dict)
    sectors: list = Field(default_factory=list)
    frequency: str = Field(default="monthly")
    top_n: int = Field(default=10)
    start_date: Optional[str] = None
    end_date: Optional[str] = None



class PortfolioBacktestRequest(BaseModel):
    weights: dict = Field(default_factory=lambda: dict(DEFAULT_WEIGHTS))
    filters: dict = Field(default_factory=dict)
    sectors: list = Field(default_factory=list)
    frequency: str = Field(default="monthly")
    top_n: int = Field(default=20, ge=1, le=100)
    portfolio_top_n: int = Field(default=20, ge=1, le=100)
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    use_model: bool = Field(default=True)
    model_horizon: int = Field(default=20, ge=1, le=250)
    model_weight: float = Field(default=0.35, ge=0.0, le=1.0)
    neutralize_by: str = Field(default="sector")
    max_position_weight: float = Field(default=0.05, gt=0.0, le=1.0)
    max_sector_weight: float = Field(default=0.25, gt=0.0, le=1.0)
    max_industry_weight: float = Field(default=0.15, gt=0.0, le=1.0)
    max_positions_per_sector: int = Field(default=4, ge=1, le=50)
    max_positions_per_industry: int = Field(default=2, ge=1, le=50)
    transaction_cost_bps: float = Field(default=10.0, ge=0.0, le=500.0)
    rebalance_buffer: int = Field(default=0, ge=0, le=250)
    max_new_positions: Optional[int] = Field(default=None, ge=0, le=100)
    min_holding_periods: int = Field(default=0, ge=0, le=12)


class PortfolioSweepRequest(BaseModel):
    weights: dict = Field(default_factory=lambda: dict(DEFAULT_WEIGHTS))
    filters: dict = Field(default_factory=dict)
    sectors: list = Field(default_factory=list)
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    optimize_for: str = Field(default="alpha_turnover")
    top_results: int = Field(default=10, ge=1, le=50)
    max_combinations: int = Field(default=60, ge=1, le=200)
    frequency_values: list[str] = Field(default_factory=lambda: ["monthly"])
    top_n_values: list[int] = Field(default_factory=lambda: [40])
    portfolio_top_n_values: list[int] = Field(default_factory=lambda: [20])
    use_model_values: list[bool] = Field(default_factory=lambda: [True])
    model_horizon_values: list[int] = Field(default_factory=lambda: [20])
    model_weight_values: list[float] = Field(default_factory=lambda: [0.35])
    neutralize_by_values: list[str] = Field(default_factory=lambda: ["sector"])
    max_position_weight_values: list[float] = Field(default_factory=lambda: [0.05])
    max_sector_weight_values: list[float] = Field(default_factory=lambda: [0.25])
    max_industry_weight_values: list[float] = Field(default_factory=lambda: [0.15])
    max_positions_per_sector_values: list[int] = Field(default_factory=lambda: [4])
    max_positions_per_industry_values: list[int] = Field(default_factory=lambda: [2])
    transaction_cost_bps_values: list[float] = Field(default_factory=lambda: [10.0])
    rebalance_buffer_values: list[int] = Field(default_factory=lambda: [0])
    max_new_positions_values: list[int] = Field(default_factory=lambda: [-1])
    min_holding_periods_values: list[int] = Field(default_factory=lambda: [0])



class PortfolioProfileOptimizeRequest(PortfolioSweepRequest):
    name: Optional[str] = None
    min_improvement: float = Field(default=0.25, ge=0.0, le=100.0)
    force_activate: bool = Field(default=False)
    weights: Optional[dict] = None
    filters: dict = Field(default_factory=dict)
    sectors: list = Field(default_factory=list)


class PortfolioSignalRunRequest(BaseModel):
    profile_id: Optional[str] = None
    persist: bool = Field(default=True)


class LearningRunRequest(BaseModel):


    top_n: int = Field(default=20, ge=1, le=100)
    label_horizons: list[int] = Field(default_factory=lambda: [5, 20, 60])
    learning_horizon: int = Field(default=20, ge=1, le=250)
    lookback_runs: int = Field(default=60, ge=10, le=500)
    min_labeled_rows: int = Field(default=200, ge=50, le=200000)
    auto_apply: bool = Field(default=True)
    refresh_before_run: bool = Field(default=False)
    weights: Optional[dict] = None
    filters: dict = Field(default_factory=dict)
    sectors: list = Field(default_factory=list)


class LearningBootstrapRequest(BaseModel):
    frequency: str = Field(default="monthly")
    max_runs: int = Field(default=6, ge=1, le=60)
    use_indicators: bool = Field(default=False)
    overwrite_existing: bool = Field(default=False)
    top_n: int = Field(default=20, ge=1, le=100)
    label_horizons: list[int] = Field(default_factory=lambda: [5, 20, 60])
    learning_horizon: int = Field(default=20, ge=1, le=250)
    lookback_runs: int = Field(default=60, ge=10, le=500)
    min_labeled_rows: int = Field(default=200, ge=50, le=200000)
    auto_apply: bool = Field(default=True)
    weights: Optional[dict] = None
    filters: dict = Field(default_factory=dict)
    sectors: list = Field(default_factory=list)


class ModelTrainRequest(BaseModel):
    horizon_days: int = Field(default=20, ge=1, le=250)
    validation_runs: int = Field(default=1, ge=1, le=12)
    min_train_runs: int = Field(default=2, ge=1, le=60)
    min_rows: int = Field(default=1000, ge=100, le=500000)
    task_type: str = Field(default="classification")
    target_column: str = Field(default="alpha")
    label_mode: str = Field(default="alpha_top_quantile")
    label_quantile: float = Field(default=0.20, gt=0.0, lt=1.0)
    alpha_threshold: float = Field(default=0.0)
    feature_columns: Optional[list[str]] = None
    score_latest_snapshot: bool = Field(default=True)
    latest_limit: int = Field(default=20, ge=1, le=200)
    activate: bool = Field(default=True)
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    promotion_gate: Optional[dict] = None
    force_activate: bool = Field(default=False)


@app.get("/api/health")
def health():
    active_model = None
    if model_engine:
        active_model = model_engine.get_serving_model_registry()
    if not active_model and db:
        raw_active_model = db.get_latest_model_registry(active_only=True)
        if raw_active_model:
            active_model = model_engine._enrich_registry(raw_active_model)
    return {
        "status": "ok",
        "tushare_connected": fetcher.is_connected() if fetcher else False,
        "cached_stocks": db.count_stocks() if db else 0,
        "cache_age_hours": db.cache_age_hours() if db else None,
        "learning_snapshot_count": db.count_feature_snapshots() if db else 0,
        "learning_label_count": db.count_learning_labels() if db else 0,
        "indicator_history_count": db.count_indicator_history() if db else 0,
        "latest_indicator_history_date": db.get_latest_indicator_history_date() if db else None,
        "model_registry_count": db.count_model_registry() if db else 0,
        "model_prediction_count": db.count_model_predictions() if db else 0,
        "model_monitor_report_count": db.count_model_monitor_reports() if db else 0,
        "portfolio_profile_count": db.count_portfolio_profiles() if db else 0,
        "portfolio_signal_report_count": db.count_portfolio_signal_reports() if db else 0,
        "active_portfolio_profile_id": (portfolio_advisor.get_active_profile() or {}).get("profile_id") if portfolio_advisor else None,
        "active_model_id": active_model["model_id"] if active_model else None,
        "active_model_serving_ready": active_model.get("serving_ready") if active_model else False,
    }



def _run_ranked_screener_pipeline(
    weights: dict,
    filters: dict,
    sectors: list,
    use_model: bool,
    model_horizon: int,
    model_weight: float,
):
    stocks = db.get_all_stocks_with_data()
    if not stocks:
        raise RuntimeError("No cached stock data. Refresh data first.")

    results = factor_engine.score_and_rank(
        stocks=stocks,
        weights=weights,
        filters=filters,
        sectors=sectors,
    )
    model_meta = {
        "applied": False,
        "reason": "Model blending disabled by request.",
    }
    if use_model:
        blended = model_engine.blend_results(
            results=results,
            horizon_days=model_horizon,
            model_weight=model_weight,
        )
        results = blended.pop("results")
        model_meta = blended
    return results, model_meta



def _build_portfolio_preview(
    results: list[dict],
    top_n: int,
    neutralize_by: str,
    max_position_weight: float,
    max_sector_weight: float,
    max_industry_weight: float,
    max_positions_per_sector: int,
    max_positions_per_industry: int,
    current_holdings: list | None = None,
    rebalance_buffer: int = 0,
    max_new_positions: int | None = None,
    min_holding_periods: int = 0,
):
    return portfolio_engine.construct(
        results=results,
        top_n=top_n,
        neutralize_by=neutralize_by,
        max_position_weight=max_position_weight,
        max_sector_weight=max_sector_weight,
        max_industry_weight=max_industry_weight,
        max_positions_per_sector=max_positions_per_sector,
        max_positions_per_industry=max_positions_per_industry,
        existing_holdings=current_holdings,
        rebalance_buffer=rebalance_buffer,
        max_new_positions=max_new_positions,
        min_holding_periods=min_holding_periods,
    )


def _build_score_distribution(results: list[dict]) -> dict:
    def summarize(values: list[float]) -> dict | None:
        cleaned = [float(value) for value in values if value is not None]
        if not cleaned:
            return None
        arr = np.array(cleaned, dtype=float)
        return {
            "min": round(float(np.min(arr)), 4),
            "p50": round(float(np.percentile(arr, 50)), 4),
            "p90": round(float(np.percentile(arr, 90)), 4),
            "max": round(float(np.max(arr)), 4),
        }

    return {
        "candidate_count": len(results),
        "final_score": summarize([item.get("final_score", item.get("composite_score")) for item in results]),
        "model_score": summarize([item.get("model_score") for item in results]),
    }


def _get_monitor_registry(horizon_days: Optional[int]) -> dict:
    registry = model_engine.get_serving_model_registry(horizon_days=horizon_days)
    if registry:
        return registry
    raw_registry = db.get_latest_model_registry(horizon_days=horizon_days, active_only=False)
    if not raw_registry:
        raise RuntimeError("No trained model available yet.")
    return model_engine._enrich_registry(raw_registry)


def _build_model_monitor_summary(
    registry: dict,
    model_meta: dict,
    portfolio: dict,
    score_distribution: dict,
) -> str:
    metrics = registry.get("metrics_json") or {}
    top_holdings_text = "\n".join(
        f"- #{item['portfolio_rank']} {item['code']} {item['name']} ({item.get('sector') or '-'}) w={item['target_weight']}% score={item.get('final_score')}"
        for item in portfolio.get("holdings", [])[:10]
    ) or "- No holdings"
    sector_text = "\n".join(
        f"- {item['sector']}: {item['positions']} positions / {item['weight']}%"
        for item in portfolio.get("sector_exposure", [])[:8]
    ) or "- No sector exposure"
    return (
        f"# Model Monitor {registry.get('model_id')}\n\n"
        f"## Validation\n"
        f"- serving ready: {registry.get('serving_ready')}\n"
        f"- rank_ic: {metrics.get('rank_ic')}\n"
        f"- top20 alpha lift: {metrics.get('top20_alpha_lift')}\n"
        f"- precision@20 lift: {metrics.get('precision_at_20_lift')}\n"
        f"- fold count: {metrics.get('fold_count')}\n\n"
        f"## Live Monitor\n"
        f"- model applied: {model_meta.get('applied')}\n"
        f"- candidate count: {score_distribution.get('candidate_count')}\n"
        f"- final score distribution: {json.dumps(score_distribution.get('final_score'), ensure_ascii=False)}\n"
        f"- model score distribution: {json.dumps(score_distribution.get('model_score'), ensure_ascii=False)}\n"
        f"- portfolio holdings: {portfolio.get('selected_count')}\n"
        f"- cash buffer: {portfolio.get('cash_buffer')}%\n\n"
        f"## Sector Exposure\n"
        f"{sector_text}\n\n"
        f"## Holdings\n"
        f"{top_holdings_text}\n"
    )
@app.post("/api/token")
def set_token(req: TokenRequest):
    result = fetcher.set_token(req.token)
    if result.get("success"):
        db.save_setting("tushare_token", req.token)
        return {"success": True, "message": "Token verified", "info": result.get("info")}
    raise HTTPException(status_code=400, detail=result.get("error", "Token verification failed"))


@app.get("/api/token/status")
def token_status():
    saved_token = db.get_setting("tushare_token") if db else None
    if saved_token:
        fetcher.set_token(saved_token)
    return {
        "has_token": saved_token is not None,
        "connected": fetcher.is_connected() if fetcher else False,
        "token_preview": (
            f"{saved_token[:8]}...{saved_token[-4:]}"
            if saved_token and len(saved_token) > 12
            else None
        ),
    }


@app.post("/api/data/refresh")
def refresh_data(full: bool = Query(default=False)):
    with refresh_lock:
        if refresh_state["running"]:
            return {
                "success": True,
                "message": "Refresh task is already running.",
                "already_running": True,
            }

        refresh_state["running"] = True
        refresh_state["progress"] = "Starting..."
        refresh_state["step"] = "init"
        refresh_state["pct"] = 0
        refresh_state["error"] = None

    thread = threading.Thread(target=_run_refresh_background, args=(full,), daemon=True)
    thread.start()

    return {
        "success": True,
        "message": "Full refresh started." if full else "Incremental refresh started.",
        "already_running": False,
    }


@app.get("/api/data/refresh/status")
def refresh_status():
    elapsed = None
    if refresh_state["start_time"]:
        end = refresh_state["end_time"] or time.time()
        elapsed = round(end - refresh_state["start_time"], 1)

    return {
        "running": refresh_state["running"],
        "step": refresh_state["step"],
        "progress": refresh_state["progress"],
        "pct": refresh_state["pct"],
        "error": refresh_state["error"],
        "elapsed_seconds": elapsed,
        "result": refresh_state["last_result"],
    }


@app.get("/api/data/status")
def data_status():
    return {
        "stock_count": db.count_stocks(),
        "has_prices": db.has_table_data("daily_prices"),
        "has_financials": db.has_table_data("financials"),
        "has_indicators": db.has_table_data("indicators"),
        "has_indicator_history": db.has_table_data("indicator_history"),
        "last_refresh": db.get_setting("last_refresh"),
        "last_price_date": db.get_latest_price_date(),
        "last_indicator_history_date": db.get_latest_indicator_history_date(),
        "indicator_history_count": db.count_indicator_history(),
        "cache_age_hours": db.cache_age_hours(),
    }


@app.get("/api/stocks")
def list_stocks():
    stocks = db.get_all_stocks()
    if not stocks:
        raise HTTPException(status_code=404, detail="No stock data found. Refresh data first.")
    return {"count": len(stocks), "stocks": stocks}


@app.get("/api/stocks/{code}")
def get_stock_detail(code: str):
    stock = db.get_stock_full(code)
    if not stock:
        raise HTTPException(status_code=404, detail=f"Stock not found: {code}")
    return stock


@app.post("/api/screener")
def run_screener(req: ScreenerRequest):
    try:
        results, model_meta = _run_ranked_screener_pipeline(
            weights=req.weights,
            filters=req.filters,
            sectors=req.sectors,
            use_model=req.use_model,
            model_horizon=req.model_horizon,
            model_weight=req.model_weight,
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    portfolio = {
        "enabled": False,
        "reason": "Portfolio preview disabled by request.",
    }
    if req.build_portfolio:
        portfolio = _build_portfolio_preview(
            results=results,
            top_n=req.portfolio_top_n,
            neutralize_by=req.neutralize_by,
            max_position_weight=req.max_position_weight,
            max_sector_weight=req.max_sector_weight,
            max_industry_weight=req.max_industry_weight,
            max_positions_per_sector=req.max_positions_per_sector,
            max_positions_per_industry=req.max_positions_per_industry,
            current_holdings=req.current_holdings,
            rebalance_buffer=req.rebalance_buffer,
            max_new_positions=req.max_new_positions,
            min_holding_periods=req.min_holding_periods,
        )

    return {
        "count": len(results),
        "results": results,
        "model": model_meta,
        "portfolio": portfolio,
        "risk_policy": factor_engine.get_risk_policy(req.filters),
    }


@app.post("/api/backtest")
def run_backtest(req: BacktestRequest):
    all_prices = db.get_all_prices()
    benchmark = db.get_benchmark_prices(limit=None)
    stocks_data = db.get_all_stocks_with_data()
    point_in_time_builder = PointInTimeDataBuilder(db=db, all_prices=all_prices)

    if not all_prices or not stocks_data:
        raise HTTPException(status_code=404, detail="No cached data. Refresh data first.")

    results = backtest_engine.run(
        stocks_data=stocks_data,
        all_prices=all_prices,
        benchmark_prices=benchmark,
        factor_engine=factor_engine,
        weights=req.weights,
        filters=req.filters,
        sectors=req.sectors,
        frequency=req.frequency,
        top_n=req.top_n,
        start_date=req.start_date,
        end_date=req.end_date,
        point_in_time_builder=point_in_time_builder,
    )
    return results


@app.post("/api/portfolio/backtest")
def run_portfolio_backtest(req: PortfolioBacktestRequest):
    all_prices = db.get_all_prices()
    benchmark = db.get_benchmark_prices(limit=None)
    stocks_data = db.get_all_stocks_with_data()
    point_in_time_builder = PointInTimeDataBuilder(db=db, all_prices=all_prices)

    if not all_prices or not stocks_data:
        raise HTTPException(status_code=404, detail="No cached data. Refresh data first.")

    results = backtest_engine.run(
        stocks_data=stocks_data,
        all_prices=all_prices,
        benchmark_prices=benchmark,
        factor_engine=factor_engine,
        weights=req.weights,
        filters=req.filters,
        sectors=req.sectors,
        frequency=req.frequency,
        top_n=req.top_n,
        start_date=req.start_date,
        end_date=req.end_date,
        point_in_time_builder=point_in_time_builder,
        model_engine=model_engine,
        portfolio_engine=portfolio_engine,
        use_model=req.use_model,
        model_horizon=req.model_horizon,
        model_weight=req.model_weight,
        build_portfolio=True,
        portfolio_top_n=req.portfolio_top_n,
        neutralize_by=req.neutralize_by,
        max_position_weight=req.max_position_weight,
        max_sector_weight=req.max_sector_weight,
        max_industry_weight=req.max_industry_weight,
        max_positions_per_sector=req.max_positions_per_sector,
        max_positions_per_industry=req.max_positions_per_industry,
        transaction_cost_bps=req.transaction_cost_bps,
        rebalance_buffer=req.rebalance_buffer,
        max_new_positions=req.max_new_positions,
        min_holding_periods=req.min_holding_periods,
    )
    return results



@app.post("/api/portfolio/sweep")
def run_portfolio_sweep(req: PortfolioSweepRequest):
    all_prices = db.get_all_prices()
    benchmark = db.get_benchmark_prices(limit=None)
    stocks_data = db.get_all_stocks_with_data()
    point_in_time_builder = PointInTimeDataBuilder(db=db, all_prices=all_prices)

    if not all_prices or not stocks_data:
        raise HTTPException(status_code=404, detail="No cached data. Refresh data first.")

    try:
        return portfolio_lab.sweep(
            stocks_data=stocks_data,
            all_prices=all_prices,
            benchmark_prices=benchmark,
            factor_engine=factor_engine,
            weights=req.weights,
            filters=req.filters,
            sectors=req.sectors,
            point_in_time_builder=point_in_time_builder,
            model_engine=model_engine,
            portfolio_engine=portfolio_engine,
            start_date=req.start_date,
            end_date=req.end_date,
            grid={
                "frequency": req.frequency_values,
                "top_n": req.top_n_values,
                "portfolio_top_n": req.portfolio_top_n_values,
                "use_model": req.use_model_values,
                "model_horizon": req.model_horizon_values,
                "model_weight": req.model_weight_values,
                "neutralize_by": req.neutralize_by_values,
                "max_position_weight": req.max_position_weight_values,
                "max_sector_weight": req.max_sector_weight_values,
                "max_industry_weight": req.max_industry_weight_values,
                "max_positions_per_sector": req.max_positions_per_sector_values,
                "max_positions_per_industry": req.max_positions_per_industry_values,
                "transaction_cost_bps": req.transaction_cost_bps_values,
                "rebalance_buffer": req.rebalance_buffer_values,
                "max_new_positions": req.max_new_positions_values,
                "min_holding_periods": req.min_holding_periods_values,
            },
            optimize_for=req.optimize_for,
            top_results=req.top_results,
            max_combinations=req.max_combinations,
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        logger.exception("Portfolio sweep failed")
        raise HTTPException(status_code=500, detail=str(exc)) from exc



@app.post("/api/portfolio/profile/optimize")
def optimize_portfolio_profile(req: PortfolioProfileOptimizeRequest):
    try:
        return portfolio_advisor.optimize_profile(
            optimize_for=req.optimize_for,
            start_date=req.start_date,
            end_date=req.end_date,
            top_results=req.top_results,
            max_combinations=req.max_combinations,
            min_improvement=req.min_improvement,
            force_activate=req.force_activate,
            name=req.name,
            weights=req.weights,
            filters=req.filters,
            sectors=req.sectors,
            grid={
                "frequency": req.frequency_values,
                "top_n": req.top_n_values,
                "portfolio_top_n": req.portfolio_top_n_values,
                "use_model": req.use_model_values,
                "model_horizon": req.model_horizon_values,
                "model_weight": req.model_weight_values,
                "neutralize_by": req.neutralize_by_values,
                "max_position_weight": req.max_position_weight_values,
                "max_sector_weight": req.max_sector_weight_values,
                "max_industry_weight": req.max_industry_weight_values,
                "max_positions_per_sector": req.max_positions_per_sector_values,
                "max_positions_per_industry": req.max_positions_per_industry_values,
                "transaction_cost_bps": req.transaction_cost_bps_values,
                "rebalance_buffer": req.rebalance_buffer_values,
                "max_new_positions": req.max_new_positions_values,
                "min_holding_periods": req.min_holding_periods_values,
            },
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        logger.exception("Portfolio profile optimization failed")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/api/portfolio/profile/active")
def get_active_portfolio_profile():
    profile = portfolio_advisor.get_active_profile()
    if not profile:
        raise HTTPException(status_code=404, detail="No active portfolio profile available yet.")
    return profile


@app.get("/api/portfolio/profile/history")
def list_portfolio_profiles(
    limit: int = Query(default=20, ge=1, le=200),
    active_only: bool = Query(default=False),
):
    return portfolio_advisor.list_profiles(limit=limit, active_only=active_only)


@app.post("/api/portfolio/signal/run")
def run_portfolio_signal(req: PortfolioSignalRunRequest):
    try:
        return portfolio_advisor.run_signal(profile_id=req.profile_id, persist=req.persist)
    except RuntimeError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:
        logger.exception("Portfolio signal run failed")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/api/portfolio/signal/latest")
def latest_portfolio_signal(profile_id: Optional[str] = None):
    report = portfolio_advisor.get_latest_signal(profile_id=profile_id)
    if not report:
        raise HTTPException(status_code=404, detail="No portfolio signal report available yet.")
    return report


@app.get("/api/portfolio/signal/history")
def list_portfolio_signals(
    profile_id: Optional[str] = None,
    limit: int = Query(default=30, ge=1, le=365),
):
    return portfolio_advisor.list_signals(limit=limit, profile_id=profile_id)


@app.get("/api/portfolio/report/latest")
def latest_portfolio_report(profile_id: Optional[str] = None):
    report = portfolio_advisor.get_latest_signal(profile_id=profile_id)
    if not report:
        raise HTTPException(status_code=404, detail="No portfolio report available yet.")
    return report


@app.get("/api/benchmark")
def get_benchmark():
    data = db.get_benchmark_prices(limit=None)
    if not data:
        raise HTTPException(status_code=404, detail="No benchmark data found.")
    return {"code": "000300.SH", "name": "CSI300", "prices": data}


@app.get("/api/sectors")
def get_sectors():
    return {"sectors": db.get_sectors()}


@app.get("/api/learning/status")
def learning_status():
    return learning_engine.get_status()


@app.get("/api/learning/weights")
def learning_weights():
    return {
        "active_weights": learning_engine.get_active_weights(),
        "recommended_weights": db.get_setting_json("learning_recommended_weights"),
    }


@app.get("/api/learning/report/latest")
def latest_learning_report():
    report = db.get_latest_review_report()
    if not report:
        raise HTTPException(status_code=404, detail="No learning report available yet.")
    return report


@app.post("/api/learning/run")
def run_learning(req: LearningRunRequest):
    if req.refresh_before_run:
        _ensure_fetcher_connected()
        fetcher.refresh_all(full_refresh=False)

    try:
        result = learning_engine.run_daily_cycle(
            top_n=req.top_n,
            horizons=req.label_horizons,
            learning_horizon=req.learning_horizon,
            lookback_runs=req.lookback_runs,
            min_labeled_rows=req.min_labeled_rows,
            filters=req.filters,
            sectors=req.sectors,
            weights=req.weights,
            auto_apply=req.auto_apply,
        )
        return {"success": True, **result}
    except Exception as exc:
        logger.exception("Learning cycle failed")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.post("/api/data/indicator-history/backfill")
def backfill_indicator_history(
    days: int = Query(default=180, ge=5, le=2000),
    full: bool = Query(default=False),
):
    try:
        _ensure_fetcher_connected()
        return fetcher._fetch_indicators(days=days, full_refresh=full)
    except Exception as exc:
        logger.exception("Indicator history backfill failed")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.post("/api/learning/bootstrap")
def bootstrap_learning(req: LearningBootstrapRequest):
    try:
        bootstrap = learning_engine.bootstrap_snapshots(
            frequency=req.frequency,
            max_runs=req.max_runs,
            filters=req.filters,
            sectors=req.sectors,
            weights=req.weights,
            top_n=req.top_n,
            use_indicators=req.use_indicators,
            overwrite_existing=req.overwrite_existing,
        )
        cycle = learning_engine.run_daily_cycle(
            top_n=req.top_n,
            horizons=req.label_horizons,
            learning_horizon=req.learning_horizon,
            lookback_runs=req.lookback_runs,
            min_labeled_rows=req.min_labeled_rows,
            filters=req.filters,
            sectors=req.sectors,
            weights=req.weights,
            auto_apply=req.auto_apply,
        )
        return {"success": True, "bootstrap": bootstrap, "cycle": cycle}
    except Exception as exc:
        logger.exception("Learning bootstrap failed")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.post("/api/model/train")
def train_model(req: ModelTrainRequest):
    try:
        result = model_engine.train_model(
            horizon_days=req.horizon_days,
            validation_runs=req.validation_runs,
            min_train_runs=req.min_train_runs,
            min_rows=req.min_rows,
            task_type=req.task_type,
            target_column=req.target_column,
            label_mode=req.label_mode,
            label_quantile=req.label_quantile,
            alpha_threshold=req.alpha_threshold,
            feature_columns=req.feature_columns or list(DEFAULT_FEATURE_COLUMNS),
            score_latest_snapshot=req.score_latest_snapshot,
            latest_limit=req.latest_limit,
            activate=req.activate,
            start_date=req.start_date,
            end_date=req.end_date,
            promotion_gate=req.promotion_gate,
            force_activate=req.force_activate,
        )
        return {"success": True, **result}
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        logger.exception("Model training failed")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/api/model/latest")
def latest_model(
    horizon_days: Optional[int] = Query(default=None, ge=1, le=250),
    snapshot_date: Optional[str] = None,
    limit: int = Query(default=20, ge=1, le=200),
):
    try:
        return model_engine.get_latest_model(
            horizon_days=horizon_days,
            snapshot_date=snapshot_date,
            limit=limit,
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:
        logger.exception("Model lookup failed")
        raise HTTPException(status_code=500, detail=str(exc)) from exc



@app.get("/api/model/monitor/latest")
def latest_model_monitor(
    horizon_days: Optional[int] = Query(default=None, ge=1, le=250),
    top_n: int = Query(default=20, ge=1, le=100),
    model_weight: float = Query(default=0.35, ge=0.0, le=1.0),
    neutralize_by: str = Query(default="sector"),
    max_position_weight: float = Query(default=0.05, gt=0.0, le=1.0),
    max_sector_weight: float = Query(default=0.25, gt=0.0, le=1.0),
    max_industry_weight: float = Query(default=0.15, gt=0.0, le=1.0),
    max_positions_per_sector: int = Query(default=4, ge=1, le=50),
    max_positions_per_industry: int = Query(default=2, ge=1, le=50),
    persist: bool = Query(default=True),
):
    try:
        return monitor_engine.build_live_payload(
            horizon_days=horizon_days,
            top_n=top_n,
            model_weight=model_weight,
            neutralize_by=neutralize_by,
            max_position_weight=max_position_weight,
            max_sector_weight=max_sector_weight,
            max_industry_weight=max_industry_weight,
            max_positions_per_sector=max_positions_per_sector,
            max_positions_per_industry=max_positions_per_industry,
            persist=persist,
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:
        logger.exception("Model monitor lookup failed")
        raise HTTPException(status_code=500, detail=str(exc)) from exc



@app.get("/api/model/monitor/dashboard")
def model_monitor_dashboard(
    horizon_days: Optional[int] = Query(default=None, ge=1, le=250),
    limit: int = Query(default=60, ge=1, le=365),
):
    try:
        return monitor_engine.build_dashboard(limit=limit, horizon_days=horizon_days)
    except RuntimeError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:
        logger.exception("Model monitor dashboard lookup failed")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/api/model/monitor/history")
def model_monitor_history(
    horizon_days: Optional[int] = Query(default=None, ge=1, le=250),
    limit: int = Query(default=30, ge=1, le=365),
):
    try:
        return monitor_engine.get_history(limit=limit, horizon_days=horizon_days)
    except Exception as exc:
        logger.exception("Model monitor history lookup failed")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)



