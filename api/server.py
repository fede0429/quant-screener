"""
A股量化选股系统 — FastAPI 后端服务器
Tushare Pro 实时数据对接 + 因子计算 + 选股筛选 + 回测引擎
"""
import os
import time
import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from typing import Optional

from data_fetcher import TushareDataFetcher
from factor_engine import FactorEngine
from backtest_engine import BacktestEngine
from cache_db import CacheDB

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# ─── Global instances ─────────────────────────────────────────────────────────
db: CacheDB = None
fetcher: TushareDataFetcher = None
factor_engine: FactorEngine = None
backtest_engine: BacktestEngine = None


@asynccontextmanager
async def lifespan(app):
    global db, fetcher, factor_engine, backtest_engine
    import os
    db_path = os.environ.get("CACHE_DB_PATH", "/app/data/cache.db")
    db = CacheDB(db_path)
    fetcher = TushareDataFetcher(db)
    factor_engine = FactorEngine()
    backtest_engine = BacktestEngine()
    logger.info("✅ 后端服务启动完成")
    yield
    db.close()
    logger.info("🛑 后端服务关闭")


app = FastAPI(title="A股量化选股系统 API", version="1.0.0", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


# ─── Request / Response Models ────────────────────────────────────────────────
class TokenRequest(BaseModel):
    token: str


class ScreenerRequest(BaseModel):
    weights: dict = Field(default={"value": 25, "growth": 25, "quality": 25, "momentum": 25})
    filters: dict = Field(default={})
    sectors: list = Field(default=[])


class BacktestRequest(BaseModel):
    weights: dict = Field(default={"value": 25, "growth": 25, "quality": 25, "momentum": 25})
    filters: dict = Field(default={})
    sectors: list = Field(default=[])
    frequency: str = Field(default="monthly")
    top_n: int = Field(default=10)
    start_date: Optional[str] = None
    end_date: Optional[str] = None


# ─── Health Check ─────────────────────────────────────────────────────────────
@app.get("/api/health")
def health():
    return {
        "status": "ok",
        "tushare_connected": fetcher.is_connected() if fetcher else False,
        "cached_stocks": db.count_stocks() if db else 0,
        "cache_age_hours": db.cache_age_hours() if db else None,
    }


# ─── Token Management ────────────────────────────────────────────────────────
@app.post("/api/token")
def set_token(req: TokenRequest):
    """设置 Tushare Pro Token 并测试连接"""
    try:
        result = fetcher.set_token(req.token)
        if result["success"]:
            db.save_setting("tushare_token", req.token)
            return {"success": True, "message": "Token 验证成功", "info": result.get("info")}
        else:
            raise HTTPException(status_code=400, detail=result.get("error", "Token 验证失败"))
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/api/token/status")
def token_status():
    """检查当前 Token 状态"""
    saved_token = db.get_setting("tushare_token") if db else None
    if saved_token:
        fetcher.set_token(saved_token)
    return {
        "has_token": saved_token is not None,
        "connected": fetcher.is_connected() if fetcher else False,
        "token_preview": f"{saved_token[:8]}...{saved_token[-4:]}" if saved_token and len(saved_token) > 12 else None
    }


# ─── Data Refresh ─────────────────────────────────────────────────────────────
@app.post("/api/data/refresh")
def refresh_data(full: bool = Query(default=False)):
    """
    刷新数据。full=False 增量更新行情；full=True 全量刷新（含财务数据）。
    需要已设置 Token。
    """
    if not fetcher.is_connected():
        saved_token = db.get_setting("tushare_token")
        if saved_token:
            fetcher.set_token(saved_token)
        else:
            raise HTTPException(status_code=400, detail="请先设置 Tushare Pro Token")

    try:
        t0 = time.time()
        stats = fetcher.refresh_all(full_refresh=full)
        elapsed = round(time.time() - t0, 1)
        return {
            "success": True,
            "elapsed_seconds": elapsed,
            "stats": stats
        }
    except Exception as e:
        logger.exception("数据刷新失败")
        raise HTTPException(status_code=500, detail=f"数据刷新失败: {str(e)}")


@app.get("/api/data/status")
def data_status():
    """返回缓存数据的概况"""
    return {
        "stock_count": db.count_stocks(),
        "has_prices": db.has_table_data("daily_prices"),
        "has_financials": db.has_table_data("financials"),
        "has_indicators": db.has_table_data("indicators"),
        "last_refresh": db.get_setting("last_refresh"),
        "last_price_date": db.get_latest_price_date(),
        "cache_age_hours": db.cache_age_hours(),
    }


# ─── Stock Universe ──────────────────────────────────────────────────────────
@app.get("/api/stocks")
def list_stocks():
    """获取所有股票列表（从缓存）"""
    stocks = db.get_all_stocks()
    if not stocks:
        raise HTTPException(status_code=404, detail="暂无股票数据，请先刷新数据")
    return {"count": len(stocks), "stocks": stocks}


@app.get("/api/stocks/{code}")
def get_stock_detail(code: str):
    """获取个股详情：基本信息 + 行情 + 财务 + 技术指标"""
    stock = db.get_stock_full(code)
    if not stock:
        raise HTTPException(status_code=404, detail=f"未找到股票 {code}")
    return stock


# ─── Screener (核心选股) ──────────────────────────────────────────────────────
@app.post("/api/screener")
def run_screener(req: ScreenerRequest):
    """运行量化选股：计算因子 → 标准化 → 加权打分 → 排名"""
    stocks = db.get_all_stocks_with_data()
    if not stocks:
        raise HTTPException(status_code=404, detail="暂无数据，请先刷新")

    results = factor_engine.score_and_rank(
        stocks=stocks,
        weights=req.weights,
        filters=req.filters,
        sectors=req.sectors
    )
    return {
        "count": len(results),
        "results": results
    }


# ─── Backtest ────────────────────────────────────────────────────────────────
@app.post("/api/backtest")
def run_backtest(req: BacktestRequest):
    """运行回测：定期再平衡 → 计算收益 → 对比沪深300"""
    all_prices = db.get_all_prices()
    benchmark = db.get_benchmark_prices()
    stocks_data = db.get_all_stocks_with_data()

    if not all_prices or not stocks_data:
        raise HTTPException(status_code=404, detail="暂无数据，请先刷新")

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
        end_date=req.end_date
    )
    return results


# ─── Benchmark Data ──────────────────────────────────────────────────────────
@app.get("/api/benchmark")
def get_benchmark():
    """获取沪深300基准数据"""
    data = db.get_benchmark_prices()
    if not data:
        raise HTTPException(status_code=404, detail="暂无基准数据")
    return {"code": "000300.SH", "name": "沪深300", "prices": data}


# ─── Sectors ─────────────────────────────────────────────────────────────────
@app.get("/api/sectors")
def get_sectors():
    """获取所有行业/板块列表"""
    sectors = db.get_sectors()
    return {"sectors": sectors}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
