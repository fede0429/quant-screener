"""
回测引擎 — 定期再平衡策略回测
月度/季度再平衡 → 等权配置 Top N → 计算收益 → 对比沪深300
"""
import math
import logging
from datetime import datetime, timedelta
from factor_engine import FactorEngine

logger = logging.getLogger(__name__)


class BacktestEngine:

    def run(
        self,
        stocks_data: list[dict],
        all_prices: dict,  # {code: [price_rows]}
        benchmark_prices: list[dict],
        factor_engine: FactorEngine,
        weights: dict,
        filters: dict,
        sectors: list,
        frequency: str = "monthly",
        top_n: int = 10,
        start_date: str = None,
        end_date: str = None,
    ) -> dict:
        """
        运行回测
        Returns: 包含净値曲线、绩效指标、持仓记录的完整结果
        """
        # 1. 建立交易日列表
        all_dates = set()
        for code, prices in all_prices.items():
            for p in prices:
                all_dates.add(p["trade_date"])
        trade_dates = sorted(all_dates)

        if not trade_dates:
            return {"error": "无可用交易日"}

        # 应用日期范围
        if start_date:
            sd = start_date.replace("-", "")
            trade_dates = [d for d in trade_dates if d >= sd]
        if end_date:
            ed = end_date.replace("-", "")
            trade_dates = [d for d in trade_dates if d <= ed]

        if len(trade_dates) < 20:
            return {"error": "交易日不足（至少需耆20个交易日）"}

        # 2. 确定再平衡日期
        rebalance_dates = self._get_rebalance_dates(trade_dates, frequency)
        if len(rebalance_dates) < 2:
            return {"error": "再平衡周期不足"}

        # 3. 建立基准价格查找表
        bench_map = {}
        if benchmark_prices:
            for p in benchmark_prices:
                bench_map[p["trade_date"]] = p["close"]

        # 4. 建立股票价格查找表
        price_map = {}  # {code: {date: close}}
        for code, prices in all_prices.items():
            price_map[code] = {p["trade_date"]: p["close"] for p in prices if p.get("close")}

        # 5. 回测主循环
        portfolio_value = 1.0
        benchmark_value = 1.0
        equity_curve = []
        holdings_history = []
        period_returns = []

        bench_start = bench_map.get(rebalance_dates[0])

        for i in range(len(rebalance_dates) - 1):
            rb_date = rebalance_dates[i]
            next_rb_date = rebalance_dates[i + 1]

            # 在每个再平衡日运行选股（使用截至该日的数据）
            # 简化：使用当前数据做选股（实际回测应用时间点数据）
            selected = self._select_stocks_at_date(
                stocks_data, price_map, rb_date,
                factor_engine, weights, filters, sectors, top_n
            )

            if not selected:
                # 无法选出股票，保持现金
                equity_curve.append({
                    "date": self._format_date(next_rb_date),
                    "portfolio": round(portfolio_value, 4),
                    "benchmark": round(benchmark_value, 4) if bench_start else None,
                })
                continue

            # 计算持仓期收益
            period_return = 0
            stock_returns = []
            for code in selected:
                p_start = price_map.get(code, {}).get(rb_date)
                p_end = price_map.get(code, {}).get(next_rb_date)
                if p_start and p_end and p_start > 0:
                    ret = (p_end / p_start - 1)
                    stock_returns.append(ret)
                else:
                    stock_returns.append(0)

            if stock_returns:
                period_return = sum(stock_returns) / len(stock_returns)  # 等权

            portfolio_value *= (1 + period_return)
            period_returns.append(period_return)

            # 基准收益
            b_start = bench_map.get(rb_date)
            b_end = bench_map.get(next_rb_date)
            if b_start and b_end and bench_start and bench_start > 0:
                benchmark_value = b_end / bench_start

            equity_curve.append({
                "date": self._format_date(next_rb_date),
                "portfolio": round(portfolio_value, 4),
                "benchmark": round(benchmark_value, 4),
            })

            holdings_history.append({
                "date": self._format_date(rb_date),
                "stocks": selected[:top_n],
                "return": round(period_return * 100, 2),
            })

        # 6. 计算绩效指标
        metrics = self._compute_metrics(
            equity_curve, period_returns, frequency
        )

        # 7. 月度收益热力图
        monthly_returns = self._compute_monthly_returns(equity_curve)

        return {
            "equity_curve": equity_curve,
            "metrics": metrics,
            "holdings_history": holdings_history,
            "monthly_returns": monthly_returns,
            "config": {
                "frequency": frequency,
                "top_n": top_n,
                "periods": len(rebalance_dates) - 1,
                "start_date": self._format_date(trade_dates[0]),
                "end_date": self._format_date(trade_dates[-1]),
            }
        }

    # ─── 再平衡日期 ─────────────────────────────────────────────────────
    def _get_rebalance_dates(self, trade_dates: list, frequency: str) -> list:
        """从交易日中提取月初/季初的再平衡日"""
        rebalance = []
        last_period = None

        for d in trade_dates:
            year = int(d[:4])
            month = int(d[4:6])

            if frequency == "monthly":
                period = (year, month)
            else:  # quarterly
                period = (year, (month - 1) // 3)

            if period != last_period:
                rebalance.append(d)
                last_period = period

        return rebalance

    # ─── 选股（某个时间点）──────────────────────────────────────────────────
    def _select_stocks_at_date(
        self, stocks_data, price_map, date,
        factor_engine, weights, filters, sectors, top_n
    ) -> list:
        """在指定日期做选股（简化版：使用现有因子数据）"""
        # 过滤出在该日期有价格的股票
        available = []
        for stock in stocks_data:
            code = stock["code"]
            if code in price_map and date in price_map[code]:
                available.append(stock)

        if not available:
            return []

        results = factor_engine.score_and_rank(
            stocks=available,
            weights=weights,
            filters=filters,
            sectors=sectors
        )

        return [r["code"] for r in results[:top_n]]

    # ─── 绩效指标计算 ──────────────────────────────────────────────────
    def _compute_metrics(self, equity_curve, period_returns, frequency) -> dict:
        if not equity_curve or not period_returns:
            return {}

        final_value = equity_curve[-1]["portfolio"]
        bench_final = equity_curve[-1].get("benchmark", 1)
        total_return = (final_value - 1) * 100

        # 年化收益率
        n_periods = len(period_returns)
        periods_per_year = 12 if frequency == "monthly" else 4
        years = n_periods / periods_per_year if periods_per_year else 1
        annualized = ((final_value) ** (1 / years) - 1) * 100 if years > 0 else 0

        # 最大回撤
        max_dd = 0
        peak = 1
        for point in equity_curve:
            v = point["portfolio"]
            if v > peak:
                peak = v
            dd = (peak - v) / peak * 100
            if dd > max_dd:
                max_dd = dd

        # 夏普比率 (假设无風险利率 2.5%)
        rf = 0.025 / periods_per_year
        excess = [r - rf for r in period_returns]
        mean_excess = sum(excess) / len(excess) if excess else 0
        if len(excess) > 1:
            std = (sum((x - mean_excess) ** 2 for x in excess) / (len(excess) - 1)) ** 0.5
        else:
            std = 1
        sharpe = (mean_excess / std * (periods_per_year ** 0.5)) if std > 0 else 0

        # 胜率
        wins = sum(1 for r in period_returns if r > 0)
        win_rate = wins / len(period_returns) * 100 if period_returns else 0

        # 超额收益
        bench_return = (bench_final - 1) * 100 if bench_final else 0
        alpha = total_return - bench_return

        return {
            "total_return": round(total_return, 2),
            "annualized_return": round(annualized, 2),
            "max_drawdown": round(max_dd, 2),
            "sharpe_ratio": round(sharpe, 2),
            "win_rate": round(win_rate, 1),
            "alpha": round(alpha, 2),
            "benchmark_return": round(bench_return, 2),
            "periods": n_periods,
            "years": round(years, 1),
        }

    # ─── 月度收益热力图 ────────────────────────────────────────────────
    def _compute_monthly_returns(self, equity_curve) -> list:
        """按月聚合收益率"""
        if len(equity_curve) < 2:
            return []

        monthly = {}
        prev_value = 1.0

        for point in equity_curve:
            d = point["date"]  # YYYY-MM-DD
            ym = d[:7]  # YYYY-MM
            v = point["portfolio"]
            monthly[ym] = v

        result = []
        prev = 1.0
        for ym in sorted(monthly.keys()):
            v = monthly[ym]
            ret = (v / prev - 1) * 100 if prev > 0 else 0
            result.append({
                "month": ym,
                "return": round(ret, 2)
            })
            prev = v

        return result

    # ─── 日期格式化 ─────────────────────────────────────────────────────
    @staticmethod
    def _format_date(d: str) -> str:
        """20240101 → 2024-01-01"""
        if len(d) == 8 and d.isdigit():
            return f"{d[:4]}-{d[4:6]}-{d[6:]}"
        return d
