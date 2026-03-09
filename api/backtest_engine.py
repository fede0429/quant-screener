"""
Backtest engine with point-in-time stock selection and portfolio simulation.
"""
import logging

from factor_engine import FactorEngine

logger = logging.getLogger(__name__)


class BacktestEngine:
    def run(
        self,
        stocks_data: list[dict],
        all_prices: dict,
        benchmark_prices: list[dict],
        factor_engine: FactorEngine,
        weights: dict,
        filters: dict,
        sectors: list,
        frequency: str = "monthly",
        top_n: int = 10,
        start_date: str = None,
        end_date: str = None,
        point_in_time_builder=None,
        model_engine=None,
        portfolio_engine=None,
        use_model: bool = False,
        model_horizon: int = 20,
        model_weight: float = 0.35,
        build_portfolio: bool = False,
        portfolio_top_n: int | None = None,
        neutralize_by: str = "sector",
        max_position_weight: float = 0.05,
        max_sector_weight: float = 0.25,
        max_industry_weight: float = 0.15,
        max_positions_per_sector: int = 4,
        max_positions_per_industry: int = 2,
        transaction_cost_bps: float = 10.0,
        rebalance_buffer: int = 0,
        max_new_positions: int | None = None,
        min_holding_periods: int = 0,
    ) -> dict:
        all_dates = set()
        for prices in all_prices.values():
            for price in prices:
                all_dates.add(price["trade_date"])
        trade_dates = sorted(all_dates)

        if not trade_dates:
            return {"error": "No trading dates available"}

        if start_date:
            trade_dates = [d for d in trade_dates if d >= start_date.replace("-", "")]
        if end_date:
            trade_dates = [d for d in trade_dates if d <= end_date.replace("-", "")]

        if len(trade_dates) < 20:
            return {"error": "Not enough trading dates for backtest"}

        rebalance_dates = self._get_rebalance_dates(trade_dates, frequency)
        if len(rebalance_dates) < 2:
            return {"error": "Not enough rebalance periods"}

        bench_map = {row["trade_date"]: row["close"] for row in benchmark_prices if row.get("close")}
        price_map = {
            code: {row["trade_date"]: row["close"] for row in prices if row.get("close")}
            for code, prices in all_prices.items()
        }

        portfolio_top_n = int(portfolio_top_n or top_n)
        transaction_cost_rate = max(0.0, float(transaction_cost_bps)) / 10000.0
        net_portfolio_value = 1.0
        gross_portfolio_value = 1.0
        benchmark_value = 1.0
        equity_curve = []
        holdings_history = []
        net_period_returns = []
        gross_period_returns = []
        prev_weights = {"__CASH__": 1.0}
        total_turnover = 0.0
        total_transaction_cost = 0.0
        previous_portfolio = None
        holding_periods = {}

        for idx in range(len(rebalance_dates) - 1):
            rb_date = rebalance_dates[idx]
            next_rb_date = rebalance_dates[idx + 1]

            ranked_results, model_meta = self._rank_results_at_date(
                stocks_data=stocks_data,
                price_map=price_map,
                trade_date=rb_date,
                factor_engine=factor_engine,
                weights=weights,
                filters=filters,
                sectors=sectors,
                point_in_time_builder=point_in_time_builder,
                model_engine=model_engine,
                use_model=use_model,
                model_horizon=model_horizon,
                model_weight=model_weight,
            )
            portfolio = self._build_target_portfolio(
                ranked_results=ranked_results,
                portfolio_engine=portfolio_engine,
                build_portfolio=build_portfolio,
                portfolio_top_n=portfolio_top_n,
                neutralize_by=neutralize_by,
                max_position_weight=max_position_weight,
                max_sector_weight=max_sector_weight,
                max_industry_weight=max_industry_weight,
                max_positions_per_sector=max_positions_per_sector,
                max_positions_per_industry=max_positions_per_industry,
                existing_holdings=(previous_portfolio or {}).get("holdings", []),
                holding_periods_by_code=holding_periods,
                rebalance_buffer=rebalance_buffer,
                max_new_positions=max_new_positions,
                min_holding_periods=min_holding_periods,
            )

            target_weights = self._portfolio_weight_map(portfolio)
            turnover = self._compute_turnover(prev_weights, target_weights)
            transaction_cost = net_portfolio_value * turnover * transaction_cost_rate
            total_turnover += turnover
            total_transaction_cost += transaction_cost

            net_after_cost = max(net_portfolio_value - transaction_cost, 0.0)
            gross_multiplier, realized_holdings, exit_weights = self._simulate_period(
                portfolio=portfolio,
                price_map=price_map,
                entry_date=rb_date,
                exit_date=next_rb_date,
            )

            gross_before = gross_portfolio_value
            net_before = net_portfolio_value
            gross_portfolio_value *= gross_multiplier
            net_portfolio_value = net_after_cost * gross_multiplier
            gross_period_return = gross_multiplier - 1.0
            net_period_return = (net_portfolio_value / net_before - 1.0) if net_before > 0 else 0.0
            gross_period_returns.append(gross_period_return)
            net_period_returns.append(net_period_return)

            bench_entry = bench_map.get(rb_date)
            bench_exit = bench_map.get(next_rb_date)
            bench_period_return = 0.0
            if bench_entry and bench_exit and bench_entry > 0:
                bench_period_return = bench_exit / bench_entry - 1.0
                benchmark_value *= 1.0 + bench_period_return

            equity_curve.append(
                {
                    "date": self._format_date(next_rb_date),
                    "portfolio": round(net_portfolio_value, 4),
                    "gross_portfolio": round(gross_portfolio_value, 4),
                    "benchmark": round(benchmark_value, 4),
                    "turnover": round(turnover * 100.0, 2),
                    "transaction_cost": round(transaction_cost * 100.0, 4),
                }
            )
            holdings_history.append(
                {
                    "date": self._format_date(rb_date),
                    "next_rebalance_date": self._format_date(next_rb_date),
                    "portfolio": portfolio,
                    "holdings": realized_holdings,
                    "turnover": round(turnover * 100.0, 2),
                    "transaction_cost": round(transaction_cost * 100.0, 4),
                    "gross_return": round(gross_period_return * 100.0, 2),
                    "net_return": round(net_period_return * 100.0, 2),
                    "benchmark_return": round(bench_period_return * 100.0, 2),
                    "model": model_meta,
                    "portfolio_value_before": round(net_before, 4),
                    "portfolio_value_after": round(net_portfolio_value, 4),
                    "gross_portfolio_before": round(gross_before, 4),
                    "gross_portfolio_after": round(gross_portfolio_value, 4),
                }
            )
            prev_weights = exit_weights
            previous_portfolio = portfolio
            holding_periods = self._next_holding_periods(holding_periods, portfolio)

        metrics = self._compute_metrics(
            equity_curve=equity_curve,
            period_returns=net_period_returns,
            gross_period_returns=gross_period_returns,
            frequency=frequency,
            total_turnover=total_turnover,
            total_transaction_cost=total_transaction_cost,
            holdings_history=holdings_history,
        )
        monthly_returns = self._compute_monthly_returns(equity_curve, field_name="portfolio")
        gross_monthly_returns = self._compute_monthly_returns(equity_curve, field_name="gross_portfolio")

        return {
            "equity_curve": equity_curve,
            "metrics": metrics,
            "holdings_history": holdings_history,
            "monthly_returns": monthly_returns,
            "gross_monthly_returns": gross_monthly_returns,
            "config": {
                "frequency": frequency,
                "top_n": top_n,
                "portfolio_top_n": portfolio_top_n,
                "periods": len(rebalance_dates) - 1,
                "start_date": self._format_date(trade_dates[0]),
                "end_date": self._format_date(trade_dates[-1]),
                "point_in_time_mode": point_in_time_builder is not None,
                "model_blending": use_model,
                "model_horizon": model_horizon,
                "model_weight": model_weight,
                "portfolio_mode": build_portfolio,
                "neutralize_by": neutralize_by if build_portfolio else "none",
                "transaction_cost_bps": round(float(transaction_cost_bps), 2),
                "rebalance_buffer": int(rebalance_buffer),
                "max_new_positions": max_new_positions,
                "min_holding_periods": int(min_holding_periods),
                "constraints": {
                    "max_position_weight": round(float(max_position_weight) * 100.0, 2),
                    "max_sector_weight": round(float(max_sector_weight) * 100.0, 2),
                    "max_industry_weight": round(float(max_industry_weight) * 100.0, 2),
                    "max_positions_per_sector": int(max_positions_per_sector),
                    "max_positions_per_industry": int(max_positions_per_industry),
                },
            },
        }

    def _get_rebalance_dates(self, trade_dates: list[str], frequency: str) -> list[str]:
        rebalance = []
        last_period = None
        for trade_date in trade_dates:
            year = int(trade_date[:4])
            month = int(trade_date[4:6])
            if frequency == "weekly":
                period = self._weekly_period(trade_date)
            elif frequency == "quarterly":
                period = (year, (month - 1) // 3)
            else:
                period = (year, month)

            if period != last_period:
                rebalance.append(trade_date)
                last_period = period
        return rebalance

    def _rank_results_at_date(
        self,
        stocks_data,
        price_map,
        trade_date,
        factor_engine,
        weights,
        filters,
        sectors,
        point_in_time_builder=None,
        model_engine=None,
        use_model: bool = False,
        model_horizon: int = 20,
        model_weight: float = 0.35,
    ) -> tuple[list[dict], dict]:
        if point_in_time_builder is not None:
            available = point_in_time_builder.build_universe(
                snapshot_date=trade_date,
                include_indicators=True,
                min_price_history=20,
                price_limit=250,
            )
        else:
            available = []
            for stock in stocks_data:
                code = stock["code"]
                if code in price_map and trade_date in price_map[code]:
                    available.append(stock)

        if not available:
            return [], {"applied": False, "reason": "No eligible stocks at rebalance date."}

        results = factor_engine.score_and_rank(
            stocks=available,
            weights=weights,
            filters=filters,
            sectors=sectors,
        )
        model_meta = {
            "applied": False,
            "reason": "Model blending disabled by request.",
        }
        if use_model and model_engine is not None:
            blended = model_engine.blend_results(
                results=results,
                horizon_days=model_horizon,
                model_weight=model_weight,
            )
            results = blended.pop("results")
            model_meta = blended
        return results, model_meta

    def _build_target_portfolio(
        self,
        ranked_results: list[dict],
        portfolio_engine,
        build_portfolio: bool,
        portfolio_top_n: int,
        neutralize_by: str,
        max_position_weight: float,
        max_sector_weight: float,
        max_industry_weight: float,
        max_positions_per_sector: int,
        max_positions_per_industry: int,
        existing_holdings: list[dict] | list[str] | None,
        holding_periods_by_code: dict | None,
        rebalance_buffer: int,
        max_new_positions: int | None,
        min_holding_periods: int,
    ) -> dict:
        if not ranked_results:
            return {
                "enabled": build_portfolio,
                "selected_count": 0,
                "cash_buffer": 100.0,
                "holdings": [],
                "sector_exposure": [],
                "industry_exposure": [],
                "constraints": {},
                "rebalance": {
                    "enabled": bool(existing_holdings),
                    "previous_count": len(existing_holdings or []),
                    "kept_count": 0,
                    "new_count": 0,
                    "dropped_count": len(existing_holdings or []),
                    "name_turnover": 100.0 if existing_holdings else 0.0,
                },
            }

        if build_portfolio and portfolio_engine is not None:
            return portfolio_engine.construct(
                results=ranked_results,
                top_n=portfolio_top_n,
                neutralize_by=neutralize_by,
                max_position_weight=max_position_weight,
                max_sector_weight=max_sector_weight,
                max_industry_weight=max_industry_weight,
                max_positions_per_sector=max_positions_per_sector,
                max_positions_per_industry=max_positions_per_industry,
                existing_holdings=existing_holdings,
                holding_periods_by_code=holding_periods_by_code,
                rebalance_buffer=rebalance_buffer,
                max_new_positions=max_new_positions,
                min_holding_periods=min_holding_periods,
            )

        selected = ranked_results[:portfolio_top_n]
        if not selected:
            return {
                "enabled": False,
                "selected_count": 0,
                "cash_buffer": 100.0,
                "holdings": [],
                "sector_exposure": [],
                "industry_exposure": [],
                "constraints": {"top_n": portfolio_top_n, "neutralize_by": "none"},
                "rebalance": {"enabled": False},
            }

        target_weight = round(100.0 / len(selected), 2)
        holdings = []
        for idx, item in enumerate(selected, 1):
            holdings.append(
                {
                    "portfolio_rank": idx,
                    "code": item.get("code"),
                    "name": item.get("name"),
                    "sector": item.get("sector"),
                    "industry": item.get("industry"),
                    "rank": item.get("rank"),
                    "composite_score": item.get("composite_score"),
                    "model_score": item.get("model_score"),
                    "final_score": item.get("final_score", item.get("composite_score")),
                    "target_weight": target_weight,
                }
            )
        return {
            "enabled": False,
            "selected_count": len(holdings),
            "requested_top_n": portfolio_top_n,
            "neutralize_by": "none",
            "target_position_weight": target_weight,
            "cash_buffer": round(max(0.0, 100.0 - target_weight * len(holdings)), 2),
            "holdings": holdings,
            "sector_exposure": self._build_exposure(holdings, "sector"),
            "industry_exposure": self._build_exposure(holdings, "industry"),
            "constraints": {
                "top_n": portfolio_top_n,
                "neutralize_by": "none",
                "target_weight": target_weight,
            },
            "rebalance": {"enabled": False},
        }

    def _simulate_period(self, portfolio: dict, price_map: dict, entry_date: str, exit_date: str):
        cash_weight = max(0.0, float(portfolio.get("cash_buffer", 0.0)) / 100.0)
        gross_multiplier = cash_weight
        exit_weights = {}
        realized_holdings = []

        for holding in portfolio.get("holdings", []):
            code = holding.get("code")
            target_weight = max(0.0, float(holding.get("target_weight", 0.0)) / 100.0)
            entry_price = price_map.get(code, {}).get(entry_date)
            exit_price = price_map.get(code, {}).get(exit_date)
            stock_return = 0.0
            if entry_price and exit_price and entry_price > 0:
                stock_return = exit_price / entry_price - 1.0
            contribution = target_weight * (1.0 + stock_return)
            gross_multiplier += contribution
            realized_holdings.append(
                {
                    **holding,
                    "entry_date": self._format_date(entry_date),
                    "exit_date": self._format_date(exit_date),
                    "entry_price": round(float(entry_price), 4) if entry_price else None,
                    "exit_price": round(float(exit_price), 4) if exit_price else None,
                    "return": round(stock_return * 100.0, 2),
                    "weight_contribution": round(contribution * 100.0, 4),
                }
            )

        gross_multiplier = max(gross_multiplier, 1e-9)
        for holding in realized_holdings:
            target_weight = float(holding.get("target_weight", 0.0)) / 100.0
            stock_return = float(holding.get("return", 0.0)) / 100.0
            exit_weights[holding["code"]] = max(0.0, target_weight * (1.0 + stock_return) / gross_multiplier)
        if cash_weight > 0:
            exit_weights["__CASH__"] = cash_weight / gross_multiplier

        return gross_multiplier, realized_holdings, exit_weights

    @staticmethod
    def _portfolio_weight_map(portfolio: dict) -> dict:
        weights = {"__CASH__": max(0.0, float(portfolio.get("cash_buffer", 0.0)) / 100.0)}
        for holding in portfolio.get("holdings", []):
            code = holding.get("code")
            if not code:
                continue
            weights[code] = max(0.0, float(holding.get("target_weight", 0.0)) / 100.0)
        return weights

    @staticmethod
    def _compute_turnover(previous_weights: dict, target_weights: dict) -> float:
        keys = set(previous_weights) | set(target_weights)
        total = sum(abs(float(target_weights.get(key, 0.0)) - float(previous_weights.get(key, 0.0))) for key in keys)
        return total / 2.0

    def _compute_metrics(
        self,
        equity_curve,
        period_returns,
        gross_period_returns,
        frequency,
        total_turnover,
        total_transaction_cost,
        holdings_history,
    ) -> dict:
        if not equity_curve or not period_returns:
            return {}

        final_value = equity_curve[-1]["portfolio"]
        gross_final_value = equity_curve[-1].get("gross_portfolio", final_value)
        bench_final = equity_curve[-1].get("benchmark", 1)
        total_return = (final_value - 1.0) * 100.0
        gross_total_return = (gross_final_value - 1.0) * 100.0

        periods_per_year = 52 if frequency == "weekly" else (4 if frequency == "quarterly" else 12)
        years = len(period_returns) / periods_per_year if periods_per_year else 1
        annualized = ((final_value ** (1.0 / years)) - 1.0) * 100.0 if years > 0 else 0.0
        gross_annualized = ((gross_final_value ** (1.0 / years)) - 1.0) * 100.0 if years > 0 else 0.0

        max_drawdown = 0.0
        peak = 1.0
        for point in equity_curve:
            value = point["portfolio"]
            if value > peak:
                peak = value
            drawdown = (peak - value) / peak * 100.0
            if drawdown > max_drawdown:
                max_drawdown = drawdown

        rf = 0.025 / periods_per_year
        excess = [value - rf for value in period_returns]
        mean_excess = sum(excess) / len(excess) if excess else 0.0
        if len(excess) > 1:
            variance = sum((value - mean_excess) ** 2 for value in excess) / (len(excess) - 1)
            std = variance ** 0.5
        else:
            std = 1.0
        sharpe = (mean_excess / std * (periods_per_year ** 0.5)) if std > 0 else 0.0

        wins = sum(1 for value in period_returns if value > 0)
        win_rate = wins / len(period_returns) * 100.0 if period_returns else 0.0
        benchmark_return = (bench_final - 1.0) * 100.0 if bench_final else 0.0
        alpha = total_return - benchmark_return
        avg_turnover = total_turnover / len(period_returns) * 100.0 if period_returns else 0.0
        average_positions = (
            sum(period.get("portfolio", {}).get("selected_count", 0) for period in holdings_history) / len(holdings_history)
            if holdings_history
            else 0.0
        )
        average_name_turnover = (
            sum(float((period.get("portfolio") or {}).get("rebalance", {}).get("name_turnover") or 0.0) for period in holdings_history) / len(holdings_history)
            if holdings_history
            else 0.0
        )
        average_new_positions = (
            sum(int((period.get("portfolio") or {}).get("rebalance", {}).get("new_count") or 0) for period in holdings_history) / len(holdings_history)
            if holdings_history
            else 0.0
        )

        return {
            "total_return": round(total_return, 2),
            "gross_total_return": round(gross_total_return, 2),
            "annualized_return": round(annualized, 2),
            "gross_annualized_return": round(gross_annualized, 2),
            "max_drawdown": round(max_drawdown, 2),
            "sharpe_ratio": round(sharpe, 2),
            "win_rate": round(win_rate, 1),
            "alpha": round(alpha, 2),
            "benchmark_return": round(benchmark_return, 2),
            "periods": len(period_returns),
            "years": round(years, 1),
            "avg_turnover": round(avg_turnover, 2),
            "total_turnover": round(total_turnover * 100.0, 2),
            "transaction_cost": round(total_transaction_cost * 100.0, 4),
            "average_positions": round(average_positions, 2),
            "average_name_turnover": round(average_name_turnover, 2),
            "average_new_positions": round(average_new_positions, 2),
        }

    def _compute_monthly_returns(self, equity_curve, field_name: str = "portfolio") -> list[dict]:
        if len(equity_curve) < 2:
            return []

        monthly = {}
        for point in equity_curve:
            month = point["date"][:7]
            monthly[month] = point[field_name]

        result = []
        prev = 1.0
        for month in sorted(monthly.keys()):
            value = monthly[month]
            monthly_return = (value / prev - 1.0) * 100.0 if prev > 0 else 0.0
            result.append({"month": month, "return": round(monthly_return, 2)})
            prev = value
        return result

    def _next_holding_periods(self, current_holding_periods: dict, portfolio: dict) -> dict:
        next_periods = {}
        for holding in portfolio.get("holdings", []):
            code = holding.get("code")
            if not code:
                continue
            next_periods[code] = int(current_holding_periods.get(code, 0)) + 1
        return next_periods

    @staticmethod
    def _build_exposure(holdings: list[dict], field: str) -> list[dict]:
        grouped = {}
        for holding in holdings:
            key = holding.get(field) or "Unclassified"
            if key not in grouped:
                grouped[key] = {"positions": 0, "weight": 0.0}
            grouped[key]["positions"] += 1
            grouped[key]["weight"] += float(holding.get("target_weight", 0.0))
        exposure = [{field: key, "positions": value["positions"], "weight": round(value["weight"], 2)} for key, value in grouped.items()]
        exposure.sort(key=lambda item: (-item["weight"], item[field]))
        return exposure

    @staticmethod
    def _weekly_period(trade_date: str):
        year = int(trade_date[:4])
        month = int(trade_date[4:6])
        day = int(trade_date[6:8])
        ordinal = (month - 1) * 31 + day
        return year, ordinal // 7

    @staticmethod
    def _format_date(trade_date: str) -> str:
        if len(trade_date) == 8 and trade_date.isdigit():
            return f"{trade_date[:4]}-{trade_date[4:6]}-{trade_date[6:]}"
        return trade_date
