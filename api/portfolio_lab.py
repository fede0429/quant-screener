"""
Portfolio research utilities for parameter sweeps.
"""
import itertools


class PortfolioLab:
    def __init__(self, backtest_engine):
        self.backtest_engine = backtest_engine

    def sweep(
        self,
        stocks_data: list[dict],
        all_prices: dict,
        benchmark_prices: list[dict],
        factor_engine,
        weights: dict,
        filters: dict,
        sectors: list,
        point_in_time_builder=None,
        model_engine=None,
        portfolio_engine=None,
        start_date: str | None = None,
        end_date: str | None = None,
        grid: dict | None = None,
        optimize_for: str = "alpha_turnover",
        top_results: int = 10,
        max_combinations: int = 80,
    ) -> dict:
        grid = grid or {}
        combinations = self._build_combinations(grid)
        if not combinations:
            raise RuntimeError("Parameter grid produced no combinations.")
        if len(combinations) > max_combinations:
            raise RuntimeError(
                f"Too many parameter combinations: {len(combinations)} > {max_combinations}"
            )

        scored = []
        failed = 0
        for idx, combo in enumerate(combinations, 1):
            result = self.backtest_engine.run(
                stocks_data=stocks_data,
                all_prices=all_prices,
                benchmark_prices=benchmark_prices,
                factor_engine=factor_engine,
                weights=weights,
                filters=filters,
                sectors=sectors,
                frequency=combo["frequency"],
                top_n=combo["top_n"],
                start_date=start_date,
                end_date=end_date,
                point_in_time_builder=point_in_time_builder,
                model_engine=model_engine,
                portfolio_engine=portfolio_engine,
                use_model=combo["use_model"],
                model_horizon=combo["model_horizon"],
                model_weight=combo["model_weight"],
                build_portfolio=True,
                portfolio_top_n=combo["portfolio_top_n"],
                neutralize_by=combo["neutralize_by"],
                max_position_weight=combo["max_position_weight"],
                max_sector_weight=combo["max_sector_weight"],
                max_industry_weight=combo["max_industry_weight"],
                max_positions_per_sector=combo["max_positions_per_sector"],
                max_positions_per_industry=combo["max_positions_per_industry"],
                transaction_cost_bps=combo["transaction_cost_bps"],
                rebalance_buffer=combo["rebalance_buffer"],
                max_new_positions=combo["max_new_positions"],
                min_holding_periods=combo["min_holding_periods"],
            )
            metrics = result.get("metrics") or {}
            error = result.get("error")
            if error:
                failed += 1
            score = self._score(metrics, optimize_for) if not error else -1e9
            scored.append(
                {
                    "config_id": f"cfg_{idx:03d}",
                    "score": round(score, 4),
                    "optimize_for": optimize_for,
                    "config": combo,
                    "metrics": metrics,
                    "error": error,
                    "periods": result.get("config", {}).get("periods"),
                    "point_in_time_mode": result.get("config", {}).get("point_in_time_mode"),
                    "portfolio_mode": result.get("config", {}).get("portfolio_mode"),
                }
            )

        scored.sort(key=lambda item: (item["error"] is not None, -item["score"]))
        for rank, item in enumerate(scored, 1):
            item["rank"] = rank

        best = next((item for item in scored if not item.get("error")), None)
        return {
            "optimize_for": optimize_for,
            "combination_count": len(combinations),
            "failed_count": failed,
            "top_results": scored[:top_results],
            "best_config": best,
            "grid": grid,
        }

    def _build_combinations(self, grid: dict) -> list[dict]:
        normalized = {
            "frequency": self._unique_list(grid.get("frequency"), ["monthly"]),
            "top_n": self._unique_list(grid.get("top_n"), [40]),
            "portfolio_top_n": self._unique_list(grid.get("portfolio_top_n"), [20]),
            "use_model": self._unique_list(grid.get("use_model"), [True]),
            "model_horizon": self._unique_list(grid.get("model_horizon"), [20]),
            "model_weight": self._unique_list(grid.get("model_weight"), [0.35]),
            "neutralize_by": self._unique_list(grid.get("neutralize_by"), ["sector"]),
            "max_position_weight": self._unique_list(grid.get("max_position_weight"), [0.05]),
            "max_sector_weight": self._unique_list(grid.get("max_sector_weight"), [0.25]),
            "max_industry_weight": self._unique_list(grid.get("max_industry_weight"), [0.15]),
            "max_positions_per_sector": self._unique_list(grid.get("max_positions_per_sector"), [4]),
            "max_positions_per_industry": self._unique_list(grid.get("max_positions_per_industry"), [2]),
            "transaction_cost_bps": self._unique_list(grid.get("transaction_cost_bps"), [10.0]),
            "rebalance_buffer": self._unique_list(grid.get("rebalance_buffer"), [0]),
            "max_new_positions": self._unique_list(grid.get("max_new_positions"), [None]),
            "min_holding_periods": self._unique_list(grid.get("min_holding_periods"), [0]),
        }
        keys = list(normalized.keys())
        combos = []
        for values in itertools.product(*(normalized[key] for key in keys)):
            combo = dict(zip(keys, values))
            if combo["portfolio_top_n"] > combo["top_n"]:
                continue
            combos.append(combo)
        return combos

    @staticmethod
    def _unique_list(values, default):
        source = values if values is not None else default
        result = []
        for value in source:
            normalized = None if value in (-1, "-1", "none", "None") else value
            if normalized not in result:
                result.append(normalized)
        return result or list(default)

    @staticmethod
    def _score(metrics: dict, optimize_for: str) -> float:
        optimize_for = (optimize_for or "alpha_turnover").strip().lower()
        alpha = float(metrics.get("alpha", 0.0) or 0.0)
        total_return = float(metrics.get("total_return", 0.0) or 0.0)
        sharpe = float(metrics.get("sharpe_ratio", 0.0) or 0.0)
        drawdown = float(metrics.get("max_drawdown", 0.0) or 0.0)
        turnover = float(metrics.get("avg_turnover", 0.0) or 0.0)
        transaction_cost = float(metrics.get("transaction_cost", 0.0) or 0.0)

        if optimize_for == "alpha":
            return alpha
        if optimize_for == "total_return":
            return total_return
        if optimize_for == "sharpe":
            return sharpe
        if optimize_for == "alpha_drawdown":
            return alpha - 0.5 * drawdown
        if optimize_for == "net_efficiency":
            return total_return - 0.15 * turnover - transaction_cost
        return alpha - 0.15 * turnover - transaction_cost
