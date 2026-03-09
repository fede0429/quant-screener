"""
Portfolio advisor for automatic profile selection and daily signal reports.
"""
from datetime import datetime

from point_in_time import PointInTimeDataBuilder


class PortfolioAdvisor:
    def __init__(
        self,
        db,
        factor_engine,
        model_engine,
        portfolio_engine,
        learning_engine,
        portfolio_lab,
    ):
        self.db = db
        self.factor_engine = factor_engine
        self.model_engine = model_engine
        self.portfolio_engine = portfolio_engine
        self.learning_engine = learning_engine
        self.portfolio_lab = portfolio_lab

    def optimize_profile(
        self,
        optimize_for: str = "alpha_turnover",
        start_date: str | None = None,
        end_date: str | None = None,
        top_results: int = 10,
        max_combinations: int = 60,
        min_improvement: float = 0.25,
        force_activate: bool = False,
        name: str | None = None,
        weights: dict | None = None,
        filters: dict | None = None,
        sectors: list | None = None,
        grid: dict | None = None,
    ) -> dict:
        weights = weights or self._get_active_weights()
        filters = filters or {}
        sectors = sectors or []
        context = self._load_market_context()
        sweep = self.portfolio_lab.sweep(
            stocks_data=context["stocks_data"],
            all_prices=context["all_prices"],
            benchmark_prices=context["benchmark_prices"],
            factor_engine=self.factor_engine,
            weights=weights,
            filters=filters,
            sectors=sectors,
            point_in_time_builder=context["point_in_time_builder"],
            model_engine=self.model_engine,
            portfolio_engine=self.portfolio_engine,
            start_date=start_date,
            end_date=end_date,
            grid=grid,
            optimize_for=optimize_for,
            top_results=top_results,
            max_combinations=max_combinations,
        )
        best = sweep.get("best_config")
        if not best:
            raise RuntimeError("No valid portfolio configuration produced by sweep.")

        active_profile = self.get_active_profile()
        current_score = float((active_profile or {}).get("selection_score") or -1e9)
        best_score = float(best.get("score") or 0.0)
        activation_applied = force_activate or active_profile is None or best_score >= current_score + float(min_improvement)

        profile_id = f"portfolio_profile_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"
        profile_name = name or f"auto_{optimize_for}_{self.db.get_latest_price_date() or datetime.utcnow().strftime('%Y%m%d')}"
        config = {
            **(best.get("config") or {}),
            "weights": weights,
            "filters": filters,
            "sectors": sectors,
        }
        metrics = {
            **(best.get("metrics") or {}),
            "selection_score": round(best_score, 4),
            "optimize_for": optimize_for,
            "top_rank": best.get("rank"),
            "combination_count": sweep.get("combination_count"),
            "failed_count": sweep.get("failed_count"),
        }
        extra = {
            "top_results": sweep.get("top_results"),
            "grid": sweep.get("grid"),
            "selected_at": datetime.utcnow().isoformat(),
            "source_start_date": start_date,
            "source_end_date": end_date,
            "activation_applied": activation_applied,
            "previous_active_profile_id": (active_profile or {}).get("profile_id"),
            "min_improvement": float(min_improvement),
        }

        self.db.upsert_portfolio_profile(
            profile_id=profile_id,
            name=profile_name,
            optimize_for=optimize_for,
            config=config,
            metrics=metrics,
            extra=extra,
            is_active=activation_applied,
        )
        if activation_applied:
            self.db.activate_portfolio_profile(profile_id)

        saved = self.get_profile(profile_id)
        return {
            "profile": saved,
            "activation_applied": activation_applied,
            "active_profile": self.get_active_profile(),
            "sweep": sweep,
        }

    def run_signal(self, profile_id: str | None = None, persist: bool = True) -> dict:
        profile = self.get_profile(profile_id) if profile_id else self.get_active_profile()
        if not profile:
            raise RuntimeError("No active portfolio profile available yet.")

        config = profile.get("config_json") or {}
        weights = config.get("weights") or self._get_active_weights()
        filters = config.get("filters") or {}
        sectors = config.get("sectors") or []
        previous_report = self.get_latest_signal(profile_id=profile["profile_id"])
        current_holdings = self._build_current_holdings(previous_report)

        results, model_meta = self._build_live_ranked_results(
            weights=weights,
            filters=filters,
            sectors=sectors,
            use_model=bool(config.get("use_model", True)),
            model_horizon=int(config.get("model_horizon", 20)),
            model_weight=float(config.get("model_weight", 0.35)),
        )
        portfolio = self.portfolio_engine.construct(
            results=results,
            top_n=int(config.get("portfolio_top_n", 20)),
            neutralize_by=config.get("neutralize_by", "sector"),
            max_position_weight=float(config.get("max_position_weight", 0.05)),
            max_sector_weight=float(config.get("max_sector_weight", 0.25)),
            max_industry_weight=float(config.get("max_industry_weight", 0.15)),
            max_positions_per_sector=int(config.get("max_positions_per_sector", 4)),
            max_positions_per_industry=int(config.get("max_positions_per_industry", 2)),
            existing_holdings=current_holdings,
            rebalance_buffer=int(config.get("rebalance_buffer", 0)),
            max_new_positions=config.get("max_new_positions"),
            min_holding_periods=int(config.get("min_holding_periods", 0)),
        )
        holding_periods = self._next_holding_periods(previous_report, portfolio)
        registry = self.model_engine.get_serving_model_registry(horizon_days=int(config.get("model_horizon", 20)))
        signal_date = self.db.get_latest_price_date() or datetime.utcnow().strftime("%Y%m%d")
        score_preview = [
            {
                "rank": item.get("rank"),
                "code": item.get("code"),
                "name": item.get("name"),
                "final_score": item.get("final_score", item.get("composite_score")),
                "model_score": item.get("model_score"),
                "sector": item.get("sector"),
                "industry": item.get("industry"),
            }
            for item in results[:20]
        ]
        summary_md = self._build_signal_summary(
            signal_date=signal_date,
            profile=profile,
            portfolio=portfolio,
            model_meta=model_meta,
            registry=registry,
        )
        payload = {
            "signal_date": signal_date,
            "profile": profile,
            "model": registry,
            "model_runtime": model_meta,
            "active_weights": weights,
            "risk_policy": self.factor_engine.get_risk_policy(filters),
            "portfolio": portfolio,
            "holding_periods": holding_periods,
            "results_preview": score_preview,
            "summary_md": summary_md,
        }
        if persist:
            self.db.upsert_portfolio_signal_report(
                signal_date=signal_date,
                profile_id=profile["profile_id"],
                model_id=(registry or {}).get("model_id"),
                summary_md=summary_md,
                stats=payload,
            )
        return payload

    def get_profile(self, profile_id: str) -> dict | None:
        if not profile_id:
            return None
        row = self.db.get_portfolio_profile(profile_id)
        return self._enrich_profile(row) if row else None

    def get_active_profile(self) -> dict | None:
        row = self.db.get_active_portfolio_profile()
        return self._enrich_profile(row) if row else None

    def list_profiles(self, limit: int = 20, active_only: bool = False) -> dict:
        rows = self.db.get_portfolio_profiles(limit=limit, active_only=active_only)
        profiles = [self._enrich_profile(row) for row in rows]
        return {
            "count": len(profiles),
            "profiles": profiles,
        }

    def get_latest_signal(self, profile_id: str | None = None) -> dict | None:
        row = self.db.get_latest_portfolio_signal_report(profile_id=profile_id)
        return self._enrich_signal_report(row) if row else None

    def list_signals(self, limit: int = 30, profile_id: str | None = None) -> dict:
        rows = self.db.get_portfolio_signal_reports(limit=limit, profile_id=profile_id)
        reports = [self._enrich_signal_report(row) for row in rows]
        return {
            "count": len(reports),
            "reports": reports,
        }

    def _load_market_context(self) -> dict:
        all_prices = self.db.get_all_prices()
        benchmark_prices = self.db.get_benchmark_prices(limit=None)
        stocks_data = self.db.get_all_stocks_with_data()
        if not all_prices or not stocks_data:
            raise RuntimeError("No cached market data available. Refresh data first.")
        return {
            "all_prices": all_prices,
            "benchmark_prices": benchmark_prices,
            "stocks_data": stocks_data,
            "point_in_time_builder": PointInTimeDataBuilder(db=self.db, all_prices=all_prices),
        }

    def _build_live_ranked_results(self, weights: dict, filters: dict, sectors: list, use_model: bool, model_horizon: int, model_weight: float):
        stocks = self.db.get_all_stocks_with_data()
        if not stocks:
            raise RuntimeError("No cached stock data available. Refresh data first.")
        results = self.factor_engine.score_and_rank(
            stocks=stocks,
            weights=weights,
            filters=filters,
            sectors=sectors,
        )
        model_meta = {
            "applied": False,
            "reason": "Model blending disabled by profile.",
        }
        if use_model:
            blended = self.model_engine.blend_results(
                results=results,
                horizon_days=model_horizon,
                model_weight=model_weight,
            )
            results = blended.pop("results")
            model_meta = blended
        return results, model_meta

    def _build_current_holdings(self, previous_report: dict | None) -> list[dict]:
        if not previous_report:
            return []
        stats = previous_report.get("stats") or {}
        portfolio = stats.get("portfolio") or {}
        holding_periods = stats.get("holding_periods") or {}
        current_holdings = []
        for holding in portfolio.get("holdings", []):
            code = holding.get("code")
            if not code:
                continue
            current_holdings.append(
                {
                    "code": code,
                    "periods_held": int(holding_periods.get(code, 1)),
                }
            )
        return current_holdings

    def _next_holding_periods(self, previous_report: dict | None, portfolio: dict) -> dict:
        previous_stats = (previous_report or {}).get("stats") or {}
        previous_periods = previous_stats.get("holding_periods") or {}
        updated = {}
        for holding in portfolio.get("holdings", []):
            code = holding.get("code")
            if not code:
                continue
            updated[code] = int(previous_periods.get(code, 0)) + 1
        return updated

    def _build_signal_summary(self, signal_date: str, profile: dict, portfolio: dict, model_meta: dict, registry: dict | None) -> str:
        rebalance = portfolio.get("rebalance") or {}
        adds = ", ".join(rebalance.get("new_codes", [])[:10]) or "None"
        drops = ", ".join(rebalance.get("dropped_codes", [])[:10]) or "None"
        keeps = ", ".join(rebalance.get("kept_codes", [])[:10]) or "None"
        top_holdings = "\n".join(
            f"- #{item['portfolio_rank']} {item['code']} {item['name']} w={item['target_weight']}% score={item.get('final_score')}"
            for item in portfolio.get("holdings", [])[:10]
        ) or "- No holdings"
        return (
            f"# Daily Rebalance Report {signal_date}\n\n"
            f"- profile: {profile.get('name')} ({profile.get('profile_id')})\n"
            f"- optimize_for: {profile.get('optimize_for')}\n"
            f"- active model: {(registry or {}).get('model_id')}\n"
            f"- model applied: {model_meta.get('applied')}\n"
            f"- holdings: {portfolio.get('selected_count')}\n"
            f"- cash buffer: {portfolio.get('cash_buffer')}%\n"
            f"- kept: {rebalance.get('kept_count')}\n"
            f"- new: {rebalance.get('new_count')}\n"
            f"- dropped: {rebalance.get('dropped_count')}\n"
            f"- est name turnover: {rebalance.get('name_turnover')}%\n\n"
            f"## Actions\n"
            f"- Keep: {keeps}\n"
            f"- Add: {adds}\n"
            f"- Drop: {drops}\n\n"
            f"## Top Holdings\n"
            f"{top_holdings}\n"
        )

    def _enrich_profile(self, row: dict | None) -> dict | None:
        if not row:
            return None
        item = dict(row)
        item["config_json"] = self._parse_json(item.get("config_json")) or {}
        item["metrics_json"] = self._parse_json(item.get("metrics_json")) or {}
        item["extra_json"] = self._parse_json(item.get("extra_json")) or {}
        item["selection_score"] = item["metrics_json"].get("selection_score")
        return item

    def _enrich_signal_report(self, row: dict | None) -> dict | None:
        if not row:
            return None
        item = dict(row)
        item["stats"] = self._parse_json(item.get("stats_json")) or {}
        return item

    def _get_active_weights(self) -> dict:
        if self.learning_engine is not None:
            return self.learning_engine.get_active_weights()
        active_weights = self.db.get_setting_json("learning_active_weights")
        if active_weights:
            return active_weights
        raise RuntimeError("No active learning weights available.")

    @staticmethod
    def _parse_json(raw_value):
        if raw_value is None:
            return None
        if isinstance(raw_value, (dict, list)):
            return raw_value
        try:
            import json
            return json.loads(raw_value)
        except Exception:
            return raw_value
