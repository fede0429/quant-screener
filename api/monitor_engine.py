"""
Model monitor payload builder and persistence helpers.
"""
import json

import numpy as np


class MonitorEngine:
    def __init__(self, db, factor_engine, model_engine, portfolio_engine, learning_engine=None):
        self.db = db
        self.factor_engine = factor_engine
        self.model_engine = model_engine
        self.portfolio_engine = portfolio_engine
        self.learning_engine = learning_engine

    def build_live_payload(
        self,
        horizon_days: int | None = None,
        top_n: int = 20,
        model_weight: float = 0.35,
        neutralize_by: str = "sector",
        max_position_weight: float = 0.05,
        max_sector_weight: float = 0.25,
        max_industry_weight: float = 0.15,
        max_positions_per_sector: int = 4,
        max_positions_per_industry: int = 2,
        filters: dict | None = None,
        sectors: list | None = None,
        weights: dict | None = None,
        persist: bool = True,
    ) -> dict:
        filters = filters or {}
        sectors = sectors or []
        weights = weights or self._get_active_weights()
        registry = self.get_monitor_registry(horizon_days)
        effective_horizon = registry.get("horizon_days") or horizon_days or 20

        results, model_meta = self._run_ranked_screener_pipeline(
            weights=weights,
            filters=filters,
            sectors=sectors,
            use_model=True,
            model_horizon=effective_horizon,
            model_weight=model_weight,
        )
        portfolio = self.portfolio_engine.construct(
            results=results,
            top_n=top_n,
            neutralize_by=neutralize_by,
            max_position_weight=max_position_weight,
            max_sector_weight=max_sector_weight,
            max_industry_weight=max_industry_weight,
            max_positions_per_sector=max_positions_per_sector,
            max_positions_per_industry=max_positions_per_industry,
        )
        score_distribution = self._build_score_distribution(results)
        summary_md = self._build_model_monitor_summary(
            registry=registry,
            model_meta=model_meta,
            portfolio=portfolio,
            score_distribution=score_distribution,
        )
        report_date = self.db.get_latest_price_date()
        payload = {
            "report_date": report_date,
            "model": registry,
            "model_runtime": model_meta,
            "active_weights": weights,
            "portfolio": portfolio,
            "score_distribution": score_distribution,
            "label_stats": self.db.get_learning_label_stats(),
            "risk_policy": self.factor_engine.get_risk_policy(filters),
            "summary_md": summary_md,
        }
        if persist and report_date and registry.get("model_id"):
            self.db.upsert_model_monitor_report(
                report_date=report_date,
                model_id=registry["model_id"],
                horizon_days=int(effective_horizon),
                summary_md=summary_md,
                stats=payload,
            )
        return payload

    def get_history(self, limit: int = 30, horizon_days: int | None = None) -> dict:
        rows = self.db.get_model_monitor_reports(limit=limit, horizon_days=horizon_days)
        reports = []
        for row in rows:
            stats = self._parse_json(row.get("stats_json"))
            reports.append(
                {
                    "report_date": row.get("report_date"),
                    "model_id": row.get("model_id"),
                    "horizon_days": row.get("horizon_days"),
                    "summary_md": row.get("summary_md"),
                    "created_at": row.get("created_at"),
                    "updated_at": row.get("updated_at"),
                    "stats": stats,
                }
            )
        return {
            "count": len(reports),
            "reports": reports,
        }

    def build_dashboard(self, limit: int = 60, horizon_days: int | None = None) -> dict:
        history = self.get_history(limit=limit, horizon_days=horizon_days)
        reports = history.get("reports", [])
        if not reports:
            raise RuntimeError("No model monitor reports available yet.")

        ascending = sorted(reports, key=lambda item: item.get("report_date") or "")
        timeline = []
        model_ids = []
        applied_flags = []
        candidate_counts = []
        holding_counts = []
        cash_buffers = []
        top_sector_weights = []
        name_turnovers = []
        overlap_rates = []
        prev_codes = None

        for report in ascending:
            stats = report.get("stats") or {}
            portfolio = stats.get("portfolio") or {}
            score_distribution = stats.get("score_distribution") or {}
            holdings = portfolio.get("holdings") or []
            sector_exposure = portfolio.get("sector_exposure") or []
            model_runtime = stats.get("model_runtime") or {}
            final_score = score_distribution.get("final_score") or {}
            model_score = score_distribution.get("model_score") or {}
            codes = [item.get("code") for item in holdings if item.get("code")]
            prev_set = set(prev_codes or [])
            curr_set = set(codes)
            kept = sorted(prev_set & curr_set)
            added = sorted(curr_set - prev_set)
            dropped = sorted(prev_set - curr_set)
            name_turnover = None
            overlap_rate = None
            if prev_codes is not None:
                base = max(len(prev_set), len(curr_set), 1)
                name_turnover = round((len(added) + len(dropped)) / base * 100.0, 2)
                overlap_rate = round(len(kept) / base * 100.0, 2)
                name_turnovers.append(name_turnover)
                overlap_rates.append(overlap_rate)
            top_sector_weight = round(float(sector_exposure[0].get("weight", 0.0)), 2) if sector_exposure else 0.0
            candidate_count = int(score_distribution.get("candidate_count") or 0)
            holdings_count = int(portfolio.get("selected_count") or 0)
            cash_buffer = round(float(portfolio.get("cash_buffer", 0.0) or 0.0), 2)

            model_ids.append(report.get("model_id"))
            applied_flags.append(bool(model_runtime.get("applied")))
            candidate_counts.append(candidate_count)
            holding_counts.append(holdings_count)
            cash_buffers.append(cash_buffer)
            top_sector_weights.append(top_sector_weight)

            timeline.append(
                {
                    "report_date": report.get("report_date"),
                    "model_id": report.get("model_id"),
                    "candidate_count": candidate_count,
                    "holdings_count": holdings_count,
                    "cash_buffer": cash_buffer,
                    "top_sector_weight": top_sector_weight,
                    "final_score_p50": final_score.get("p50"),
                    "final_score_p90": final_score.get("p90"),
                    "model_score_p50": model_score.get("p50"),
                    "model_score_p90": model_score.get("p90"),
                    "model_applied": bool(model_runtime.get("applied")),
                    "name_turnover": name_turnover,
                    "overlap_rate": overlap_rate,
                    "new_codes": added,
                    "dropped_codes": dropped,
                }
            )
            prev_codes = codes

        latest = reports[0]
        summary = {
            "report_count": len(reports),
            "latest_report_date": latest.get("report_date"),
            "latest_model_id": latest.get("model_id"),
            "distinct_model_count": len({item for item in model_ids if item}),
            "avg_candidate_count": self._avg(candidate_counts),
            "avg_holdings_count": self._avg(holding_counts),
            "avg_cash_buffer": self._avg(cash_buffers),
            "avg_top_sector_weight": self._avg(top_sector_weights),
            "max_top_sector_weight": round(max(top_sector_weights), 2) if top_sector_weights else 0.0,
            "avg_name_turnover": self._avg(name_turnovers),
            "avg_overlap_rate": self._avg(overlap_rates),
            "model_applied_rate": round(sum(1 for flag in applied_flags if flag) / len(applied_flags) * 100.0, 2),
        }
        summary_md = (
            f"# Monitor Dashboard\n\n"
            f"- reports: {summary['report_count']}\n"
            f"- latest report: {summary['latest_report_date']}\n"
            f"- latest model: {summary['latest_model_id']}\n"
            f"- avg candidate count: {summary['avg_candidate_count']}\n"
            f"- avg holdings count: {summary['avg_holdings_count']}\n"
            f"- avg top sector weight: {summary['avg_top_sector_weight']}%\n"
            f"- avg name turnover: {summary['avg_name_turnover']}%\n"
            f"- model applied rate: {summary['model_applied_rate']}%\n"
        )
        return {
            "summary": summary,
            "summary_md": summary_md,
            "timeline": timeline,
            "latest_report": latest,
        }

    def get_monitor_registry(self, horizon_days: int | None) -> dict:
        registry = self.model_engine.get_serving_model_registry(horizon_days=horizon_days)
        if registry:
            return registry
        raw_registry = self.db.get_latest_model_registry(horizon_days=horizon_days, active_only=False)
        if not raw_registry:
            raise RuntimeError("No trained model available yet.")
        return self.model_engine._enrich_registry(raw_registry)

    def _run_ranked_screener_pipeline(
        self,
        weights: dict,
        filters: dict,
        sectors: list,
        use_model: bool,
        model_horizon: int,
        model_weight: float,
    ):
        stocks = self.db.get_all_stocks_with_data()
        if not stocks:
            raise RuntimeError("No cached stock data. Refresh data first.")

        results = self.factor_engine.score_and_rank(
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
            blended = self.model_engine.blend_results(
                results=results,
                horizon_days=model_horizon,
                model_weight=model_weight,
            )
            results = blended.pop("results")
            model_meta = blended
        return results, model_meta

    def _get_active_weights(self) -> dict:
        if self.learning_engine is not None:
            return self.learning_engine.get_active_weights()
        active_weights = self.db.get_setting_json("learning_active_weights")
        if active_weights:
            return active_weights
        raise RuntimeError("No active learning weights available.")

    @staticmethod
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

    @staticmethod
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
        rebalance = portfolio.get("rebalance") or {}
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
            f"- cash buffer: {portfolio.get('cash_buffer')}%\n"
            f"- estimated name turnover: {rebalance.get('name_turnover')}%\n\n"
            f"## Sector Exposure\n"
            f"{sector_text}\n\n"
            f"## Holdings\n"
            f"{top_holdings_text}\n"
        )

    @staticmethod
    def _avg(values: list[float]) -> float:
        cleaned = [float(value) for value in values if value is not None]
        if not cleaned:
            return 0.0
        return round(sum(cleaned) / len(cleaned), 2)

    @staticmethod
    def _parse_json(raw_value):
        if raw_value is None:
            return None
        if isinstance(raw_value, (dict, list)):
            return raw_value
        try:
            return json.loads(raw_value)
        except Exception:
            return raw_value
