"""
Daily review and online learning engine.
"""
import json
import logging
import math
from datetime import datetime
from typing import Iterable

from point_in_time import PointInTimeDataBuilder

logger = logging.getLogger(__name__)

DEFAULT_WEIGHTS = {
    "value": 25.0,
    "growth": 25.0,
    "quality": 25.0,
    "momentum": 25.0,
}

DEFAULT_HORIZONS = (5, 20, 60)


class DailyLearningEngine:
    def __init__(self, db, factor_engine):
        self.db = db
        self.factor_engine = factor_engine

    def get_active_weights(self) -> dict:
        stored = self.db.get_setting_json("learning_active_weights")
        if not isinstance(stored, dict):
            return dict(DEFAULT_WEIGHTS)
        return self._normalize_weights(stored)

    def get_status(self) -> dict:
        latest_run = self.db.get_latest_learning_run()
        latest_report = self.db.get_latest_review_report()
        if latest_run:
            latest_run = {
                **latest_run,
                "active_weights_json": self._safe_json_loads(latest_run.get("active_weights_json")),
                "learned_weights_json": self._safe_json_loads(latest_run.get("learned_weights_json")),
                "label_stats_json": self._safe_json_loads(latest_run.get("label_stats_json")),
                "meta_json": self._safe_json_loads(latest_run.get("meta_json")),
            }
        if latest_report:
            latest_report = {
                **latest_report,
                "stats_json": self._safe_json_loads(latest_report.get("stats_json")),
            }
        return {
            "active_weights": self.get_active_weights(),
            "latest_snapshot_date": self.db.get_latest_snapshot_date(),
            "snapshot_count": self.db.count_feature_snapshots(),
            "label_count": self.db.count_learning_labels(),
            "label_stats": self.db.get_learning_label_stats(),
            "latest_run": latest_run,
            "latest_report": latest_report,
        }

    def run_daily_cycle(
        self,
        top_n: int = 20,
        horizons: Iterable[int] | None = None,
        learning_horizon: int = 20,
        lookback_runs: int = 60,
        min_labeled_rows: int = 200,
        filters: dict | None = None,
        sectors: list | None = None,
        weights: dict | None = None,
        auto_apply: bool = True,
    ) -> dict:
        filters = filters or {}
        sectors = sectors or []
        horizons = sorted({int(h) for h in (horizons or DEFAULT_HORIZONS) if int(h) > 0})
        if learning_horizon not in horizons:
            horizons.append(learning_horizon)
            horizons = sorted(horizons)

        run_date = self.db.get_latest_price_date()
        if not run_date:
            raise RuntimeError("No price data available. Refresh market data first.")

        active_weights = self._normalize_weights(weights or self.get_active_weights())
        snapshot_result = self._build_daily_snapshots(
            snapshot_date=run_date,
            weights=active_weights,
            filters=filters,
            sectors=sectors,
            top_n=top_n,
        )
        label_result = self.refresh_labels(horizons)
        learned_result = self.compute_learned_weights(
            horizon_days=learning_horizon,
            lookback_runs=lookback_runs,
            min_labeled_rows=min_labeled_rows,
            base_weights=active_weights,
        )

        applied_weights = dict(active_weights)
        if auto_apply and learned_result.get("ready"):
            applied_weights = learned_result["recommended_weights"]
            self.db.save_setting_json("learning_active_weights", applied_weights)
            self.db.save_setting_json("learning_recommended_weights", applied_weights)
        elif learned_result.get("ready"):
            self.db.save_setting_json(
                "learning_recommended_weights",
                learned_result["recommended_weights"],
            )

        report = self._build_review_report(
            run_date=run_date,
            top_n=top_n,
            snapshot_result=snapshot_result,
            label_result=label_result,
            learned_result=learned_result,
            applied_weights=applied_weights,
            learning_horizon=learning_horizon,
        )

        self.db.upsert_review_report(
            report_date=run_date,
            learning_horizon=learning_horizon,
            summary_md=report["summary_md"],
            stats=report["stats"],
        )
        self.db.upsert_learning_run(
            {
                "run_date": run_date,
                "top_n": top_n,
                "total_candidates": snapshot_result["total_candidates"],
                "snapshot_count": snapshot_result["snapshot_count"],
                "active_weights_json": json.dumps(applied_weights, ensure_ascii=False, sort_keys=True),
                "learned_weights_json": json.dumps(
                    learned_result.get("recommended_weights"),
                    ensure_ascii=False,
                    sort_keys=True,
                ),
                "label_stats_json": json.dumps(label_result, ensure_ascii=False, sort_keys=True),
                "meta_json": json.dumps(
                    {
                        "learning_horizon": learning_horizon,
                        "lookback_runs": lookback_runs,
                        "min_labeled_rows": min_labeled_rows,
                        "filters": filters,
                        "sectors": sectors,
                        "top_picks": snapshot_result["top_picks"],
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                ),
            }
        )

        return {
            "run_date": run_date,
            "active_weights": applied_weights,
            "snapshot": snapshot_result,
            "labels": label_result,
            "learning": learned_result,
            "report": report,
        }

    def bootstrap_snapshots(
        self,
        frequency: str = "monthly",
        max_runs: int = 6,
        filters: dict | None = None,
        sectors: list | None = None,
        weights: dict | None = None,
        top_n: int = 20,
        use_indicators: bool = False,
        overwrite_existing: bool = False,
    ) -> dict:
        filters = filters or {}
        sectors = sectors or []
        weights = self._normalize_weights(weights or self.get_active_weights())

        benchmark_prices = self.db.get_benchmark_prices(limit=None)
        trade_dates = [row["trade_date"] for row in benchmark_prices]
        if not trade_dates:
            raise RuntimeError("No benchmark calendar available for bootstrap.")

        selected_dates = self._select_period_dates(trade_dates, frequency, max_runs)
        existing_dates = set(self.db.get_snapshot_dates(limit=max_runs * 10 + 20))
        builder = PointInTimeDataBuilder(self.db)

        created_dates = []
        skipped_dates = []
        total_rows = 0

        for snapshot_date in selected_dates:
            if snapshot_date in existing_dates and not overwrite_existing:
                skipped_dates.append(snapshot_date)
                continue
            if overwrite_existing:
                self.db.delete_feature_snapshots(snapshot_date)

            trimmed_universe = builder.build_universe(
                snapshot_date=snapshot_date,
                include_indicators=use_indicators,
                min_price_history=20,
                price_limit=250,
            )

            results = self.factor_engine.score_and_rank(
                stocks=trimmed_universe,
                weights=weights,
                filters=filters,
                sectors=sectors,
            )
            if not results:
                skipped_dates.append(snapshot_date)
                continue

            records = self._build_snapshot_records(
                snapshot_date=snapshot_date,
                results=results,
                weights=weights,
                filters=filters,
                sectors=sectors,
            )
            self.db.upsert_feature_snapshots(records)
            created_dates.append(snapshot_date)
            total_rows += len(records)

        return {
            "frequency": frequency,
            "requested_runs": max_runs,
            "created_dates": created_dates,
            "skipped_dates": skipped_dates,
            "created_rows": total_rows,
            "weights_used": weights,
            "use_indicators": use_indicators,
            "top_n": top_n,
            "overwrite_existing": overwrite_existing,
        }

    def _build_daily_snapshots(
        self,
        snapshot_date: str,
        weights: dict,
        filters: dict,
        sectors: list,
        top_n: int,
    ) -> dict:
        builder = PointInTimeDataBuilder(self.db)
        stocks = builder.build_universe(
            snapshot_date=snapshot_date,
            include_indicators=True,
            min_price_history=20,
            price_limit=250,
        )
        if not stocks:
            raise RuntimeError("No stock data available for screening.")

        results = self.factor_engine.score_and_rank(
            stocks=stocks,
            weights=weights,
            filters=filters,
            sectors=sectors,
        )

        records = self._build_snapshot_records(
            snapshot_date=snapshot_date,
            results=results,
            weights=weights,
            filters=filters,
            sectors=sectors,
        )
        self.db.delete_feature_snapshots(snapshot_date)
        self.db.upsert_feature_snapshots(records)

        top_picks = [
            {
                "rank": item["rank"],
                "code": item["code"],
                "name": item["name"],
                "sector": item.get("sector"),
                "composite_score": item.get("composite_score"),
            }
            for item in results[:top_n]
        ]
        return {
            "snapshot_date": snapshot_date,
            "snapshot_count": len(records),
            "total_candidates": len(results),
            "top_picks": top_picks,
            "weights_used": weights,
        }

    @staticmethod
    def _build_snapshot_records(
        snapshot_date: str,
        results: list[dict],
        weights: dict,
        filters: dict,
        sectors: list,
    ) -> list[dict]:
        records = []
        for item in results:
            records.append(
                {
                    "snapshot_date": snapshot_date,
                    "code": item["code"],
                    "name": item.get("name"),
                    "industry": item.get("industry"),
                    "sector": item.get("sector"),
                    "rank": item.get("rank"),
                    "composite_score": item.get("composite_score"),
                    "value_score": item.get("value_score"),
                    "growth_score": item.get("growth_score"),
                    "quality_score": item.get("quality_score"),
                    "momentum_score": item.get("momentum_score"),
                    "price": item.get("price"),
                    "pe": item.get("pe"),
                    "pb": item.get("pb"),
                    "roe": item.get("roe"),
                    "revenue_growth": item.get("revenue_growth"),
                    "net_income_growth": item.get("net_income_growth"),
                    "momentum_20d": item.get("momentum_20d"),
                    "momentum_60d": item.get("momentum_60d"),
                    "weights_json": json.dumps(weights, ensure_ascii=False, sort_keys=True),
                    "filters_json": json.dumps(filters, ensure_ascii=False, sort_keys=True),
                    "sectors_json": json.dumps(sectors, ensure_ascii=False),
                    "extra_json": json.dumps(
                        {
                            "dividend_yield": item.get("dividend_yield"),
                            "gross_margin": item.get("gross_margin"),
                            "debt_ratio": item.get("debt_ratio"),
                            "market_cap": item.get("market_cap"),
                            "fcf_yield": item.get("fcf_yield"),
                            "momentum_120d": item.get("momentum_120d"),
                            "rsi_14": item.get("rsi_14"),
                            "volume_ratio": item.get("volume_ratio"),
                            "avg_amount_20d": item.get("avg_amount_20d"),
                            "avg_turnover_20d": item.get("avg_turnover_20d"),
                            "volatility_20d": item.get("volatility_20d"),
                            "volatility_60d": item.get("volatility_60d"),
                            "max_drawdown_60d": item.get("max_drawdown_60d"),
                            "price_vs_ma20": item.get("price_vs_ma20"),
                            "price_vs_ma60": item.get("price_vs_ma60"),
                            "is_st": item.get("is_st"),
                            "is_suspended": item.get("is_suspended"),
                            "latest_trade_date": item.get("latest_trade_date"),
                            "latest_market_date": item.get("latest_market_date"),
                            "risk_flags": item.get("risk_flags", []),
                        },
                        ensure_ascii=False,
                        sort_keys=True,
                    ),
                }
            )
        return records

    def refresh_labels(self, horizons: Iterable[int]) -> dict:
        all_prices = self.db.get_all_prices()
        benchmark_prices = self.db.get_benchmark_prices(limit=None)
        calendar = [row["trade_date"] for row in benchmark_prices]
        calendar_index = {trade_date: idx for idx, trade_date in enumerate(calendar)}
        benchmark_close = {row["trade_date"]: row.get("close") for row in benchmark_prices}
        stock_close = {
            code: {price["trade_date"]: price.get("close") for price in prices}
            for code, prices in all_prices.items()
        }

        stats = {}
        for horizon in horizons:
            unlabeled = self.db.get_unlabeled_snapshots(horizon_days=horizon)
            created = 0
            pending = 0
            records = []

            for row in unlabeled:
                snapshot_date = row["snapshot_date"]
                calendar_pos = calendar_index.get(snapshot_date)
                if calendar_pos is None or calendar_pos + horizon >= len(calendar):
                    pending += 1
                    continue

                exit_date = calendar[calendar_pos + horizon]
                code = row["code"]
                entry_price = stock_close.get(code, {}).get(snapshot_date)
                exit_price = stock_close.get(code, {}).get(exit_date)
                bench_entry = benchmark_close.get(snapshot_date)
                bench_exit = benchmark_close.get(exit_date)

                if not self._valid_price(entry_price, exit_price, bench_entry, bench_exit):
                    continue

                stock_return = (exit_price / entry_price - 1.0) * 100.0
                benchmark_return = (bench_exit / bench_entry - 1.0) * 100.0
                alpha = stock_return - benchmark_return
                records.append(
                    {
                        "snapshot_date": snapshot_date,
                        "code": code,
                        "horizon_days": horizon,
                        "entry_date": snapshot_date,
                        "exit_date": exit_date,
                        "entry_price": round(entry_price, 4),
                        "exit_price": round(exit_price, 4),
                        "stock_return": round(stock_return, 4),
                        "benchmark_return": round(benchmark_return, 4),
                        "alpha": round(alpha, 4),
                    }
                )

            if records:
                self.db.upsert_learning_labels(records)
                created = len(records)

            stats[str(horizon)] = {
                "created": created,
                "pending_future_window": pending,
            }

        stats["summary"] = self.db.get_learning_label_stats()
        return stats

    def compute_learned_weights(
        self,
        horizon_days: int = 20,
        lookback_runs: int = 60,
        min_labeled_rows: int = 200,
        base_weights: dict | None = None,
    ) -> dict:
        rows = self.db.get_labeled_rows(horizon_days=horizon_days)
        if not rows:
            return {
                "ready": False,
                "reason": "No labeled rows available yet.",
                "horizon_days": horizon_days,
            }

        kept_rows = self._limit_rows_by_recent_runs(rows, lookback_runs)
        if len(kept_rows) < min_labeled_rows:
            return {
                "ready": False,
                "reason": "Not enough labeled rows yet.",
                "horizon_days": horizon_days,
                "sample_size": len(kept_rows),
                "min_labeled_rows": min_labeled_rows,
            }

        factor_names = ("value_score", "growth_score", "quality_score", "momentum_score")
        correlations = {}
        alpha_values = [row["alpha"] for row in kept_rows if row.get("alpha") is not None]
        if not alpha_values:
            return {
                "ready": False,
                "reason": "Alpha labels are empty.",
                "horizon_days": horizon_days,
            }

        for factor_name in factor_names:
            xs = []
            ys = []
            for row in kept_rows:
                factor_value = row.get(factor_name)
                alpha = row.get("alpha")
                if factor_value is None or alpha is None:
                    continue
                xs.append(float(factor_value))
                ys.append(float(alpha))
            correlations[factor_name] = round(self._pearson(xs, ys), 4) if len(xs) >= 2 else 0.0

        signal_strength = {
            "value": max(correlations["value_score"], 0.0),
            "growth": max(correlations["growth_score"], 0.0),
            "quality": max(correlations["quality_score"], 0.0),
            "momentum": max(correlations["momentum_score"], 0.0),
        }
        baseline = self._normalize_weights(base_weights or DEFAULT_WEIGHTS)

        if sum(signal_strength.values()) <= 0:
            recommended = dict(baseline)
        else:
            raw = self._normalize_weights(signal_strength, floor=5.0)
            blended = {
                key: baseline[key] * 0.4 + raw[key] * 0.6
                for key in baseline
            }
            recommended = self._normalize_weights(blended, floor=5.0)

        top_rows = [row for row in kept_rows if row.get("rank", 10**9) <= 20 and row.get("alpha") is not None]
        avg_top20_alpha = round(
            sum(row["alpha"] for row in top_rows) / len(top_rows),
            4,
        ) if top_rows else 0.0

        positive_alpha_rate = round(
            100.0 * sum(1 for row in kept_rows if row.get("alpha", 0) > 0) / len(kept_rows),
            2,
        )

        return {
            "ready": True,
            "horizon_days": horizon_days,
            "sample_size": len(kept_rows),
            "recent_run_count": len({row["snapshot_date"] for row in kept_rows}),
            "correlations": {
                "value": correlations["value_score"],
                "growth": correlations["growth_score"],
                "quality": correlations["quality_score"],
                "momentum": correlations["momentum_score"],
            },
            "recommended_weights": recommended,
            "avg_top20_alpha": avg_top20_alpha,
            "positive_alpha_rate": positive_alpha_rate,
        }

    def _build_review_report(
        self,
        run_date: str,
        top_n: int,
        snapshot_result: dict,
        label_result: dict,
        learned_result: dict,
        applied_weights: dict,
        learning_horizon: int,
    ) -> dict:
        latest_picks = snapshot_result["top_picks"][:top_n]
        latest_picks_text = "\n".join(
            f"- #{item['rank']} {item['code']} {item['name']} ({item.get('sector') or '-'}) score={item['composite_score']}"
            for item in latest_picks
        ) or "- No picks"

        if learned_result.get("ready"):
            learning_text = (
                f"- learning horizon: {learning_horizon} days\n"
                f"- sample size: {learned_result['sample_size']}\n"
                f"- avg top20 alpha: {learned_result['avg_top20_alpha']}%\n"
                f"- positive alpha rate: {learned_result['positive_alpha_rate']}%\n"
                f"- recommended weights: {json.dumps(learned_result['recommended_weights'], ensure_ascii=False)}\n"
                f"- factor correlations: {json.dumps(learned_result['correlations'], ensure_ascii=False)}"
            )
        else:
            learning_text = (
                f"- learning not ready yet\n"
                f"- reason: {learned_result.get('reason', 'unknown')}\n"
                f"- current active weights: {json.dumps(applied_weights, ensure_ascii=False)}"
            )

        label_lines = []
        for horizon_key, stat in label_result.items():
            if horizon_key == "summary":
                continue
            label_lines.append(
                f"- horizon {horizon_key}d: created={stat['created']}, pending={stat['pending_future_window']}"
            )
        label_text = "\n".join(label_lines) or "- No label updates"

        summary_md = (
            f"# Daily Review {self._fmt_date(run_date)}\n\n"
            f"## Snapshot\n"
            f"- total candidates: {snapshot_result['total_candidates']}\n"
            f"- snapshot rows saved: {snapshot_result['snapshot_count']}\n"
            f"- weights used: {json.dumps(snapshot_result['weights_used'], ensure_ascii=False)}\n\n"
            f"## Top Picks\n"
            f"{latest_picks_text}\n\n"
            f"## Label Update\n"
            f"{label_text}\n\n"
            f"## Learning\n"
            f"{learning_text}\n"
        )

        return {
            "summary_md": summary_md,
            "stats": {
                "run_date": run_date,
                "top_n": top_n,
                "learning_horizon": learning_horizon,
                "snapshot": snapshot_result,
                "labels": label_result,
                "learning": learned_result,
                "active_weights": applied_weights,
            },
        }

    @staticmethod
    def _valid_price(*values) -> bool:
        for value in values:
            if value is None or not math.isfinite(float(value)) or value <= 0:
                return False
        return True

    @staticmethod
    def _safe_json_loads(value):
        if not value:
            return None
        try:
            return json.loads(value)
        except (TypeError, json.JSONDecodeError):
            return value

    @staticmethod
    def _limit_rows_by_recent_runs(rows: list[dict], lookback_runs: int) -> list[dict]:
        allowed_dates = []
        seen = set()
        for row in rows:
            snapshot_date = row["snapshot_date"]
            if snapshot_date not in seen:
                seen.add(snapshot_date)
                allowed_dates.append(snapshot_date)
            if len(allowed_dates) >= lookback_runs:
                break
        allowed = set(allowed_dates)
        return [row for row in rows if row["snapshot_date"] in allowed]

    @staticmethod
    def _normalize_weights(weights: dict, floor: float = 0.0) -> dict:
        normalized = {
            "value": max(float(weights.get("value", 0.0)), floor),
            "growth": max(float(weights.get("growth", 0.0)), floor),
            "quality": max(float(weights.get("quality", 0.0)), floor),
            "momentum": max(float(weights.get("momentum", 0.0)), floor),
        }
        total = sum(normalized.values())
        if total <= 0:
            return dict(DEFAULT_WEIGHTS)
        normalized = {key: round(value / total * 100.0, 2) for key, value in normalized.items()}
        drift = round(100.0 - sum(normalized.values()), 2)
        normalized["momentum"] = round(normalized["momentum"] + drift, 2)
        return normalized

    @staticmethod
    def _pearson(xs: list[float], ys: list[float]) -> float:
        if len(xs) != len(ys) or len(xs) < 2:
            return 0.0
        mean_x = sum(xs) / len(xs)
        mean_y = sum(ys) / len(ys)
        num = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, ys))
        den_x = math.sqrt(sum((x - mean_x) ** 2 for x in xs))
        den_y = math.sqrt(sum((y - mean_y) ** 2 for y in ys))
        if den_x <= 0 or den_y <= 0:
            return 0.0
        return num / (den_x * den_y)

    @staticmethod
    def _fmt_date(trade_date: str) -> str:
        if len(trade_date) == 8 and trade_date.isdigit():
            return f"{trade_date[:4]}-{trade_date[4:6]}-{trade_date[6:]}"
        return trade_date

    @staticmethod
    def _select_period_dates(trade_dates: list[str], frequency: str, max_runs: int) -> list[str]:
        if frequency not in {"weekly", "monthly"}:
            raise RuntimeError("Bootstrap frequency must be weekly or monthly.")

        selected = []
        current_key = None
        current_last = None
        for trade_date in trade_dates:
            dt = datetime.strptime(trade_date, "%Y%m%d")
            date_key = trade_date[:6]
            if frequency == "weekly":
                iso_year, iso_week, _ = dt.isocalendar()
                date_key = f"{iso_year:04d}-W{iso_week:02d}"

            if current_key is None:
                current_key = date_key
                current_last = trade_date
                continue

            if date_key != current_key:
                selected.append(current_last)
                current_key = date_key
            current_last = trade_date

        if current_last:
            selected.append(current_last)

        return selected[-max_runs:]
