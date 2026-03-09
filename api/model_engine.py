"""
Model training, promotion gating, and safe score blending.
"""
import json
import logging
import os
import pickle
from datetime import datetime

import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)

DEFAULT_FEATURE_COLUMNS = [
    "rank",
    "rank_pct",
    "rank_inverse",
    "composite_score",
    "composite_gap",
    "value_score",
    "growth_score",
    "quality_score",
    "momentum_score",
    "quality_value_spread",
    "growth_momentum_blend",
    "value_momentum_blend",
    "price",
    "pe",
    "pb",
    "earnings_yield",
    "book_to_price",
    "roe",
    "revenue_growth",
    "net_income_growth",
    "momentum_20d",
    "momentum_60d",
    "momentum_120d",
    "momentum_accel_20_60",
    "momentum_accel_60_120",
    "market_cap",
    "log_market_cap",
    "dividend_yield",
    "gross_margin",
    "debt_ratio",
    "fcf_yield",
    "fcf_to_book",
    "rsi_14",
    "volume_ratio",
    "avg_amount_20d",
    "avg_turnover_20d",
    "liquidity_to_cap",
    "volatility_20d",
    "volatility_60d",
    "volatility_ratio_20_60",
    "max_drawdown_60d",
    "drawdown_to_vol_60",
    "price_vs_ma20",
    "price_vs_ma60",
    "trend_strength",
    "trend_gap",
    "risk_flag_count",
    "sector_size",
    "industry_size",
    "sector_relative_composite",
    "sector_relative_momentum",
    "sector_relative_value",
    "industry_relative_composite",
    "industry_relative_momentum",
    "sector_rank_pct",
    "industry_rank_pct",
    "market_cap_pct",
    "earnings_yield_pct",
    "book_to_price_pct",
    "momentum_120d_pct",
    "avg_amount_20d_pct",
    "low_volatility_20d_pct",
]

DEFAULT_PROMOTION_GATE = {
    "min_rank_ic": 0.01,
    "min_top20_alpha_lift": 0.0,
    "min_hit_rate_lift": 0.0,
    "min_precision_at_20_lift": 0.0,
}

CLASSIFICATION_LABEL_MODES = {
    "alpha_top_quantile",
    "alpha_positive",
    "alpha_threshold",
}



class PortfolioEngine:
    def construct(
        self,
        results: list[dict],
        top_n: int = 20,
        neutralize_by: str = "sector",
        max_position_weight: float = 0.05,
        max_sector_weight: float = 0.25,
        max_industry_weight: float = 0.15,
        max_positions_per_sector: int = 4,
        max_positions_per_industry: int = 2,
        existing_holdings: list | None = None,
        holding_periods_by_code: dict | None = None,
        rebalance_buffer: int = 0,
        max_new_positions: int | None = None,
        min_holding_periods: int = 0,
    ) -> dict:
        top_n = max(1, int(top_n))
        neutralize_by = (neutralize_by or "sector").strip().lower()
        if neutralize_by not in {"none", "sector", "industry"}:
            raise RuntimeError(f"Unsupported neutralize_by: {neutralize_by}")

        max_position_weight = max(0.001, min(1.0, float(max_position_weight)))
        max_sector_weight = max(0.0, min(1.0, float(max_sector_weight)))
        max_industry_weight = max(0.0, min(1.0, float(max_industry_weight)))
        max_positions_per_sector = max(1, int(max_positions_per_sector))
        max_positions_per_industry = max(1, int(max_positions_per_industry))
        rebalance_buffer = max(0, int(rebalance_buffer))
        min_holding_periods = max(0, int(min_holding_periods))
        max_new_positions = None if max_new_positions is None else max(0, int(max_new_positions))

        target_weight = min(1.0 / top_n, max_position_weight)
        sector_position_cap = min(
            max_positions_per_sector,
            self._positions_from_weight_cap(target_weight, max_sector_weight),
        )
        industry_position_cap = min(
            max_positions_per_industry,
            self._positions_from_weight_cap(target_weight, max_industry_weight),
        )

        existing_records = self._normalize_existing_holdings(existing_holdings)
        ranked_by_code = {item.get("code"): item for item in results if item.get("code")}
        holding_periods_by_code = {
            str(code): self._safe_int(periods, 0)
            for code, periods in (holding_periods_by_code or {}).items()
        }
        existing_codes = {record["code"] for record in existing_records}

        if not results:
            return self._empty_portfolio(
                top_n=top_n,
                neutralize_by=neutralize_by,
                max_position_weight=max_position_weight,
                max_sector_weight=max_sector_weight,
                max_industry_weight=max_industry_weight,
                max_positions_per_sector=max_positions_per_sector,
                max_positions_per_industry=max_positions_per_industry,
                existing_holdings=existing_records,
                rebalance_buffer=rebalance_buffer,
                max_new_positions=max_new_positions,
                min_holding_periods=min_holding_periods,
            )

        selected = []
        selected_codes = set()
        sector_counts = {}
        industry_counts = {}
        protected_existing = []
        fallback_existing = []
        for record in existing_records:
            code = record["code"]
            item = ranked_by_code.get(code)
            if not item:
                continue
            hold_periods = self._safe_int(record.get("periods_held"), holding_periods_by_code.get(code, 0))
            rank = self._safe_rank(item.get("rank"))
            keep_reason = None
            if min_holding_periods > 0 and hold_periods < min_holding_periods:
                keep_reason = "min_holding_periods"
            elif rebalance_buffer > 0 and rank <= top_n + rebalance_buffer:
                keep_reason = "rebalance_buffer"
            payload = {"item": item, "rank": rank, "hold_periods": hold_periods, "reason": keep_reason}
            if keep_reason:
                protected_existing.append(payload)
            else:
                fallback_existing.append(payload)

        protected_existing.sort(key=lambda entry: (0 if entry["reason"] == "min_holding_periods" else 1, entry["rank"]))
        fallback_existing.sort(key=lambda entry: entry["rank"])

        for entry in protected_existing:
            self._try_add_candidate(
                item=entry["item"],
                selected=selected,
                selected_codes=selected_codes,
                sector_counts=sector_counts,
                industry_counts=industry_counts,
                sector_position_cap=sector_position_cap,
                industry_position_cap=industry_position_cap,
            )
            if len(selected) >= top_n:
                break

        if neutralize_by == "none":
            candidate_groups = [("all", list(results))]
        else:
            candidate_groups = self._build_group_queues(results, neutralize_by)

        new_positions_used = 0
        while len(selected) < top_n:
            progressed = False
            for _, queue in candidate_groups:
                while queue and len(selected) < top_n:
                    item = queue.pop(0)
                    code = item.get("code")
                    if not code or code in selected_codes:
                        continue
                    if not self._can_add(
                        item=item,
                        sector_counts=sector_counts,
                        industry_counts=industry_counts,
                        sector_position_cap=sector_position_cap,
                        industry_position_cap=industry_position_cap,
                    ):
                        continue
                    is_new_position = code not in existing_codes
                    if is_new_position and max_new_positions is not None and new_positions_used >= max_new_positions:
                        continue
                    self._append_candidate(
                        item=item,
                        selected=selected,
                        selected_codes=selected_codes,
                        sector_counts=sector_counts,
                        industry_counts=industry_counts,
                    )
                    if is_new_position:
                        new_positions_used += 1
                    progressed = True
                    break
            if not progressed:
                break

        if len(selected) < top_n:
            for entry in fallback_existing:
                if len(selected) >= top_n:
                    break
                self._try_add_candidate(
                    item=entry["item"],
                    selected=selected,
                    selected_codes=selected_codes,
                    sector_counts=sector_counts,
                    industry_counts=industry_counts,
                    sector_position_cap=sector_position_cap,
                    industry_position_cap=industry_position_cap,
                )

        cash_buffer = max(0.0, 1.0 - target_weight * len(selected))
        holdings = []
        for portfolio_rank, item in enumerate(selected, 1):
            holdings.append(
                {
                    "portfolio_rank": portfolio_rank,
                    "code": item.get("code"),
                    "name": item.get("name"),
                    "sector": item.get("sector"),
                    "industry": item.get("industry"),
                    "rank": item.get("rank"),
                    "composite_score": item.get("composite_score"),
                    "model_score": item.get("model_score"),
                    "final_score": item.get("final_score", item.get("composite_score")),
                    "target_weight": round(target_weight * 100, 2),
                }
            )

        return {
            "enabled": True,
            "selected_count": len(holdings),
            "requested_top_n": top_n,
            "neutralize_by": neutralize_by,
            "target_position_weight": round(target_weight * 100, 2),
            "cash_buffer": round(cash_buffer * 100, 2),
            "holdings": holdings,
            "sector_exposure": self._build_exposure(holdings, "sector"),
            "industry_exposure": self._build_exposure(holdings, "industry"),
            "constraints": self._build_constraints(
                top_n=top_n,
                neutralize_by=neutralize_by,
                max_position_weight=max_position_weight,
                max_sector_weight=max_sector_weight,
                max_industry_weight=max_industry_weight,
                max_positions_per_sector=sector_position_cap,
                max_positions_per_industry=industry_position_cap,
                target_weight=target_weight,
                rebalance_buffer=rebalance_buffer,
                max_new_positions=max_new_positions,
                min_holding_periods=min_holding_periods,
            ),
            "rebalance": self._build_rebalance_summary(
                existing_records=existing_records,
                holdings=holdings,
                target_weight=target_weight,
                rebalance_buffer=rebalance_buffer,
                max_new_positions=max_new_positions,
                min_holding_periods=min_holding_periods,
                protected_existing=protected_existing,
            ),
        }

    def _empty_portfolio(
        self,
        top_n: int,
        neutralize_by: str,
        max_position_weight: float,
        max_sector_weight: float,
        max_industry_weight: float,
        max_positions_per_sector: int,
        max_positions_per_industry: int,
        existing_holdings: list | None = None,
        rebalance_buffer: int = 0,
        max_new_positions: int | None = None,
        min_holding_periods: int = 0,
    ) -> dict:
        existing_records = self._normalize_existing_holdings(existing_holdings)
        return {
            "enabled": True,
            "selected_count": 0,
            "requested_top_n": int(top_n),
            "neutralize_by": neutralize_by,
            "target_position_weight": 0.0,
            "cash_buffer": 100.0,
            "holdings": [],
            "sector_exposure": [],
            "industry_exposure": [],
            "constraints": self._build_constraints(
                top_n=top_n,
                neutralize_by=neutralize_by,
                max_position_weight=max_position_weight,
                max_sector_weight=max_sector_weight,
                max_industry_weight=max_industry_weight,
                max_positions_per_sector=max_positions_per_sector,
                max_positions_per_industry=max_positions_per_industry,
                target_weight=0.0,
                rebalance_buffer=rebalance_buffer,
                max_new_positions=max_new_positions,
                min_holding_periods=min_holding_periods,
            ),
            "rebalance": self._build_rebalance_summary(
                existing_records=existing_records,
                holdings=[],
                target_weight=0.0,
                rebalance_buffer=rebalance_buffer,
                max_new_positions=max_new_positions,
                min_holding_periods=min_holding_periods,
                protected_existing=[],
            ),
        }

    def _build_group_queues(self, results: list[dict], group_key: str) -> list[tuple[str, list[dict]]]:
        grouped = {}
        for item in results:
            group_name = self._group_name(item, group_key)
            grouped.setdefault(group_name, []).append(item)
        ordered = sorted(
            grouped.items(),
            key=lambda entry: float(entry[1][0].get("final_score") or entry[1][0].get("composite_score") or 0.0),
            reverse=True,
        )
        return [(group_name, list(items)) for group_name, items in ordered]

    @staticmethod
    def _group_name(item: dict, group_key: str) -> str:
        return item.get(group_key) or "Unclassified"

    @staticmethod
    def _positions_from_weight_cap(target_weight: float, weight_cap: float) -> int:
        if weight_cap <= 0 or target_weight <= 0:
            return 0
        return max(1, int(weight_cap / target_weight + 1e-9))

    def _can_add(self, item: dict, sector_counts: dict, industry_counts: dict, sector_position_cap: int, industry_position_cap: int) -> bool:
        sector = self._group_name(item, "sector")
        industry = self._group_name(item, "industry")
        return sector_counts.get(sector, 0) < sector_position_cap and industry_counts.get(industry, 0) < industry_position_cap

    def _append_candidate(self, item: dict, selected: list[dict], selected_codes: set, sector_counts: dict, industry_counts: dict):
        code = item.get("code")
        if not code or code in selected_codes:
            return False
        selected.append(item)
        selected_codes.add(code)
        self._apply_counts(item, sector_counts, industry_counts)
        return True

    def _try_add_candidate(self, item: dict, selected: list[dict], selected_codes: set, sector_counts: dict, industry_counts: dict, sector_position_cap: int, industry_position_cap: int) -> bool:
        if not self._can_add(item=item, sector_counts=sector_counts, industry_counts=industry_counts, sector_position_cap=sector_position_cap, industry_position_cap=industry_position_cap):
            return False
        return self._append_candidate(item=item, selected=selected, selected_codes=selected_codes, sector_counts=sector_counts, industry_counts=industry_counts)

    def _apply_counts(self, item: dict, sector_counts: dict, industry_counts: dict):
        sector = self._group_name(item, "sector")
        industry = self._group_name(item, "industry")
        sector_counts[sector] = sector_counts.get(sector, 0) + 1
        industry_counts[industry] = industry_counts.get(industry, 0) + 1

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

    def _build_constraints(self, top_n: int, neutralize_by: str, max_position_weight: float, max_sector_weight: float, max_industry_weight: float, max_positions_per_sector: int, max_positions_per_industry: int, target_weight: float, rebalance_buffer: int, max_new_positions: int | None, min_holding_periods: int) -> dict:
        return {
            "top_n": int(top_n),
            "neutralize_by": neutralize_by,
            "max_position_weight": round(float(max_position_weight) * 100, 2),
            "max_sector_weight": round(float(max_sector_weight) * 100, 2),
            "max_industry_weight": round(float(max_industry_weight) * 100, 2),
            "max_positions_per_sector": int(max_positions_per_sector),
            "max_positions_per_industry": int(max_positions_per_industry),
            "target_weight": round(float(target_weight) * 100, 2),
            "rebalance_buffer": int(rebalance_buffer),
            "max_new_positions": max_new_positions,
            "min_holding_periods": int(min_holding_periods),
        }

    def _build_rebalance_summary(self, existing_records: list[dict], holdings: list[dict], target_weight: float, rebalance_buffer: int, max_new_positions: int | None, min_holding_periods: int, protected_existing: list[dict]) -> dict:
        previous_codes = [record["code"] for record in existing_records]
        current_codes = [holding.get("code") for holding in holdings if holding.get("code")]
        previous_set = set(previous_codes)
        current_set = set(current_codes)
        kept_codes = sorted(previous_set & current_set)
        new_codes = sorted(current_set - previous_set)
        dropped_codes = sorted(previous_set - current_set)
        locked_codes = sorted(entry["item"].get("code") for entry in protected_existing if entry["reason"] == "min_holding_periods" and entry["item"].get("code") in current_set)
        buffer_codes = sorted(entry["item"].get("code") for entry in protected_existing if entry["reason"] == "rebalance_buffer" and entry["item"].get("code") in current_set)
        return {
            "enabled": bool(previous_codes or rebalance_buffer or max_new_positions is not None or min_holding_periods > 0),
            "previous_count": len(previous_codes),
            "kept_count": len(kept_codes),
            "new_count": len(new_codes),
            "dropped_count": len(dropped_codes),
            "kept_codes": kept_codes,
            "new_codes": new_codes,
            "dropped_codes": dropped_codes,
            "kept_due_to_min_holding": locked_codes,
            "kept_due_to_buffer": buffer_codes,
            "name_turnover": self._estimate_name_turnover(previous_codes, current_codes),
            "estimated_weight_turnover": self._estimate_weight_turnover(previous_codes, current_codes, target_weight),
            "rebalance_buffer": int(rebalance_buffer),
            "max_new_positions": max_new_positions,
            "min_holding_periods": int(min_holding_periods),
        }

    @staticmethod
    def _estimate_name_turnover(previous_codes: list[str], current_codes: list[str]) -> float:
        previous_set = set(previous_codes)
        current_set = set(current_codes)
        base = max(len(previous_set), len(current_set), 1)
        changed = len(current_set - previous_set) + len(previous_set - current_set)
        return round(changed / base * 100.0, 2)

    @staticmethod
    def _estimate_weight_turnover(previous_codes: list[str], current_codes: list[str], target_weight: float) -> float:
        previous_map = {}
        current_map = {}
        if previous_codes:
            prev_weight = 1.0 / len(previous_codes)
            for code in previous_codes:
                previous_map[code] = prev_weight
        else:
            previous_map["__CASH__"] = 1.0
        for code in current_codes:
            current_map[code] = target_weight
        current_cash = max(0.0, 1.0 - target_weight * len(current_codes))
        if current_cash > 0:
            current_map["__CASH__"] = current_cash
        keys = set(previous_map) | set(current_map)
        turnover = sum(abs(current_map.get(key, 0.0) - previous_map.get(key, 0.0)) for key in keys) / 2.0
        return round(turnover * 100.0, 2)

    @staticmethod
    def _normalize_existing_holdings(existing_holdings: list | None) -> list[dict]:
        normalized = []
        for item in existing_holdings or []:
            if isinstance(item, str):
                code = item.strip()
                if code:
                    normalized.append({"code": code})
                continue
            if isinstance(item, dict):
                code = (item.get("code") or item.get("ts_code") or "").strip()
                if code:
                    normalized.append(item | {"code": code})
        return normalized

    @staticmethod
    def _safe_int(value, default: int = 0) -> int:
        try:
            if value is None:
                return int(default)
            return int(value)
        except (TypeError, ValueError):
            return int(default)

    @staticmethod
    def _safe_rank(value) -> int:
        try:
            return int(value)
        except (TypeError, ValueError):
            return 999999


class ModelEngine:
    def __init__(self, db, artifact_dir: str | None = None):
        self.db = db
        self.artifact_dir = artifact_dir or os.environ.get("MODEL_ARTIFACT_DIR", "/app/data/models")
        os.makedirs(self.artifact_dir, exist_ok=True)

    def train_model(
        self,
        horizon_days: int = 20,
        validation_runs: int = 1,
        min_train_runs: int = 2,
        min_rows: int = 1000,
        task_type: str = "classification",
        target_column: str = "alpha",
        label_mode: str = "alpha_top_quantile",
        label_quantile: float = 0.20,
        alpha_threshold: float = 0.0,
        feature_columns: list[str] | None = None,
        score_latest_snapshot: bool = True,
        latest_limit: int = 20,
        activate: bool = True,
        start_date: str | None = None,
        end_date: str | None = None,
        promotion_gate: dict | None = None,
        force_activate: bool = False,
    ) -> dict:
        feature_columns = feature_columns or list(DEFAULT_FEATURE_COLUMNS)
        task_type = self._normalize_task_type(task_type)
        label_config = self._build_label_config(
            task_type=task_type,
            target_column=target_column,
            label_mode=label_mode,
            label_quantile=label_quantile,
            alpha_threshold=alpha_threshold,
        )
        promotion_gate = self._build_promotion_gate(promotion_gate)

        dataset = self.build_training_dataset(
            horizon_days=horizon_days,
            target_column=target_column,
            task_type=task_type,
            label_mode=label_config["label_mode"],
            label_quantile=label_config["label_quantile"],
            alpha_threshold=label_config["alpha_threshold"],
            feature_columns=feature_columns,
            start_date=start_date,
            end_date=end_date,
        )
        if dataset.empty:
            raise RuntimeError("No labeled training rows available for model training.")
        if len(dataset) < min_rows:
            raise RuntimeError(f"Not enough training rows: {len(dataset)} < {min_rows}")

        training_target = label_config["target_column"]
        unique_dates = sorted(dataset["snapshot_date"].dropna().unique().tolist())
        if len(unique_dates) < min_train_runs + validation_runs:
            raise RuntimeError(
                f"Not enough snapshot dates for walk-forward validation: {len(unique_dates)} available"
            )

        validation_dates = unique_dates[-validation_runs:]
        fold_metrics = []
        validation_frames = []
        for validation_date in validation_dates:
            train_dates = [date for date in unique_dates if date < validation_date]
            if len(train_dates) < min_train_runs:
                continue

            train_frame = dataset[dataset["snapshot_date"].isin(train_dates)].copy()
            validation_frame = dataset[dataset["snapshot_date"] == validation_date].copy()
            if train_frame.empty or validation_frame.empty:
                continue
            if task_type == "classification" and train_frame[training_target].nunique() < 2:
                logger.warning(
                    "Skipping validation date %s because train labels only contain one class.",
                    validation_date,
                )
                continue

            model = self._build_pipeline(task_type)
            model.fit(train_frame[feature_columns], train_frame[training_target])
            validation_frame["model_score"] = self._predict_scores(
                model=model,
                features=validation_frame[feature_columns],
                task_type=task_type,
            )
            validation_frames.append(validation_frame)
            fold_metrics.append(
                self._evaluate_predictions(
                    validation_frame,
                    prediction_column="model_score",
                    target_column=training_target,
                    task_type=task_type,
                )
            )

        if not fold_metrics:
            raise RuntimeError("Walk-forward validation could not produce any valid fold.")
        used_validation_dates = [fold["snapshot_date"] for fold in fold_metrics]

        if task_type == "classification" and dataset[training_target].nunique() < 2:
            raise RuntimeError("Final training dataset only contains one class after labeling.")

        final_model = self._build_pipeline(task_type)
        final_model.fit(dataset[feature_columns], dataset[training_target])

        metrics = self._aggregate_metrics(fold_metrics)
        metrics["feature_importance"] = self._extract_feature_importance(final_model, feature_columns)
        if task_type == "classification":
            metrics["positive_rate"] = round(100.0 * float(dataset[training_target].mean()), 2)

        promotion = self._evaluate_promotion(metrics, promotion_gate)
        activation_applied = bool(activate and (promotion["eligible"] or force_activate))

        model_prefix = "cls" if task_type == "classification" else "reg"
        model_id = f"model_{model_prefix}_{horizon_days}d_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"
        artifact_path = os.path.join(self.artifact_dir, f"{model_id}.pkl")
        bundle = {
            "model_id": model_id,
            "trained_at": datetime.utcnow().isoformat(),
            "task_type": task_type,
            "horizon_days": horizon_days,
            "target_column": training_target,
            "label_source_column": label_config["label_source_column"],
            "label_mode": label_config["label_mode"],
            "label_quantile": label_config["label_quantile"],
            "alpha_threshold": label_config["alpha_threshold"],
            "feature_columns": feature_columns,
            "pipeline": final_model,
            "metrics": metrics,
            "promotion": promotion,
        }
        with open(artifact_path, "wb") as handle:
            pickle.dump(bundle, handle)

        registry_record = {
            "model_id": model_id,
            "model_type": self._resolve_model_type(task_type),
            "horizon_days": horizon_days,
            "train_start_date": min(unique_dates),
            "train_end_date": max(unique_dates),
            "validation_start_date": min(used_validation_dates),
            "validation_end_date": max(used_validation_dates),
            "feature_names_json": json.dumps(feature_columns, ensure_ascii=False),
            "metrics_json": json.dumps(metrics, ensure_ascii=False, sort_keys=True),
            "artifact_path": artifact_path,
            "train_rows": int(len(dataset)),
            "validation_rows": int(sum(len(frame) for frame in validation_frames)),
            "is_active": 1 if activation_applied else 0,
            "extra_json": json.dumps(
                {
                    "task_type": task_type,
                    "target_column": target_column,
                    "training_target": training_target,
                    "label_source_column": label_config["label_source_column"],
                    "label_mode": label_config["label_mode"],
                    "label_quantile": label_config["label_quantile"],
                    "alpha_threshold": label_config["alpha_threshold"],
                    "validation_runs": validation_runs,
                    "min_train_runs": min_train_runs,
                    "folds": fold_metrics,
                    "promotion": promotion,
                    "activation_requested": activate,
                    "activation_applied": activation_applied,
                    "force_activate": force_activate,
                },
                ensure_ascii=False,
                sort_keys=True,
            ),
        }
        self.db.upsert_model_registry(registry_record, activate=activation_applied)

        latest_predictions = []
        latest_snapshot_date = self.db.get_latest_snapshot_date()
        if score_latest_snapshot and latest_snapshot_date:
            latest_predictions = self.score_snapshot(
                model_id=model_id,
                snapshot_date=latest_snapshot_date,
                bundle=bundle,
                persist=True,
                limit=latest_limit,
            )

        return {
            "model_id": model_id,
            "model_type": registry_record["model_type"],
            "task_type": task_type,
            "horizon_days": horizon_days,
            "dataset": {
                "rows": int(len(dataset)),
                "snapshot_dates": len(unique_dates),
                "train_start_date": registry_record["train_start_date"],
                "train_end_date": registry_record["train_end_date"],
                "feature_columns": feature_columns,
                "target_column": training_target,
                "label_mode": label_config["label_mode"],
                "label_quantile": label_config["label_quantile"],
                "positive_rate": metrics.get("positive_rate"),
            },
            "metrics": metrics,
            "promotion": promotion,
            "activation_applied": activation_applied,
            "latest_snapshot_date": latest_snapshot_date,
            "latest_predictions": latest_predictions,
            "artifact_path": artifact_path,
        }

    def build_training_dataset(
        self,
        horizon_days: int,
        target_column: str = "alpha",
        task_type: str = "classification",
        label_mode: str = "alpha_top_quantile",
        label_quantile: float = 0.20,
        alpha_threshold: float = 0.0,
        feature_columns: list[str] | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> pd.DataFrame:
        rows = self.db.get_model_training_rows(
            horizon_days=horizon_days,
            start_date=start_date,
            end_date=end_date,
        )
        frame = pd.DataFrame(rows)
        if frame.empty:
            return frame

        task_type = self._normalize_task_type(task_type)
        label_config = self._build_label_config(
            task_type=task_type,
            target_column=target_column,
            label_mode=label_mode,
            label_quantile=label_quantile,
            alpha_threshold=alpha_threshold,
        )
        feature_columns = feature_columns or DEFAULT_FEATURE_COLUMNS
        frame = self._prepare_feature_frame(frame, feature_columns)

        numeric_columns = set(feature_columns + ["alpha", target_column, "stock_return", "benchmark_return"])
        for column in numeric_columns:
            if column in frame.columns:
                frame[column] = pd.to_numeric(frame[column], errors="coerce")

        frame = frame.dropna(subset=["alpha"]).reset_index(drop=True)
        if task_type == "classification":
            frame[label_config["target_column"]] = self._build_classification_target(
                frame=frame,
                label_mode=label_config["label_mode"],
                label_quantile=label_config["label_quantile"],
                alpha_threshold=label_config["alpha_threshold"],
            )
            frame = frame.dropna(subset=[label_config["target_column"]]).reset_index(drop=True)
            frame[label_config["target_column"]] = frame[label_config["target_column"]].astype(int)
        else:
            if target_column not in frame.columns:
                raise RuntimeError(f"Target column not found in training rows: {target_column}")
            frame = frame.dropna(subset=[target_column]).reset_index(drop=True)
        return frame

    def get_latest_model(
        self,
        horizon_days: int | None = None,
        snapshot_date: str | None = None,
        limit: int = 20,
    ) -> dict:
        registry = self.db.get_latest_model_registry(horizon_days=horizon_days, active_only=False)
        if not registry:
            raise RuntimeError("No trained model available yet.")

        parsed_registry = self._enrich_registry(registry)
        target_snapshot = snapshot_date or self.db.get_latest_snapshot_date()
        if not target_snapshot:
            return {"model": parsed_registry, "predictions": []}

        predictions = self.db.get_model_predictions(
            model_id=registry["model_id"],
            snapshot_date=target_snapshot,
            limit=limit,
        )
        if not predictions:
            predictions = self.score_snapshot(
                model_id=registry["model_id"],
                snapshot_date=target_snapshot,
                persist=True,
                limit=limit,
            )
        return {
            "model": parsed_registry,
            "snapshot_date": target_snapshot,
            "predictions": predictions,
        }

    def get_serving_model_registry(self, horizon_days: int | None = None) -> dict | None:
        registry = self.db.get_latest_model_registry(horizon_days=horizon_days, active_only=True)
        if not registry:
            return None
        enriched = self._enrich_registry(registry)
        if enriched["serving_ready"]:
            return enriched
        return None

    def blend_results(
        self,
        results: list[dict],
        horizon_days: int = 20,
        model_weight: float = 0.35,
    ) -> dict:
        if not results:
            return {
                "applied": False,
                "reason": "No screener results available.",
                "results": [],
            }

        registry = self.get_serving_model_registry(horizon_days=horizon_days)
        if not registry:
            return {
                "applied": False,
                "reason": "No active model passed the promotion gate.",
                "results": results,
            }

        bundle = self._load_bundle(registry["model_id"])
        feature_columns = bundle["feature_columns"]
        task_type = bundle.get("task_type", "regression")
        base_result_columns = list(results[0].keys())
        frame = self._prepare_feature_frame(pd.DataFrame(results), feature_columns)
        for column in feature_columns:
            if column not in frame.columns:
                frame[column] = pd.NA
            frame[column] = pd.to_numeric(frame[column], errors="coerce")

        frame["model_score"] = self._predict_scores(
            model=bundle["pipeline"],
            features=frame[feature_columns],
            task_type=task_type,
        )
        ranked = frame["model_score"].rank(method="first", ascending=False)
        total_rows = len(frame)
        if total_rows <= 1:
            frame["model_percentile"] = 100.0
        else:
            frame["model_percentile"] = 100.0 * (1 - (ranked - 1) / (total_rows - 1))

        model_weight = max(0.0, min(1.0, float(model_weight)))
        frame["final_score"] = frame["composite_score"] * (1 - model_weight) + frame["model_percentile"] * model_weight
        frame = frame.sort_values(["final_score", "composite_score"], ascending=[False, False]).reset_index(drop=True)
        frame["rank"] = frame.index + 1

        blended_results = []
        for row in frame.to_dict(orient="records"):
            blended_results.append(
                {
                    **{key: self._json_safe_value(row.get(key)) for key in base_result_columns},
                    "model_id": registry["model_id"],
                    "model_score": self._safe_round(row.get("model_score")),
                    "model_percentile": self._safe_round(row.get("model_percentile")),
                    "final_score": self._safe_round(row.get("final_score")),
                }
            )

        return {
            "applied": True,
            "model_id": registry["model_id"],
            "model_weight": model_weight,
            "promotion": registry.get("promotion"),
            "results": blended_results,
        }

    def score_snapshot(
        self,
        model_id: str,
        snapshot_date: str,
        bundle: dict | None = None,
        persist: bool = True,
        limit: int = 20,
    ) -> list[dict]:
        bundle = bundle or self._load_bundle(model_id)
        feature_columns = bundle["feature_columns"]
        task_type = bundle.get("task_type", "regression")
        frame = pd.DataFrame(self.db.get_feature_snapshots(snapshot_date, limit=None))
        if frame.empty:
            raise RuntimeError(f"No feature snapshot found for {snapshot_date}")

        frame = self._prepare_feature_frame(frame, feature_columns)
        for column in feature_columns:
            if column not in frame.columns:
                frame[column] = pd.NA
            frame[column] = pd.to_numeric(frame[column], errors="coerce")

        frame["model_score"] = self._predict_scores(
            model=bundle["pipeline"],
            features=frame[feature_columns],
            task_type=task_type,
        )
        frame = frame.sort_values(["model_score", "composite_score"], ascending=[False, False]).reset_index(drop=True)
        frame["model_rank"] = frame.index + 1

        records = []
        for row in frame.to_dict(orient="records"):
            records.append(
                {
                    "model_id": model_id,
                    "snapshot_date": snapshot_date,
                    "code": row["code"],
                    "name": row.get("name"),
                    "sector": row.get("sector"),
                    "baseline_rank": row.get("rank"),
                    "composite_score": self._safe_round(row.get("composite_score")),
                    "model_score": self._safe_round(row.get("model_score")),
                    "model_rank": int(row["model_rank"]),
                }
            )
        if persist:
            self.db.upsert_model_predictions(records)
        return records[:limit]

    def _load_bundle(self, model_id: str) -> dict:
        registry = self.db.get_model_registry(model_id)
        if not registry:
            raise RuntimeError(f"Model not found: {model_id}")
        artifact_path = registry.get("artifact_path")
        if not artifact_path or not os.path.exists(artifact_path):
            raise RuntimeError(f"Model artifact missing: {artifact_path}")
        with open(artifact_path, "rb") as handle:
            return pickle.load(handle)

    @staticmethod
    def _build_pipeline(task_type: str):
        from sklearn.ensemble import HistGradientBoostingClassifier, RandomForestRegressor
        from sklearn.impute import SimpleImputer
        from sklearn.pipeline import Pipeline

        if task_type == "classification":
            estimator = HistGradientBoostingClassifier(
                learning_rate=0.05,
                max_depth=6,
                max_iter=240,
                min_samples_leaf=40,
                l2_regularization=0.1,
                random_state=42,
            )
        else:
            estimator = RandomForestRegressor(
                n_estimators=240,
                max_depth=8,
                min_samples_leaf=25,
                random_state=42,
                n_jobs=-1,
            )

        return Pipeline(
            steps=[
                ("imputer", SimpleImputer(strategy="median")),
                ("model", estimator),
            ]
        )

    @staticmethod
    def _predict_scores(model, features: pd.DataFrame, task_type: str):
        if task_type == "classification" and hasattr(model, "predict_proba"):
            probabilities = model.predict_proba(features)
            if probabilities.ndim == 2 and probabilities.shape[1] > 1:
                return probabilities[:, 1]
        if task_type == "classification" and hasattr(model, "decision_function"):
            return model.decision_function(features)
        return model.predict(features)

    @staticmethod
    def _normalize_task_type(task_type: str | None) -> str:
        normalized = (task_type or "classification").strip().lower()
        if normalized not in {"classification", "regression"}:
            raise RuntimeError(f"Unsupported task_type: {task_type}")
        return normalized

    @staticmethod
    def _resolve_model_type(task_type: str) -> str:
        if task_type == "classification":
            return "hist_gradient_boosting_classifier"
        return "random_forest_regressor"

    def _build_label_config(
        self,
        task_type: str,
        target_column: str,
        label_mode: str,
        label_quantile: float,
        alpha_threshold: float,
    ) -> dict:
        if task_type == "regression":
            return {
                "target_column": target_column,
                "label_source_column": target_column,
                "label_mode": None,
                "label_quantile": None,
                "alpha_threshold": None,
            }

        normalized_mode = (label_mode or "alpha_top_quantile").strip().lower()
        if normalized_mode not in CLASSIFICATION_LABEL_MODES:
            raise RuntimeError(f"Unsupported label_mode: {label_mode}")

        normalized_quantile = min(max(float(label_quantile), 0.01), 0.99)
        return {
            "target_column": "target_label",
            "label_source_column": "alpha",
            "label_mode": normalized_mode,
            "label_quantile": normalized_quantile,
            "alpha_threshold": float(alpha_threshold),
        }

    @staticmethod
    def _build_classification_target(
        frame: pd.DataFrame,
        label_mode: str,
        label_quantile: float,
        alpha_threshold: float,
    ) -> pd.Series:
        if label_mode == "alpha_positive":
            return (frame["alpha"] > 0).astype(int)
        if label_mode == "alpha_threshold":
            return (frame["alpha"] >= float(alpha_threshold)).astype(int)

        alpha_rank = frame.groupby("snapshot_date")["alpha"].rank(method="first", ascending=False)
        group_size = frame.groupby("snapshot_date")["alpha"].transform("count")
        cutoff = (group_size * float(label_quantile)).apply(lambda value: max(int(np.ceil(value)), 1))
        return (alpha_rank <= cutoff).astype(int)

    def _prepare_feature_frame(self, frame: pd.DataFrame, feature_columns: list[str]) -> pd.DataFrame:
        frame = frame.copy()
        if "extra_json" in frame.columns:
            extra_frame = frame["extra_json"].apply(self._safe_json_loads).apply(
                lambda value: value if isinstance(value, dict) else {}
            )
            extra_frame = pd.json_normalize(extra_frame)
            if not extra_frame.empty:
                for column in extra_frame.columns:
                    if column not in frame.columns or frame[column].isna().all():
                        frame[column] = extra_frame[column]

        numeric_seed_columns = {
            "rank",
            "composite_score",
            "value_score",
            "growth_score",
            "quality_score",
            "momentum_score",
            "price",
            "pe",
            "pb",
            "roe",
            "revenue_growth",
            "net_income_growth",
            "momentum_20d",
            "momentum_60d",
            "momentum_120d",
            "market_cap",
            "dividend_yield",
            "gross_margin",
            "debt_ratio",
            "fcf_yield",
            "rsi_14",
            "volume_ratio",
            "avg_amount_20d",
            "avg_turnover_20d",
            "volatility_20d",
            "volatility_60d",
            "max_drawdown_60d",
            "price_vs_ma20",
            "price_vs_ma60",
            "stock_return",
            "benchmark_return",
            "alpha",
            "is_st",
            "is_suspended",
        }
        for column in numeric_seed_columns:
            if column in frame.columns:
                frame[column] = pd.to_numeric(frame[column], errors="coerce")

        if "risk_flags" in frame.columns:
            frame["risk_flag_count"] = frame["risk_flags"].apply(
                lambda value: float(len(value)) if isinstance(value, list) else 0.0
            )
        else:
            frame["risk_flag_count"] = 0.0

        if "rank" in frame.columns:
            max_rank_by_date = (
                frame.groupby("snapshot_date")["rank"].transform("max")
                if "snapshot_date" in frame.columns
                else frame["rank"].max()
            )
            if isinstance(max_rank_by_date, pd.Series):
                frame["rank_pct"] = frame["rank"] / max_rank_by_date.replace({0: pd.NA})
            else:
                frame["rank_pct"] = frame["rank"] / max_rank_by_date if max_rank_by_date else pd.NA
            frame["rank_inverse"] = self._safe_divide(1.0, frame["rank"])

        frame["composite_gap"] = self._series_value(frame, "composite_score") - 50.0
        frame["quality_value_spread"] = self._series_value(frame, "quality_score") - self._series_value(frame, "value_score")
        frame["growth_momentum_blend"] = (
            self._series_value(frame, "growth_score") + self._series_value(frame, "momentum_score")
        ) / 2.0
        frame["value_momentum_blend"] = (
            self._series_value(frame, "value_score") + self._series_value(frame, "momentum_score")
        ) / 2.0

        frame["earnings_yield"] = self._positive_inverse(self._series_value(frame, "pe"), scale=100.0)
        frame["book_to_price"] = self._positive_inverse(self._series_value(frame, "pb"), scale=100.0)
        frame["log_market_cap"] = np.log1p(self._series_value(frame, "market_cap").clip(lower=0))
        frame["fcf_to_book"] = self._safe_divide(
            self._series_value(frame, "fcf_yield"),
            self._series_value(frame, "pb"),
        )

        frame["momentum_accel_20_60"] = self._series_value(frame, "momentum_20d") - self._series_value(frame, "momentum_60d")
        frame["momentum_accel_60_120"] = self._series_value(frame, "momentum_60d") - self._series_value(frame, "momentum_120d")
        frame["trend_strength"] = self._series_value(frame, "price_vs_ma20") + self._series_value(frame, "price_vs_ma60")
        frame["trend_gap"] = self._series_value(frame, "price_vs_ma20") - self._series_value(frame, "price_vs_ma60")
        frame["liquidity_to_cap"] = self._safe_divide(
            self._series_value(frame, "avg_amount_20d"),
            self._series_value(frame, "market_cap"),
        )
        frame["volatility_ratio_20_60"] = self._safe_divide(
            self._series_value(frame, "volatility_20d"),
            self._series_value(frame, "volatility_60d"),
        )
        frame["drawdown_to_vol_60"] = self._safe_divide(
            self._series_value(frame, "max_drawdown_60d").abs(),
            self._series_value(frame, "volatility_60d"),
        )

        sector_group_columns = [column for column in ("snapshot_date", "sector") if column in frame.columns]
        industry_group_columns = [column for column in ("snapshot_date", "industry") if column in frame.columns]
        cross_section_columns = ["snapshot_date"] if "snapshot_date" in frame.columns else []

        frame["sector_size"] = self._group_size(frame, sector_group_columns)
        frame["industry_size"] = self._group_size(frame, industry_group_columns)
        frame["sector_relative_composite"] = self._series_value(frame, "composite_score") - self._group_mean(
            frame, sector_group_columns, "composite_score"
        )
        frame["sector_relative_momentum"] = self._series_value(frame, "momentum_score") - self._group_mean(
            frame, sector_group_columns, "momentum_score"
        )
        frame["sector_relative_value"] = self._series_value(frame, "value_score") - self._group_mean(
            frame, sector_group_columns, "value_score"
        )
        frame["industry_relative_composite"] = self._series_value(frame, "composite_score") - self._group_mean(
            frame, industry_group_columns, "composite_score"
        )
        frame["industry_relative_momentum"] = self._series_value(frame, "momentum_score") - self._group_mean(
            frame, industry_group_columns, "momentum_score"
        )
        frame["sector_rank_pct"] = self._group_rank_pct(frame, sector_group_columns, "composite_score", ascending=False)
        frame["industry_rank_pct"] = self._group_rank_pct(
            frame, industry_group_columns, "composite_score", ascending=False
        )
        frame["market_cap_pct"] = self._group_rank_pct(frame, cross_section_columns, "market_cap", ascending=False)
        frame["earnings_yield_pct"] = self._group_rank_pct(
            frame, cross_section_columns, "earnings_yield", ascending=False
        )
        frame["book_to_price_pct"] = self._group_rank_pct(
            frame, cross_section_columns, "book_to_price", ascending=False
        )
        frame["momentum_120d_pct"] = self._group_rank_pct(
            frame, cross_section_columns, "momentum_120d", ascending=False
        )
        frame["avg_amount_20d_pct"] = self._group_rank_pct(
            frame, cross_section_columns, "avg_amount_20d", ascending=False
        )
        frame["low_volatility_20d_pct"] = self._group_rank_pct(
            frame, cross_section_columns, "volatility_20d", ascending=True
        )

        for column in feature_columns:
            if column not in frame.columns:
                frame[column] = pd.NA

        return frame

    @staticmethod
    def _series_value(frame: pd.DataFrame, column: str) -> pd.Series:
        if column not in frame.columns:
            return pd.Series(np.nan, index=frame.index, dtype="float64")
        return pd.to_numeric(frame[column], errors="coerce")

    @staticmethod
    def _safe_divide(numerator, denominator) -> pd.Series:
        if isinstance(numerator, pd.Series):
            numerator_series = pd.to_numeric(numerator, errors="coerce")
        else:
            denominator_index = denominator.index if isinstance(denominator, pd.Series) else None
            numerator_series = pd.Series(float(numerator), index=denominator_index, dtype="float64")
        denominator_series = pd.to_numeric(denominator, errors="coerce").replace({0: np.nan})
        return numerator_series / denominator_series

    @staticmethod
    def _positive_inverse(series: pd.Series, scale: float = 1.0) -> pd.Series:
        series = pd.to_numeric(series, errors="coerce")
        result = pd.Series(np.nan, index=series.index, dtype="float64")
        mask = series > 0
        result.loc[mask] = float(scale) / series.loc[mask]
        return result

    @staticmethod
    def _group_mean(frame: pd.DataFrame, group_columns: list[str], value_column: str) -> pd.Series:
        if value_column not in frame.columns:
            return pd.Series(np.nan, index=frame.index, dtype="float64")
        valid_groups = [column for column in group_columns if column in frame.columns]
        if not valid_groups:
            return pd.Series(frame[value_column].mean(), index=frame.index, dtype="float64")
        return frame.groupby(valid_groups, dropna=False)[value_column].transform("mean")

    @staticmethod
    def _group_size(frame: pd.DataFrame, group_columns: list[str]) -> pd.Series:
        valid_groups = [column for column in group_columns if column in frame.columns]
        if not valid_groups:
            return pd.Series(float(len(frame)), index=frame.index, dtype="float64")
        anchor_column = "code" if "code" in frame.columns else valid_groups[0]
        return frame.groupby(valid_groups, dropna=False)[anchor_column].transform("size").astype("float64")

    @staticmethod
    def _rank_pct(series: pd.Series, ascending: bool = False) -> pd.Series:
        series = pd.to_numeric(series, errors="coerce")
        result = pd.Series(np.nan, index=series.index, dtype="float64")
        valid = series.notna()
        valid_count = int(valid.sum())
        if valid_count == 0:
            return result
        if valid_count == 1:
            result.loc[valid] = 1.0
            return result
        ranks = series.loc[valid].rank(method="average", ascending=ascending)
        result.loc[valid] = 1.0 - (ranks - 1) / (valid_count - 1)
        return result

    @staticmethod
    def _group_rank_pct(
        frame: pd.DataFrame,
        group_columns: list[str],
        value_column: str,
        ascending: bool = False,
    ) -> pd.Series:
        if value_column not in frame.columns:
            return pd.Series(np.nan, index=frame.index, dtype="float64")
        valid_groups = [column for column in group_columns if column in frame.columns]
        if not valid_groups:
            return ModelEngine._rank_pct(frame[value_column], ascending=ascending)
        return frame.groupby(valid_groups, dropna=False)[value_column].transform(
            lambda series: ModelEngine._rank_pct(series, ascending=ascending)
        )

    @staticmethod
    def _evaluate_predictions(
        frame: pd.DataFrame,
        prediction_column: str,
        target_column: str,
        task_type: str,
    ) -> dict:
        actual = frame[target_column]
        predicted = frame[prediction_column]
        alpha = pd.to_numeric(frame["alpha"], errors="coerce")
        rank_ic = ModelEngine._spearman(alpha, predicted)
        model_top20_alpha = ModelEngine._top_bucket_alpha(frame, prediction_column, 20)
        baseline_top20_alpha = ModelEngine._top_bucket_alpha(frame, "composite_score", 20)
        model_top20_hit_rate = ModelEngine._top_bucket_hit_rate(frame, prediction_column, 20, "alpha")
        baseline_top20_hit_rate = ModelEngine._top_bucket_hit_rate(frame, "composite_score", 20, "alpha")
        metrics = {
            "snapshot_date": str(frame["snapshot_date"].iloc[0]),
            "rows": int(len(frame)),
            "rank_ic": round(rank_ic, 4),
            "model_top20_alpha": round(model_top20_alpha, 4),
            "baseline_top20_alpha": round(baseline_top20_alpha, 4),
            "top20_alpha_lift": round(model_top20_alpha - baseline_top20_alpha, 4),
            "model_top20_hit_rate": round(model_top20_hit_rate, 2),
            "baseline_top20_hit_rate": round(baseline_top20_hit_rate, 2),
            "top20_hit_rate_lift": round(model_top20_hit_rate - baseline_top20_hit_rate, 2),
        }

        if task_type == "classification":
            model_precision = ModelEngine._top_bucket_precision(frame, prediction_column, 20, target_column)
            baseline_precision = ModelEngine._top_bucket_precision(frame, "composite_score", 20, target_column)
            metrics.update(
                {
                    "positive_rate": round(100.0 * float(pd.to_numeric(actual, errors="coerce").mean()), 2),
                    "model_precision_at_20": round(model_precision, 2),
                    "baseline_precision_at_20": round(baseline_precision, 2),
                    "precision_at_20_lift": round(model_precision - baseline_precision, 2),
                }
            )
        else:
            errors = predicted - actual
            mae = float(errors.abs().mean())
            rmse = float((errors.pow(2).mean()) ** 0.5)
            metrics.update(
                {
                    "mae": round(mae, 4),
                    "rmse": round(rmse, 4),
                }
            )

        return metrics

    @staticmethod
    def _aggregate_metrics(folds: list[dict]) -> dict:
        summary = {"fold_count": len(folds), "folds": folds}
        excluded = {"snapshot_date", "rows"}
        metric_keys = sorted({key for fold in folds for key in fold.keys() if key not in excluded})
        for key in metric_keys:
            values = [float(fold[key]) for fold in folds if isinstance(fold.get(key), (int, float))]
            if values:
                summary[key] = round(sum(values) / len(values), 4)
        return summary

    @staticmethod
    def _extract_feature_importance(model, feature_columns: list[str]) -> list[dict]:
        estimator = model.named_steps["model"]
        importances = getattr(estimator, "feature_importances_", None)
        if importances is None:
            return []
        pairs = [
            {"feature": feature, "importance": round(float(value), 6)}
            for feature, value in zip(feature_columns, importances)
        ]
        pairs.sort(key=lambda item: item["importance"], reverse=True)
        return pairs

    def _enrich_registry(self, registry: dict) -> dict:
        parsed = self._parse_registry(registry)
        promotion = None
        if isinstance(parsed.get("extra_json"), dict):
            promotion = parsed["extra_json"].get("promotion")
        if not isinstance(promotion, dict):
            promotion = self._evaluate_promotion(
                parsed.get("metrics_json") or {},
                self._build_promotion_gate(None),
            )
        parsed["promotion"] = promotion
        parsed["serving_ready"] = bool(parsed.get("is_active") and promotion.get("eligible"))
        return parsed

    @staticmethod
    def _top_bucket_alpha(frame: pd.DataFrame, score_column: str, top_n: int) -> float:
        ranked = frame.sort_values(score_column, ascending=False).head(top_n)
        if ranked.empty:
            return 0.0
        return float(ranked["alpha"].mean())

    @staticmethod
    def _top_bucket_hit_rate(frame: pd.DataFrame, score_column: str, top_n: int, target_column: str) -> float:
        ranked = frame.sort_values(score_column, ascending=False).head(top_n)
        if ranked.empty:
            return 0.0
        return 100.0 * float((ranked[target_column] > 0).mean())

    @staticmethod
    def _top_bucket_precision(frame: pd.DataFrame, score_column: str, top_n: int, target_column: str) -> float:
        ranked = frame.sort_values(score_column, ascending=False).head(top_n)
        if ranked.empty:
            return 0.0
        return 100.0 * float((pd.to_numeric(ranked[target_column], errors="coerce") > 0).mean())

    @staticmethod
    def _spearman(actual: pd.Series, predicted: pd.Series) -> float:
        if len(actual) < 2:
            return 0.0
        ranked_actual = actual.rank(method="average")
        ranked_predicted = predicted.rank(method="average")
        corr = ranked_actual.corr(ranked_predicted)
        return float(corr) if pd.notna(corr) else 0.0

    @staticmethod
    def _build_promotion_gate(promotion_gate: dict | None) -> dict:
        gate = dict(DEFAULT_PROMOTION_GATE)
        if isinstance(promotion_gate, dict):
            for key, default_value in gate.items():
                if key in promotion_gate and promotion_gate[key] is not None:
                    gate[key] = float(promotion_gate[key])
                else:
                    gate[key] = float(default_value)
        return gate

    @staticmethod
    def _evaluate_promotion(metrics: dict, gate: dict) -> dict:
        checks = [
            {
                "name": "rank_ic",
                "actual": float(metrics.get("rank_ic", 0.0)),
                "threshold": float(gate["min_rank_ic"]),
                "passed": float(metrics.get("rank_ic", 0.0)) >= float(gate["min_rank_ic"]),
            },
            {
                "name": "top20_alpha_lift",
                "actual": float(metrics.get("top20_alpha_lift", 0.0)),
                "threshold": float(gate["min_top20_alpha_lift"]),
                "passed": float(metrics.get("top20_alpha_lift", 0.0)) >= float(gate["min_top20_alpha_lift"]),
            },
        ]
        if "precision_at_20_lift" in metrics:
            checks.append(
                {
                    "name": "precision_at_20_lift",
                    "actual": float(metrics.get("precision_at_20_lift", 0.0)),
                    "threshold": float(gate["min_precision_at_20_lift"]),
                    "passed": float(metrics.get("precision_at_20_lift", 0.0))
                    >= float(gate["min_precision_at_20_lift"]),
                }
            )
        else:
            checks.append(
                {
                    "name": "top20_hit_rate_lift",
                    "actual": float(metrics.get("top20_hit_rate_lift", 0.0)),
                    "threshold": float(gate["min_hit_rate_lift"]),
                    "passed": float(metrics.get("top20_hit_rate_lift", 0.0))
                    >= float(gate["min_hit_rate_lift"]),
                }
            )
        failed_checks = [check["name"] for check in checks if not check["passed"]]
        return {
            "eligible": not failed_checks,
            "gate": gate,
            "checks": checks,
            "failed_checks": failed_checks,
        }

    @staticmethod
    def _parse_registry(registry: dict) -> dict:
        return {
            **registry,
            "feature_names_json": ModelEngine._safe_json_loads(registry.get("feature_names_json")),
            "metrics_json": ModelEngine._safe_json_loads(registry.get("metrics_json")),
            "extra_json": ModelEngine._safe_json_loads(registry.get("extra_json")),
        }

    @staticmethod
    def _safe_json_loads(value):
        if not value:
            return None
        if isinstance(value, (dict, list)):
            return value
        try:
            return json.loads(value)
        except (TypeError, json.JSONDecodeError):
            return value

    @staticmethod
    def _safe_round(value, digits: int = 4):
        if value is None or pd.isna(value):
            return None
        return round(float(value), digits)

    @staticmethod
    def _json_safe_value(value):
        if value is None:
            return None
        if isinstance(value, (list, dict, str, bool)):
            return value
        try:
            if pd.isna(value):
                return None
        except (TypeError, ValueError):
            pass
        if isinstance(value, np.integer):
            return int(value)
        if isinstance(value, (np.floating, float)):
            if not np.isfinite(value):
                return None
            return float(value)
        return value
