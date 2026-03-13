from __future__ import annotations

"""
真实技术评分桥接层

用途：
- 把真实技术面特征聚合成 technical_score
- 与已有 score_inputs 合并
- 调用 BaseScreenerRuntimeBridge
"""

from typing import Any, Dict, List, Optional

from api.base_screener_runtime_bridge import BaseScreenerRuntimeBridge
from api.technical_score_aggregator import TechnicalScoreAggregator


class TechnicalScoreBridge:
    def __init__(
        self,
        runtime_bridge: BaseScreenerRuntimeBridge,
        aggregator: Optional[TechnicalScoreAggregator] = None,
    ) -> None:
        self.runtime_bridge = runtime_bridge
        self.aggregator = aggregator or TechnicalScoreAggregator()

    def run_with_technical_features(
        self,
        trade_date: str,
        next_trade_date: str,
        base_rows: List[Dict[str, Any]],
        technical_rows: List[Dict[str, Any]],
        existing_score_inputs: Optional[Dict[str, Dict[str, float]]] = None,
        quantity: int = 100,
    ) -> Dict[str, Any]:
        technical_scores = self.aggregator.aggregate_for_symbols(technical_rows)
        merged_scores: Dict[str, Dict[str, float]] = {}
        existing_score_inputs = existing_score_inputs or {}

        all_codes = set(existing_score_inputs.keys()) | set(technical_scores.keys())
        for code in all_codes:
            merged_scores[code] = {
                "policy_score": float(existing_score_inputs.get(code, {}).get("policy_score", 0.0)),
                "event_score": float(existing_score_inputs.get(code, {}).get("event_score", 0.0)),
                "technical_score": float(technical_scores.get(code, {}).get("technical_score", 0.0)),
                "intl_adjustment": float(existing_score_inputs.get(code, {}).get("intl_adjustment", 0.0)),
                "reference_price": existing_score_inputs.get(code, {}).get("reference_price"),
                "latest_price": existing_score_inputs.get(code, {}).get("latest_price"),
            }

        return self.runtime_bridge.run_from_rows(
            trade_date=trade_date,
            next_trade_date=next_trade_date,
            base_rows=base_rows,
            quantity=quantity,
            score_inputs=merged_scores,
        )
