from __future__ import annotations

"""
Base Screener 真实接入桥接层

作用：
- 接受现有 quant-screener 的真实候选结果
- 适配为 Agent 主链路可消费的数据
"""

from typing import Any, Dict, List, Optional

from api.base_screener_adapter import BaseScreenerAdapter
from api.pipeline_stage1 import Stage1Pipeline
from api.compliance_logger import ComplianceLogger


class BaseScreenerRuntimeBridge:
    def __init__(
        self,
        pipeline: Stage1Pipeline,
        compliance_logger: ComplianceLogger,
        adapter: Optional[BaseScreenerAdapter] = None,
    ) -> None:
        self.pipeline = pipeline
        self.compliance_logger = compliance_logger
        self.adapter = adapter or BaseScreenerAdapter()

    def run_from_rows(
        self,
        trade_date: str,
        next_trade_date: str,
        base_rows: List[Dict[str, Any]],
        quantity: int = 100,
        score_inputs: Optional[Dict[str, Dict[str, float]]] = None,
    ) -> Dict[str, Any]:
        """
        score_inputs 格式：
        {
            "000001.SZ": {
                "policy_score": 80,
                "event_score": 75,
                "technical_score": 90,
                "intl_adjustment": 50
            }
        }
        """
        score_inputs = score_inputs or {}
        candidates = self.adapter.normalize_candidates(base_rows)
        buckets = self.adapter.split_buckets(candidates)

        results = []
        for candidate in buckets["core"]:
            code_scores = score_inputs.get(candidate.code, {})
            decision_input = self.adapter.to_decision_input(
                trade_date=trade_date,
                candidate=candidate,
                policy_score=float(code_scores.get("policy_score", 0.0)),
                event_score=float(code_scores.get("event_score", 0.0)),
                technical_score=float(code_scores.get("technical_score", 0.0)),
                intl_adjustment=float(code_scores.get("intl_adjustment", 0.0)),
                thesis="runtime_base_screener_bridge",
                reference_price=code_scores.get("reference_price"),
                latest_price=code_scores.get("latest_price"),
                extra={"runtime_source": "base_screener_real"},
            )
            result = self.pipeline.run_from_decision_input(
                item=decision_input,
                quantity=quantity,
                next_trade_date=next_trade_date,
            )

            self.compliance_logger.log_decision(
                trade_date=trade_date,
                code=candidate.code,
                stage="runtime_bridge",
                decision_reason="accepted" if result.get("accepted") else "rejected",
                final_score=float(result.get("decision", {}).get("final_score", 0.0) or 0.0),
                accepted=bool(result.get("accepted", False)),
                payload=result,
            )
            results.append(result)

        return {
            "trade_date": trade_date,
            "next_trade_date": next_trade_date,
            "core_count": len(buckets["core"]),
            "watch_count": len(buckets["watch"]),
            "blocked_count": len(buckets["blocked"]),
            "results": results,
        }
