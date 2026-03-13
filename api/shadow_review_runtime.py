from __future__ import annotations

from typing import Dict

from api.proposal_outcome_evaluator import ProposalOutcomeEvaluator
from api.real_trade_shadow_comparator import RealTradeVsShadowComparator
from api.review_agent_hooks import ReviewAgentHooks


class ShadowReviewRuntime:
    def __init__(self) -> None:
        self.evaluator = ProposalOutcomeEvaluator()
        self.comparator = RealTradeVsShadowComparator()
        self.review_hooks = ReviewAgentHooks()

    def review_one(
        self,
        proposal: Dict,
        shadow_result: Dict,
        real_trade: Dict | None = None,
    ) -> Dict:
        outcome = self.evaluator.evaluate(proposal, shadow_result)
        comparison = None
        if real_trade is not None:
            comparison = self.comparator.compare(real_trade, shadow_result)
        payload = self.review_hooks.build_review_payload(
            proposal=proposal,
            outcome=outcome,
            comparison=comparison,
        )
        return {
            "outcome": outcome,
            "comparison": comparison,
            "review_payload": payload,
        }
