from __future__ import annotations

from typing import Dict, List

from api.agent_weight_adjustor import AgentWeightAdjustor
from api.strategy_template_stats import StrategyTemplateStats
from api.suggestion_engine import SuggestionEngine


class ShadowSuggestionRuntime:
    def __init__(self) -> None:
        self.suggestion_engine = SuggestionEngine()
        self.weight_adjustor = AgentWeightAdjustor()
        self.template_stats = StrategyTemplateStats()

    def run_one(self, review_payload: Dict) -> Dict:
        suggestions = self.suggestion_engine.build_suggestions(review_payload)
        weight_delta = self.weight_adjustor.suggest_weight_delta(
            proposal=review_payload.get("proposal", {}),
            outcome=review_payload.get("outcome", {}),
        )
        return {
            "suggestions": suggestions,
            "weight_delta": weight_delta,
        }

    def summarize_many(self, review_payloads: List[Dict]) -> Dict:
        return {
            "template_stats": self.template_stats.summarize(review_payloads),
        }
