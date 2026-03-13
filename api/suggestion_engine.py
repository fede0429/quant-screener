from __future__ import annotations

from typing import Dict, List


class SuggestionEngine:
    def build_suggestions(self, review_payload: Dict) -> List[Dict]:
        tags = review_payload.get("review_tags", [])
        proposal = review_payload.get("proposal", {})
        suggestions: List[Dict] = []

        if "missed_opportunity" in tags:
            suggestions.append({
                "type": "decision_rule_adjustment",
                "target": "orchestrator_threshold",
                "action": "consider_relaxing_for_similar_setup",
                "strategy_template": proposal.get("strategy_template"),
            })

        if "bad_acceptance" in tags:
            suggestions.append({
                "type": "risk_rule_adjustment",
                "target": "acceptance_rule",
                "action": "consider_tightening_for_similar_setup",
                "strategy_template": proposal.get("strategy_template"),
            })

        if "execution_worse_than_shadow" in tags:
            suggestions.append({
                "type": "execution_adjustment",
                "target": "auction_or_exit_logic",
                "action": "review_execution_parameters",
                "strategy_template": proposal.get("strategy_template"),
            })

        return suggestions
