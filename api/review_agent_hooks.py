from __future__ import annotations

from typing import Dict, List


class ReviewAgentHooks:
    def build_review_payload(
        self,
        proposal: Dict,
        outcome: Dict,
        comparison: Dict | None = None,
    ) -> Dict:
        return {
            "proposal": proposal,
            "outcome": outcome,
            "comparison": comparison or {},
            "review_tags": self._derive_tags(proposal, outcome, comparison or {}),
        }

    def _derive_tags(self, proposal: Dict, outcome: Dict, comparison: Dict) -> List[str]:
        tags: List[str] = []
        if proposal.get("accepted_flag") is False and outcome.get("outcome_label") == "positive":
            tags.append("missed_opportunity")
        if proposal.get("accepted_flag") is True and outcome.get("outcome_label") == "negative":
            tags.append("bad_acceptance")
        if comparison.get("return_diff") is not None and comparison["return_diff"] < -2.0:
            tags.append("execution_worse_than_shadow")
        if comparison.get("return_diff") is not None and comparison["return_diff"] > 2.0:
            tags.append("execution_better_than_shadow")
        return tags
