from __future__ import annotations

from typing import Dict


class ProposalOutcomeEvaluator:
    def evaluate(self, proposal: Dict, shadow_result: Dict) -> Dict:
        if not shadow_result.get("replayed"):
            return {
                "proposal_id": proposal.get("proposal_id"),
                "evaluated": False,
                "reason": shadow_result.get("reason", "shadow_not_replayed"),
            }

        t1 = shadow_result.get("return_t1")
        t3 = shadow_result.get("return_t3")
        t5 = shadow_result.get("return_t5")

        label = "neutral"
        if t3 is not None:
            if t3 >= 2.0:
                label = "positive"
            elif t3 <= -2.0:
                label = "negative"

        return {
            "proposal_id": proposal.get("proposal_id"),
            "evaluated": True,
            "outcome_label": label,
            "return_t1": t1,
            "return_t3": t3,
            "return_t5": t5,
            "mfe": shadow_result.get("max_favorable_excursion"),
            "mae": shadow_result.get("max_adverse_excursion"),
            "hit_stop_flag": shadow_result.get("hit_stop_flag", False),
            "hit_tp_flag": shadow_result.get("hit_tp_flag", False),
        }
