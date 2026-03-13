from __future__ import annotations

from typing import Dict, List


class AgentWeightAdjustor:
    def suggest_weight_delta(self, proposal: Dict, outcome: Dict) -> Dict:
        agent_group = proposal.get("agent_group", "unknown")
        label = outcome.get("outcome_label", "neutral")

        delta = 0.0
        if label == "positive":
            delta = 0.02
        elif label == "negative":
            delta = -0.02

        return {
            "agent_group": agent_group,
            "suggested_weight_delta": delta,
            "reason": f"outcome_label={label}",
        }
