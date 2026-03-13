from __future__ import annotations

from typing import Dict, List


class StrategyTemplateStats:
    def summarize(self, review_payloads: List[Dict]) -> Dict:
        stats: Dict[str, Dict] = {}

        for payload in review_payloads:
            proposal = payload.get("proposal", {})
            outcome = payload.get("outcome", {})
            tpl = proposal.get("strategy_template", "unknown")
            stats.setdefault(tpl, {
                "count": 0,
                "positive": 0,
                "negative": 0,
                "neutral": 0,
            })
            stats[tpl]["count"] += 1
            label = outcome.get("outcome_label", "neutral")
            if label in {"positive", "negative", "neutral"}:
                stats[tpl][label] += 1

        return stats
