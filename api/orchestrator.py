from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Optional
from uuid import uuid4

from api.models.decision_input import DecisionInput
from api.models.trade_intent import TradeIntent
from api.risk_firewall import RiskFirewall


@dataclass
class OrchestratorConfig:
    strategy_name: str = "agent_overlay_v1"
    window_name: str = "tail_session"
    min_final_score: float = 70.0
    base_weight: float = 0.25
    policy_weight: float = 0.20
    event_weight: float = 0.20
    technical_weight: float = 0.30
    intl_weight: float = 0.05
    degrade_on_policy_tech_conflict: bool = True
    conflict_penalty: float = 8.0
    confidence_multiplier: float = 0.01


class Orchestrator:
    def __init__(self, risk_firewall: RiskFirewall, config: Optional[OrchestratorConfig] = None) -> None:
        self.risk_firewall = risk_firewall
        self.config = config or OrchestratorConfig()

    def evaluate(self, item: DecisionInput) -> Dict:
        risk_result = self.risk_firewall.evaluate(item)
        if not risk_result.passed:
            return {"accepted": False, "final_score": 0.0, "risk": risk_result.to_dict(), "conflict": {}, "intent": None, "reason": "risk_veto"}

        raw_score = self._compute_final_score(item)
        conflict = self._resolve_conflict(item)
        final_score = max(0.0, raw_score - conflict.get("penalty", 0.0))

        if final_score < self.config.min_final_score:
            return {"accepted": False, "final_score": final_score, "risk": risk_result.to_dict(), "conflict": conflict, "intent": None, "reason": "score_too_low"}

        intent = self._build_trade_intent(item, final_score)
        return {"accepted": True, "final_score": final_score, "risk": risk_result.to_dict(), "conflict": conflict, "intent": intent.to_dict(), "reason": "ok"}

    def _compute_final_score(self, item: DecisionInput) -> float:
        return round(item.base_selection_score * self.config.base_weight + item.policy_score * self.config.policy_weight +
                     item.event_score * self.config.event_weight + item.technical_score * self.config.technical_weight +
                     item.intl_adjustment * self.config.intl_weight, 4)

    def _resolve_conflict(self, item: DecisionInput) -> Dict:
        penalty = 0.0
        conflict_type = ""
        if self.config.degrade_on_policy_tech_conflict:
            if item.policy_score >= 80 and item.technical_score <= 45:
                penalty += self.config.conflict_penalty
                conflict_type = "policy_strong_technical_weak"
            elif item.technical_score >= 85 and item.event_score <= 35 and item.policy_score <= 35:
                penalty += self.config.conflict_penalty
                conflict_type = "technical_strong_event_policy_weak"
        return {"type": conflict_type, "penalty": penalty}

    def _build_trade_intent(self, item: DecisionInput, final_score: float) -> TradeIntent:
        confidence = min(1.0, max(0.0, final_score * self.config.confidence_multiplier))
        return TradeIntent(
            intent_id=str(uuid4()),
            trade_date=item.trade_date,
            strategy_name=self.config.strategy_name,
            window_name=self.config.window_name,
            code=item.code,
            exchange=item.exchange,
            side="buy",
            thesis=item.thesis,
            base_selection_score=item.base_selection_score,
            policy_score=item.policy_score,
            event_score=item.event_score,
            technical_score=item.technical_score,
            intl_adjustment=item.intl_adjustment,
            final_score=final_score,
            confidence=confidence,
            reference_price=item.reference_price,
            planned_entry_price=item.latest_price,
            planned_limit_price=item.latest_price,
            tags=list(item.tags),
            reason_detail={"extra": dict(item.extra), "risk_flags": list(item.risk_flags)},
        )
