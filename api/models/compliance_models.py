from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict


@dataclass
class DecisionLog:
    log_id: str
    trade_date: str
    code: str
    stage: str
    decision_reason: str
    final_score: float = 0.0
    accepted: bool = False
    payload: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {"log_id": self.log_id, "trade_date": self.trade_date, "code": self.code, "stage": self.stage,
                "decision_reason": self.decision_reason, "final_score": self.final_score, "accepted": self.accepted,
                "payload": dict(self.payload)}


@dataclass
class OrderLog:
    log_id: str
    trade_date: str
    code: str
    action: str
    status: str
    order_payload: Dict[str, Any] = field(default_factory=dict)
    result_payload: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {"log_id": self.log_id, "trade_date": self.trade_date, "code": self.code, "action": self.action,
                "status": self.status, "order_payload": dict(self.order_payload), "result_payload": dict(self.result_payload)}


@dataclass
class RiskIncidentLog:
    log_id: str
    trade_date: str
    incident_type: str
    severity: str
    code: str = ""
    reason: str = ""
    payload: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {"log_id": self.log_id, "trade_date": self.trade_date, "incident_type": self.incident_type,
                "severity": self.severity, "code": self.code, "reason": self.reason, "payload": dict(self.payload)}


@dataclass
class ManualOverrideRecord:
    override_id: str
    override_time: str
    operator: str
    override_type: str
    related_code: str = ""
    related_intent_id: str = ""
    related_order_id: str = ""
    reason: str = ""
    payload: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {"override_id": self.override_id, "override_time": self.override_time, "operator": self.operator,
                "override_type": self.override_type, "related_code": self.related_code,
                "related_intent_id": self.related_intent_id, "related_order_id": self.related_order_id,
                "reason": self.reason, "payload": dict(self.payload)}
