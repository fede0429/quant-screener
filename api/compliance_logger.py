from __future__ import annotations

from typing import Any, Dict, List
from uuid import uuid4

from api.models.compliance_models import DecisionLog, OrderLog, RiskIncidentLog


class ComplianceLogger:
    def __init__(self) -> None:
        self.decision_logs: List[DecisionLog] = []
        self.order_logs: List[OrderLog] = []
        self.risk_logs: List[RiskIncidentLog] = []

    def log_decision(self, trade_date: str, code: str, stage: str, decision_reason: str, final_score: float,
                     accepted: bool, payload: Dict[str, Any]) -> Dict[str, Any]:
        log = DecisionLog(str(uuid4()), trade_date, code, stage, decision_reason, final_score, accepted, payload)
        self.decision_logs.append(log)
        return log.to_dict()

    def log_order(self, trade_date: str, code: str, action: str, status: str, order_payload: Dict[str, Any],
                  result_payload: Dict[str, Any]) -> Dict[str, Any]:
        log = OrderLog(str(uuid4()), trade_date, code, action, status, order_payload, result_payload)
        self.order_logs.append(log)
        return log.to_dict()

    def log_risk_incident(self, trade_date: str, incident_type: str, severity: str, code: str, reason: str,
                          payload: Dict[str, Any]) -> Dict[str, Any]:
        log = RiskIncidentLog(str(uuid4()), trade_date, incident_type, severity, code, reason, payload)
        self.risk_logs.append(log)
        return log.to_dict()

    def dump(self) -> Dict[str, Any]:
        return {"decision_logs": [x.to_dict() for x in self.decision_logs],
                "order_logs": [x.to_dict() for x in self.order_logs],
                "risk_logs": [x.to_dict() for x in self.risk_logs]}
