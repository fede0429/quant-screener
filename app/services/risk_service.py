from uuid import uuid4
from datetime import datetime
from app.models.proposal import Proposal
from app.models.risk_decision import RiskDecision
from app.models.audit_event import AuditEvent
from app.core.policy_engine import PolicyEngine

class RiskService:
    def __init__(self, db):
        self.db = db
        self.policy_engine = PolicyEngine({"max_position_weight": 0.08, "max_sector_weight": 0.25})

    def evaluate(self, proposal_id, portfolio_context, market_context):
        proposal = self.db.query(Proposal).filter(Proposal.proposal_id == proposal_id).first()
        if not proposal:
            raise ValueError("proposal not found")
        result = self.policy_engine.evaluate(
            {"desired_weight": proposal.desired_weight or 0.0},
            portfolio_context,
            market_context,
        )
        rd = RiskDecision(
            decision_id=str(uuid4()),
            proposal_id=proposal.proposal_id,
            decision=result.decision,
            reason_codes=result.reasons,
            approved_weight=result.approved_weight,
            risk_snapshot={"portfolio_context": portfolio_context, "market_context": market_context},
            market_state=market_context.get("market_state", "normal"),
            reviewer="system",
        )
        proposal.status = "approved" if result.decision in {"approve", "degrade"} else "rejected"
        self.db.add(rd)
        self.db.add(AuditEvent(
            event_id=str(uuid4()),
            event_time=datetime.utcnow(),
            event_type="risk_decision_created",
            actor="system",
            ref_type="proposal",
            ref_id=proposal.proposal_id,
            payload={"decision_id": rd.decision_id, "decision": rd.decision},
        ))
        self.db.commit()
        self.db.refresh(rd)
        return rd
