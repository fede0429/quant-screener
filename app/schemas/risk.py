from pydantic import BaseModel

class RiskEvaluateRequest(BaseModel):
    proposal_id: str
    portfolio_context: dict
    market_context: dict

class RiskDecisionRead(BaseModel):
    decision_id: str
    proposal_id: str
    decision: str
    reason_codes: list[str]
    approved_weight: float | None = None
    risk_snapshot: dict
    market_state: str | None = None
    reviewer: str

    model_config = {"from_attributes": True}
