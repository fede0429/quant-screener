from datetime import datetime
from pydantic import BaseModel

class ProposalGenerateRequest(BaseModel):
    run_id: str
    strategy_name: str

class ProposalRead(BaseModel):
    proposal_id: str
    strategy_name: str
    symbol: str
    side: str
    proposal_type: str
    as_of_time: datetime
    thesis: str
    entry_logic: dict
    invalidation_logic: dict
    stop_rule: dict | None = None
    target_rule: dict | None = None
    horizon_days: str | None = None
    confidence: float | None = None
    desired_weight: float | None = None
    max_weight: float | None = None
    urgency: str | None = None
    status: str

    model_config = {"from_attributes": True}
