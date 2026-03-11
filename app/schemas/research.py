from datetime import date
from pydantic import BaseModel

class ResearchFundamentalRequest(BaseModel):
    symbol: str
    as_of_date: date

class ResearchReportRead(BaseModel):
    report_id: str
    symbol: str
    as_of_date: date
    report_type: str
    fundamental_score: float | None = None
    valuation_percentile: float | None = None
    decision_label: str | None = None
    confidence: float | None = None
    summary: str | None = None
    payload: dict

    model_config = {"from_attributes": True}
