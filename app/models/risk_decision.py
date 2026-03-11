from sqlalchemy import Column, String, Float, JSON
from app.db.session import Base

class RiskDecision(Base):
    __tablename__ = "risk_decisions"
    decision_id = Column(String, primary_key=True)
    proposal_id = Column(String, nullable=False, index=True)
    decision = Column(String(32), nullable=False)
    reason_codes = Column(JSON, nullable=False)
    approved_weight = Column(Float)
    risk_snapshot = Column(JSON, nullable=False)
    market_state = Column(String(32))
    reviewer = Column(String(64), nullable=False)
