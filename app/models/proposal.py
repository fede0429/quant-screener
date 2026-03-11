from sqlalchemy import Column, String, DateTime, Text, Float, JSON
from app.db.session import Base

class Proposal(Base):
    __tablename__ = "proposals"
    proposal_id = Column(String, primary_key=True)
    strategy_name = Column(String(128), nullable=False)
    symbol = Column(String(32), nullable=False, index=True)
    side = Column(String(16), nullable=False)
    proposal_type = Column(String(32), nullable=False)
    as_of_time = Column(DateTime, nullable=False)
    thesis = Column(Text, nullable=False)
    entry_logic = Column(JSON, nullable=False)
    invalidation_logic = Column(JSON, nullable=False)
    stop_rule = Column(JSON)
    target_rule = Column(JSON)
    horizon_days = Column(String(16))
    confidence = Column(Float)
    desired_weight = Column(Float)
    max_weight = Column(Float)
    urgency = Column(String(16))
    status = Column(String(32), nullable=False)
