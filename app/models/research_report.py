from sqlalchemy import Column, String, Date, Text, Float, JSON
from app.db.session import Base

class ResearchReport(Base):
    __tablename__ = "research_reports"
    report_id = Column(String, primary_key=True)
    symbol = Column(String(32), nullable=False, index=True)
    as_of_date = Column(Date, nullable=False, index=True)
    report_type = Column(String(32), nullable=False)
    fundamental_score = Column(Float)
    valuation_percentile = Column(Float)
    decision_label = Column(String(32))
    confidence = Column(Float)
    summary = Column(Text)
    payload = Column(JSON, nullable=False)
