from sqlalchemy import Column, String, DateTime, JSON
from app.db.session import Base

class AuditEvent(Base):
    __tablename__ = "audit_events"
    event_id = Column(String, primary_key=True)
    event_time = Column(DateTime, nullable=False, index=True)
    event_type = Column(String(64), nullable=False, index=True)
    actor = Column(String(64), nullable=False)
    ref_type = Column(String(64), nullable=False, index=True)
    ref_id = Column(String(128), nullable=False, index=True)
    payload = Column(JSON, nullable=False)
