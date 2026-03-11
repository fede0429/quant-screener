from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from app.db.session import get_db
from app.models.audit_event import AuditEvent

router = APIRouter(prefix="/replay", tags=["replay"])

@router.get("/{ref_type}/{ref_id}")
def get_replay(ref_type: str, ref_id: str, db: Session = Depends(get_db)):
    events = db.query(AuditEvent).filter(
        AuditEvent.ref_type == ref_type,
        AuditEvent.ref_id == ref_id
    ).order_by(AuditEvent.event_time.asc()).all()
    return {
        "ref_type": ref_type,
        "ref_id": ref_id,
        "timeline": [
            {
                "event_type": e.event_type,
                "event_time": e.event_time.isoformat() if e.event_time else None,
                "actor": e.actor,
                "payload": e.payload,
            } for e in events
        ],
    }
