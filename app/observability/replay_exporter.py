from app.models.audit_event import AuditEvent

class ReplayExporter:
    def __init__(self, db):
        self.db = db

    def export(self, ref_type: str, ref_id: str) -> dict:
        events = (
            self.db.query(AuditEvent)
            .filter(AuditEvent.ref_type == ref_type, AuditEvent.ref_id == ref_id)
            .order_by(AuditEvent.event_time.asc())
            .all()
        )
        return {
            "ref_type": ref_type,
            "ref_id": ref_id,
            "count": len(events),
            "timeline": [
                {
                    "event_type": e.event_type,
                    "event_time": e.event_time.isoformat() if e.event_time else None,
                    "actor": e.actor,
                    "payload": e.payload,
                }
                for e in events
            ],
        }
