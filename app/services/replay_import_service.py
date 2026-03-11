from __future__ import annotations

from uuid import uuid4
from datetime import datetime

from app.models.audit_event import AuditEvent


class ReplayImportService:
    def __init__(self, db):
        self.db = db

    def record_imported_run(self, run_id: str, strategy_name: str, symbols: list[str]) -> None:
        event = AuditEvent(
            event_id=str(uuid4()),
            event_time=datetime.utcnow(),
            event_type="portfolio_run_imported",
            actor="system",
            ref_type="portfolio_run",
            ref_id=run_id,
            payload={
                "strategy_name": strategy_name,
                "symbols": symbols,
                "source": "adapter_import",
            },
        )
        self.db.add(event)
        self.db.commit()
