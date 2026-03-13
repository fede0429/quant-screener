from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List
from uuid import uuid4

from api.models.compliance_models import ManualOverrideRecord


class ManualOverrideManager:
    def __init__(self) -> None:
        self.records: List[ManualOverrideRecord] = []

    def create_override(self, operator: str, override_type: str, reason: str, related_code: str = "",
                        related_intent_id: str = "", related_order_id: str = "",
                        payload: Dict[str, Any] | None = None) -> Dict[str, Any]:
        record = ManualOverrideRecord(
            override_id=str(uuid4()), override_time=datetime.utcnow().isoformat(), operator=operator,
            override_type=override_type, related_code=related_code, related_intent_id=related_intent_id,
            related_order_id=related_order_id, reason=reason, payload=payload or {}
        )
        self.records.append(record)
        return record.to_dict()

    def list_records(self) -> List[Dict[str, Any]]:
        return [x.to_dict() for x in self.records]
