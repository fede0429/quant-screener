from __future__ import annotations

from datetime import datetime
from typing import Dict, List
from uuid import uuid4

from api.models.event_models import PolicyEvent
from api.utils.source_levels import infer_source_level


class PolicyIngestion:
    def parse_records(self, records: List[Dict]) -> List[PolicyEvent]:
        events = []
        for item in records:
            now = datetime.utcnow().isoformat()
            title = item.get("title", "").strip()
            if not title:
                continue
            source_name = item.get("source_name", "gov")
            events.append(PolicyEvent(
                event_id=item.get("event_id") or str(uuid4()),
                source_name=source_name,
                source_level=infer_source_level(source_name),
                publish_time=item.get("publish_time", now),
                ingest_time=item.get("ingest_time", now),
                title=title,
                content=item.get("content", ""),
                direction=item.get("direction", "neutral"),
                strength=float(item.get("strength", 0.0)),
                sectors=list(item.get("sectors", [])),
                symbols=list(item.get("symbols", [])),
                summary=dict(item.get("summary", {})),
                policy_level=item.get("policy_level", ""),
                impact_horizon=item.get("impact_horizon", "swing"),
            ))
        return events
