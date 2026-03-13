from __future__ import annotations

from datetime import datetime
from typing import Dict, List
from uuid import uuid4

from api.models.event_models import AnnouncementEvent
from api.utils.source_levels import infer_source_level


class AnnouncementIngestion:
    def parse_records(self, records: List[Dict]) -> List[AnnouncementEvent]:
        events = []
        for item in records:
            now = datetime.utcnow().isoformat()
            title = item.get("title", "").strip()
            code = item.get("code", "").strip()
            if not title or not code:
                continue
            source_name = item.get("source_name", "cninfo")
            events.append(AnnouncementEvent(
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
                symbols=list(item.get("symbols", [code])),
                summary=dict(item.get("summary", {})),
                exchange=item.get("exchange", ""),
                code=code,
                company_name=item.get("company_name", ""),
                announcement_type=item.get("announcement_type", ""),
                tradable_flag=bool(item.get("tradable_flag", False)),
            ))
        return events
