from __future__ import annotations

from datetime import datetime
from typing import Dict, List
from uuid import uuid4

from api.models.event_models import MarketNewsEvent
from api.utils.source_levels import infer_source_level


class NewsIngestion:
    def parse_records(self, records: List[Dict]) -> List[MarketNewsEvent]:
        events = []
        for item in records:
            now = datetime.utcnow().isoformat()
            title = item.get("title", "").strip()
            if not title:
                continue
            source_name = item.get("source_name", "cls")
            events.append(MarketNewsEvent(
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
                event_type=item.get("event_type", ""),
                heat_score=float(item.get("heat_score", 0.0)),
                diffusion_score=float(item.get("diffusion_score", 0.0)),
            ))
        return events
