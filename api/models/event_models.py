from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List


@dataclass
class BaseEvent:
    event_id: str
    source_name: str
    source_level: str
    publish_time: str
    ingest_time: str
    title: str
    content: str
    direction: str = "neutral"
    strength: float = 0.0
    sectors: List[str] = field(default_factory=list)
    symbols: List[str] = field(default_factory=list)
    summary: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "event_id": self.event_id,
            "source_name": self.source_name,
            "source_level": self.source_level,
            "publish_time": self.publish_time,
            "ingest_time": self.ingest_time,
            "title": self.title,
            "content": self.content,
            "direction": self.direction,
            "strength": self.strength,
            "sectors": list(self.sectors),
            "symbols": list(self.symbols),
            "summary": dict(self.summary),
            "event_class": self.__class__.__name__,
        }


@dataclass
class PolicyEvent(BaseEvent):
    policy_level: str = ""
    impact_horizon: str = "swing"


@dataclass
class AnnouncementEvent(BaseEvent):
    exchange: str = ""
    code: str = ""
    company_name: str = ""
    announcement_type: str = ""
    tradable_flag: bool = False


@dataclass
class MarketNewsEvent(BaseEvent):
    event_type: str = ""
    heat_score: float = 0.0
    diffusion_score: float = 0.0
