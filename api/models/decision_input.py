from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class DecisionInput:
    trade_date: str
    code: str
    exchange: str
    base_selection_score: float = 0.0
    policy_score: float = 0.0
    event_score: float = 0.0
    technical_score: float = 0.0
    intl_adjustment: float = 0.0
    thesis: str = ""
    tags: List[str] = field(default_factory=list)
    reference_price: Optional[float] = None
    latest_price: Optional[float] = None
    risk_flags: List[str] = field(default_factory=list)
    extra: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "trade_date": self.trade_date,
            "code": self.code,
            "exchange": self.exchange,
            "base_selection_score": self.base_selection_score,
            "policy_score": self.policy_score,
            "event_score": self.event_score,
            "technical_score": self.technical_score,
            "intl_adjustment": self.intl_adjustment,
            "thesis": self.thesis,
            "tags": list(self.tags),
            "reference_price": self.reference_price,
            "latest_price": self.latest_price,
            "risk_flags": list(self.risk_flags),
            "extra": dict(self.extra),
        }
