from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List


@dataclass
class BaseCandidate:
    code: str
    exchange: str
    name: str = ""
    sector: str = ""
    industry: str = ""
    base_selection_score: float = 0.0
    model_score: float = 0.0
    composite_score: float = 0.0
    risk_flags: List[str] = field(default_factory=list)
    tags: List[str] = field(default_factory=list)
    extra: Dict[str, Any] = field(default_factory=dict)
    candidate_bucket: str = "watch"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "code": self.code,
            "exchange": self.exchange,
            "name": self.name,
            "sector": self.sector,
            "industry": self.industry,
            "base_selection_score": self.base_selection_score,
            "model_score": self.model_score,
            "composite_score": self.composite_score,
            "risk_flags": list(self.risk_flags),
            "tags": list(self.tags),
            "extra": dict(self.extra),
            "candidate_bucket": self.candidate_bucket,
        }
