from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List


@dataclass
class RiskResult:
    passed: bool
    veto_reason: str = ""
    warnings: List[str] = field(default_factory=list)
    detail: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "passed": self.passed,
            "veto_reason": self.veto_reason,
            "warnings": list(self.warnings),
            "detail": dict(self.detail),
        }
