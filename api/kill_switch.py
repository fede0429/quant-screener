from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict


@dataclass
class KillSwitchState:
    enabled: bool = False
    reason: str = ""
    trigger_time: str = ""
    resume_allowed: bool = False
    payload: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {"enabled": self.enabled, "reason": self.reason, "trigger_time": self.trigger_time,
                "resume_allowed": self.resume_allowed, "payload": dict(self.payload)}


class KillSwitchManager:
    def __init__(self) -> None:
        self.state = KillSwitchState()

    def trigger(self, reason: str, payload: Dict[str, Any] | None = None) -> Dict[str, Any]:
        self.state.enabled = True
        self.state.reason = reason
        self.state.trigger_time = datetime.utcnow().isoformat()
        self.state.resume_allowed = False
        self.state.payload = payload or {}
        return self.state.to_dict()

    def allow_resume(self, payload: Dict[str, Any] | None = None) -> Dict[str, Any]:
        self.state.resume_allowed = True
        if payload:
            self.state.payload.update(payload)
        return self.state.to_dict()

    def resume(self, reason: str = "manual_resume") -> Dict[str, Any]:
        if not self.state.resume_allowed:
            raise RuntimeError("resume_not_allowed")
        self.state.enabled = False
        self.state.reason = reason
        self.state.resume_allowed = False
        self.state.payload = {}
        return self.state.to_dict()

    def is_blocking(self) -> bool:
        return self.state.enabled
