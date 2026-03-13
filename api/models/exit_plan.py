from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List


@dataclass
class ExitAction:
    action_type: str
    trigger_desc: str
    qty_ratio: float
    price_rule: str
    note: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "action_type": self.action_type,
            "trigger_desc": self.trigger_desc,
            "qty_ratio": self.qty_ratio,
            "price_rule": self.price_rule,
            "note": self.note,
        }


@dataclass
class ExitPlan:
    plan_id: str
    intent_id: str
    code: str
    trade_date: str
    next_trade_date: str
    high_open_plan: List[ExitAction] = field(default_factory=list)
    flat_open_plan: List[ExitAction] = field(default_factory=list)
    low_open_plan: List[ExitAction] = field(default_factory=list)
    limit_up_plan: List[ExitAction] = field(default_factory=list)
    limit_down_plan: List[ExitAction] = field(default_factory=list)
    override_rules: List[Dict[str, Any]] = field(default_factory=list)
    meta: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "plan_id": self.plan_id,
            "intent_id": self.intent_id,
            "code": self.code,
            "trade_date": self.trade_date,
            "next_trade_date": self.next_trade_date,
            "high_open_plan": [x.to_dict() for x in self.high_open_plan],
            "flat_open_plan": [x.to_dict() for x in self.flat_open_plan],
            "low_open_plan": [x.to_dict() for x in self.low_open_plan],
            "limit_up_plan": [x.to_dict() for x in self.limit_up_plan],
            "limit_down_plan": [x.to_dict() for x in self.limit_down_plan],
            "override_rules": list(self.override_rules),
            "meta": dict(self.meta),
        }
