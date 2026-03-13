from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class TradeIntent:
    intent_id: str
    trade_date: str
    strategy_name: str
    window_name: str
    code: str
    exchange: str
    side: str = "buy"
    thesis: str = ""
    base_selection_score: float = 0.0
    policy_score: float = 0.0
    event_score: float = 0.0
    technical_score: float = 0.0
    intl_adjustment: float = 0.0
    final_score: float = 0.0
    confidence: float = 0.0
    reference_price: Optional[float] = None
    planned_entry_price: Optional[float] = None
    planned_limit_price: Optional[float] = None
    stop_loss_price: Optional[float] = None
    take_profit_prices: List[float] = field(default_factory=list)
    abandon_flag: bool = False
    abandon_reason: str = ""
    tags: List[str] = field(default_factory=list)
    reason_detail: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "intent_id": self.intent_id,
            "trade_date": self.trade_date,
            "strategy_name": self.strategy_name,
            "window_name": self.window_name,
            "code": self.code,
            "exchange": self.exchange,
            "side": self.side,
            "thesis": self.thesis,
            "base_selection_score": self.base_selection_score,
            "policy_score": self.policy_score,
            "event_score": self.event_score,
            "technical_score": self.technical_score,
            "intl_adjustment": self.intl_adjustment,
            "final_score": self.final_score,
            "confidence": self.confidence,
            "reference_price": self.reference_price,
            "planned_entry_price": self.planned_entry_price,
            "planned_limit_price": self.planned_limit_price,
            "stop_loss_price": self.stop_loss_price,
            "take_profit_prices": list(self.take_profit_prices),
            "abandon_flag": self.abandon_flag,
            "abandon_reason": self.abandon_reason,
            "tags": list(self.tags),
            "reason_detail": dict(self.reason_detail),
        }
