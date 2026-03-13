from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Optional

from api.models.trade_intent import TradeIntent
from api.utils.exchange_rules import get_auction_rule


@dataclass
class ExecutionDecision:
    executable: bool
    reason: str
    order_payload: Optional[Dict] = None


class AuctionExecutor:
    def build_order(self, intent: TradeIntent, quantity: int) -> ExecutionDecision:
        if intent.abandon_flag:
            return ExecutionDecision(False, f"intent_abandoned:{intent.abandon_reason}", None)
        if quantity <= 0:
            return ExecutionDecision(False, "invalid_quantity", None)
        if intent.planned_limit_price is None:
            return ExecutionDecision(False, "missing_limit_price", None)

        rule = get_auction_rule(intent.exchange)
        payload = {
            "code": intent.code,
            "exchange": rule.exchange,
            "side": intent.side,
            "order_type": "limit",
            "price": intent.planned_limit_price,
            "quantity": quantity,
            "auction_window": {"start": rule.auction_start, "end": rule.auction_end, "cancel_allowed": rule.cancel_allowed_in_auction},
            "meta": {"intent_id": intent.intent_id, "strategy_name": intent.strategy_name, "window_name": intent.window_name,
                     "final_score": intent.final_score, "confidence": intent.confidence},
        }
        return ExecutionDecision(True, "ok", payload)
