from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from api.models.trade_intent import TradeIntent
from api.utils.exchange_rules import should_force_abandon_on_last_minute_spike


@dataclass
class TailSessionConfig:
    strategy_name: str = "tail_session_v1"
    window_name: str = "tail_session"
    min_final_score: float = 70.0
    max_candidates: int = 10
    szse_max_spike_ratio: float = 0.008
    sse_max_spike_ratio: float = 0.010
    default_confidence_multiplier: float = 0.01


class TailSessionEngine:
    def __init__(self, config: Optional[TailSessionConfig] = None) -> None:
        self.config = config or TailSessionConfig()

    def apply_tail_guard(self, intent: TradeIntent, latest_price: Optional[float]) -> TradeIntent:
        max_spike_ratio = self.config.szse_max_spike_ratio if intent.exchange.upper() in {"SZ", "SZSE", "XSHE"} else self.config.sse_max_spike_ratio
        abandon_flag = should_force_abandon_on_last_minute_spike(intent.exchange, intent.reference_price, latest_price, max_spike_ratio)
        intent.planned_entry_price = latest_price
        if intent.reference_price and latest_price:
            safe_cap = intent.reference_price * (1.0 + max_spike_ratio)
            intent.planned_limit_price = round(min(latest_price, safe_cap), 3)
        if abandon_flag:
            intent.abandon_flag = True
            intent.abandon_reason = "last_minute_price_spike"
        return intent
