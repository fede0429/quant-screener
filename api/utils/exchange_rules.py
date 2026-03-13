from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ExchangeAuctionRule:
    exchange: str
    auction_start: str
    auction_end: str
    cancel_allowed_in_auction: bool
    note: str


SSE_RULE = ExchangeAuctionRule("SSE", "14:57", "15:00", True, "上交所常规股票收盘集合竞价窗口。")
SZSE_RULE = ExchangeAuctionRule("SZSE", "14:57", "15:00", False, "深交所收盘集合竞价阶段不接受撤销参与竞价交易的申报。")

def normalize_exchange(exchange: str) -> str:
    value = (exchange or "").strip().upper()
    if value in {"SH", "SSE", "XSHG"}:
        return "SSE"
    if value in {"SZ", "SZSE", "XSHE"}:
        return "SZSE"
    raise ValueError(f"Unsupported exchange: {exchange}")

def get_auction_rule(exchange: str) -> ExchangeAuctionRule:
    normalized = normalize_exchange(exchange)
    return SSE_RULE if normalized == "SSE" else SZSE_RULE

def should_force_abandon_on_last_minute_spike(exchange: str, reference_price: float, latest_price: float, max_spike_ratio: float) -> bool:
    if reference_price is None or latest_price is None or reference_price <= 0:
        return True
    ratio = (latest_price - reference_price) / reference_price
    return ratio > max_spike_ratio
