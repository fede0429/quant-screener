from __future__ import annotations

OPEN_HIGH = "OPEN_HIGH"
OPEN_FLAT = "OPEN_FLAT"
OPEN_LOW = "OPEN_LOW"
LIMIT_UP = "LIMIT_UP"
LIMIT_DOWN = "LIMIT_DOWN"

def classify_open_state(prev_close: float, open_price: float, up_limit_price: float | None = None,
                        down_limit_price: float | None = None, high_threshold: float = 0.02,
                        low_threshold: float = -0.02) -> str:
    if prev_close is None or prev_close <= 0 or open_price is None or open_price <= 0:
        raise ValueError("invalid prev_close/open_price")
    if up_limit_price is not None and abs(open_price - up_limit_price) < 1e-9:
        return LIMIT_UP
    if down_limit_price is not None and abs(open_price - down_limit_price) < 1e-9:
        return LIMIT_DOWN
    gap = (open_price - prev_close) / prev_close
    if gap >= high_threshold:
        return OPEN_HIGH
    if gap <= low_threshold:
        return OPEN_LOW
    return OPEN_FLAT
