from __future__ import annotations

L1 = "L1"
L2 = "L2"
L3 = "L3"
L4 = "L4"

SOURCE_LEVEL_MAP = {
    "csrc": L1, "sse": L1, "szse": L1, "cninfo": L1, "gov": L1, "ndrc": L1, "miit": L1, "pboc": L1,
    "stcn": L2, "cls": L2, "eastmoney": L2, "10jqka": L2, "xinhua_finance": L2,
    "aggregator": L3,
    "forum": L4, "rumor": L4,
}

def infer_source_level(source_name: str) -> str:
    if not source_name:
        return L4
    return SOURCE_LEVEL_MAP.get(source_name.strip().lower(), L3)

def can_trigger_trade(source_level: str) -> bool:
    return source_level in {L1, L2}

def requires_technical_confirmation(source_level: str) -> bool:
    return source_level in {L2, L3, L4}

def should_filter_as_noise(source_level: str) -> bool:
    return source_level == L4
