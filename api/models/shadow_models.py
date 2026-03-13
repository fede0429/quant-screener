from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List


@dataclass
class StrategyProposal:
    proposal_id: str
    trade_date: str
    symbol: str
    agent_group: str
    strategy_template: str
    proposal_reason: str
    policy_score: float = 0.0
    event_score: float = 0.0
    technical_score: float = 0.0
    intl_adjustment: float = 0.0
    decision_score: float = 0.0
    accepted_flag: bool = False
    rejected_reason: str = ""
    risk_veto_reason: str = ""
    human_override_reason: str = ""
    planned_entry: float | None = None
    planned_stop: float | None = None
    planned_tp: float | None = None
    planned_holding_days: int = 1
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ShadowPosition:
    shadow_id: str
    proposal_id: str
    symbol: str
    entry_price: float | None = None
    exit_price: float | None = None
    holding_days: int = 1
    return_t1: float | None = None
    return_t3: float | None = None
    return_t5: float | None = None
    max_favorable_excursion: float | None = None
    max_adverse_excursion: float | None = None
    hit_stop_flag: bool = False
    hit_tp_flag: bool = False
    metadata: Dict[str, Any] = field(default_factory=dict)
