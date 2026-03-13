from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from api.models.decision_input import DecisionInput
from api.models.risk_result import RiskResult


@dataclass
class RiskFirewallConfig:
    block_st: bool = True
    block_regulatory_flag: bool = True
    block_major_unlock: bool = True
    block_major_reduction: bool = True
    max_price_spike_ratio: float = 0.015
    require_reference_price: bool = True


class RiskFirewall:
    def __init__(self, config: Optional[RiskFirewallConfig] = None) -> None:
        self.config = config or RiskFirewallConfig()

    def evaluate(self, item: DecisionInput) -> RiskResult:
        if self.config.require_reference_price and (item.reference_price is None or item.reference_price <= 0):
            return RiskResult(False, "missing_reference_price", detail={"code": item.code})

        flags = {x.lower() for x in item.risk_flags}
        if self.config.block_st and "st" in flags:
            return RiskResult(False, "st_blocked", detail={"risk_flags": list(item.risk_flags)})
        if self.config.block_regulatory_flag and "regulatory" in flags:
            return RiskResult(False, "regulatory_blocked", detail={"risk_flags": list(item.risk_flags)})
        if self.config.block_major_unlock and "major_unlock" in flags:
            return RiskResult(False, "major_unlock_blocked", detail={"risk_flags": list(item.risk_flags)})
        if self.config.block_major_reduction and "major_reduction" in flags:
            return RiskResult(False, "major_reduction_blocked", detail={"risk_flags": list(item.risk_flags)})

        warnings = []
        if item.reference_price and item.latest_price and item.reference_price > 0:
            ratio = (item.latest_price - item.reference_price) / item.reference_price
            if ratio > self.config.max_price_spike_ratio:
                warnings.append("price_spike_warning")

        return RiskResult(True, warnings=warnings, detail={"risk_flags": list(item.risk_flags)})
