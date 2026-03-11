from dataclasses import dataclass

@dataclass
class PolicyResult:
    allowed: bool
    decision: str
    approved_weight: float
    reasons: list[str]

class PolicyEngine:
    def __init__(self, config: dict):
        self.config = config

    def evaluate(self, proposal: dict, portfolio_context: dict, market_context: dict) -> PolicyResult:
        reasons = []
        desired_weight = float(proposal.get("desired_weight", 0.0))
        max_position_weight = float(portfolio_context.get("max_position_weight", self.config.get("max_position_weight", 0.08)))
        max_sector_weight = float(portfolio_context.get("max_sector_weight", self.config.get("max_sector_weight", 0.25)))
        sector = market_context.get("proposal_sector", "UNKNOWN")
        sector_exposure = portfolio_context.get("sector_exposure", {})
        current_sector_weight = float(sector_exposure.get(sector, 0.0))
        liquidity_ok = bool(market_context.get("liquidity_ok", True))
        approved_weight = desired_weight
        decision = "approve"
        if approved_weight > max_position_weight:
            approved_weight = max_position_weight
            decision = "degrade"
            reasons.append("MAX_POSITION_WEIGHT_CAPPED")
        if current_sector_weight + approved_weight > max_sector_weight:
            approved_weight = max(0.0, max_sector_weight - current_sector_weight)
            decision = "degrade" if approved_weight > 0 else "reject"
            reasons.append("MAX_SECTOR_WEIGHT_CAPPED")
        if not liquidity_ok:
            approved_weight = 0.0
            decision = "reject"
            reasons.append("LIQUIDITY_FILTER_FAILED")
        if market_context.get("market_state") == "stress" and approved_weight > 0:
            approved_weight *= 0.5
            if decision == "approve":
                decision = "degrade"
            reasons.append("MARKET_STRESS_DEGRADED")
        return PolicyResult(decision in {"approve","degrade"}, decision, approved_weight, reasons or ["PASS_ALL_RULES"])
