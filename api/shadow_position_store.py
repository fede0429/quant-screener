from __future__ import annotations

from typing import Dict, List
from api.models.shadow_models import ShadowPosition, StrategyProposal


class ShadowPositionStore:
    def __init__(self) -> None:
        self.positions: List[ShadowPosition] = []

    def create_from_proposal(self, proposal: StrategyProposal, shadow_id: str) -> ShadowPosition:
        pos = ShadowPosition(
            shadow_id=shadow_id,
            proposal_id=proposal.proposal_id,
            symbol=proposal.symbol,
            entry_price=proposal.planned_entry,
            holding_days=proposal.planned_holding_days,
            metadata={
                "accepted_flag": proposal.accepted_flag,
                "agent_group": proposal.agent_group,
                "strategy_template": proposal.strategy_template,
            },
        )
        self.positions.append(pos)
        return pos

    def list_all(self) -> List[ShadowPosition]:
        return list(self.positions)

    def snapshot(self) -> Dict:
        return {
            "shadow_position_count": len(self.positions),
        }
