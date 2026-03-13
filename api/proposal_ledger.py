from __future__ import annotations

from typing import Dict, List
from api.models.shadow_models import StrategyProposal


class ProposalLedger:
    def __init__(self) -> None:
        self.proposals: List[StrategyProposal] = []

    def add(self, proposal: StrategyProposal) -> StrategyProposal:
        self.proposals.append(proposal)
        return proposal

    def list_all(self) -> List[StrategyProposal]:
        return list(self.proposals)

    def snapshot(self) -> Dict:
        return {
            "proposal_count": len(self.proposals),
            "accepted_count": sum(1 for p in self.proposals if p.accepted_flag),
            "rejected_count": sum(1 for p in self.proposals if not p.accepted_flag),
        }
