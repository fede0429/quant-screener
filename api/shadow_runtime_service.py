from __future__ import annotations

from uuid import uuid4

from api.models.shadow_models import StrategyProposal
from api.proposal_ledger import ProposalLedger
from api.shadow_position_store import ShadowPositionStore
from api.shadow_replay_engine import ShadowReplayEngine


class ShadowRuntimeService:
    def __init__(self) -> None:
        self.ledger = ProposalLedger()
        self.store = ShadowPositionStore()
        self.replay_engine = ShadowReplayEngine()

    def register_proposal(self, proposal: StrategyProposal) -> dict:
        self.ledger.add(proposal)
        pos = self.store.create_from_proposal(proposal, shadow_id=str(uuid4()))
        return {
            "proposal_id": proposal.proposal_id,
            "shadow_id": pos.shadow_id,
            "registered": True,
        }

    def replay_one(self, shadow_id: str, price_path: list[float]) -> dict:
        positions = [p for p in self.store.list_all() if p.shadow_id == shadow_id]
        if not positions:
            return {"shadow_id": shadow_id, "replayed": False, "reason": "shadow_not_found"}
        return self.replay_engine.replay(positions[0], price_path)

    def snapshot(self) -> dict:
        return {
            "ledger": self.ledger.snapshot(),
            "store": self.store.snapshot(),
        }
