from app.core.enums import ProposalStatus
from app.core.state_machine import can_transition

def test_state_machine():
    assert can_transition(ProposalStatus.DRAFT, ProposalStatus.REVIEWED) is True
    assert can_transition(ProposalStatus.APPROVED, ProposalStatus.DRAFT) is False
