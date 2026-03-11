from app.core.enums import ProposalStatus

ALLOWED_TRANSITIONS = {
    ProposalStatus.DRAFT: {ProposalStatus.REVIEWED, ProposalStatus.REJECTED, ProposalStatus.EXPIRED},
    ProposalStatus.REVIEWED: {ProposalStatus.APPROVED, ProposalStatus.REJECTED, ProposalStatus.EXPIRED},
    ProposalStatus.APPROVED: {ProposalStatus.EXECUTED, ProposalStatus.EXPIRED, ProposalStatus.REJECTED},
    ProposalStatus.REJECTED: set(),
    ProposalStatus.EXPIRED: set(),
    ProposalStatus.EXECUTED: set(),
}

def can_transition(current: ProposalStatus, target: ProposalStatus) -> bool:
    return target in ALLOWED_TRANSITIONS.get(current, set())
