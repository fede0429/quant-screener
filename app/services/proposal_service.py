from uuid import uuid4
from datetime import datetime
from app.models.portfolio_run import PortfolioHolding
from app.models.research_report import ResearchReport
from app.models.proposal import Proposal
from app.models.audit_event import AuditEvent

class ProposalService:
    def __init__(self, db):
        self.db = db

    def generate_from_portfolio_run(self, run_id, strategy_name):
        holdings = self.db.query(PortfolioHolding).filter(PortfolioHolding.run_id == run_id).all()
        proposals = []
        for holding in holdings:
            latest_report = self.db.query(ResearchReport).filter(
                ResearchReport.symbol == holding.symbol
            ).order_by(ResearchReport.as_of_date.desc()).first()
            desired_weight = float(holding.weight_target or 0.0)
            actual_weight = float(holding.weight_actual or 0.0)
            proposal = Proposal(
                proposal_id=str(uuid4()),
                strategy_name=strategy_name,
                symbol=holding.symbol,
                side="buy" if desired_weight > actual_weight else "hold",
                proposal_type="rebalance",
                as_of_time=datetime.utcnow(),
                thesis=latest_report.summary if latest_report and latest_report.summary else "组合调仓提案",
                entry_logic={"trigger": "rebalance_window", "run_id": str(run_id)},
                invalidation_logic={"type": "score_drop", "threshold": 0.20},
                stop_rule={"type": "soft_stop", "max_drawdown": 0.12},
                target_rule={"type": "target_weight", "value": desired_weight},
                horizon_days="20",
                confidence=float(holding.score_source or 0.5),
                desired_weight=desired_weight,
                max_weight=desired_weight,
                urgency="medium",
                status="draft",
            )
            self.db.add(proposal)
            self.db.add(AuditEvent(
                event_id=str(uuid4()),
                event_time=datetime.utcnow(),
                event_type="proposal_created",
                actor="system",
                ref_type="proposal",
                ref_id=proposal.proposal_id,
                payload={"symbol": proposal.symbol, "strategy_name": strategy_name},
            ))
            proposals.append(proposal)
        self.db.commit()
        return proposals
