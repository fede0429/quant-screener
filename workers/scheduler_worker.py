from __future__ import annotations

from datetime import date

from app.db.session import SessionLocal
from app.services.portfolio_service import PortfolioService
from app.services.research_service import ResearchService
from app.services.proposal_service import ProposalService
from app.services.risk_service import RiskService
from app.services.execution_service import ExecutionService
from app.ops.daily_summary import DailySummaryBuilder


def run_once():
    db = SessionLocal()
    try:
        portfolio_service = PortfolioService(db)
        research_service = ResearchService(db)
        proposal_service = ProposalService(db)
        risk_service = RiskService(db)
        execution_service = ExecutionService()
        summary_builder = DailySummaryBuilder()

        run_id = portfolio_service.import_portfolio_preview(
            strategy_name="quality_growth_v2",
            as_of_date=date.today(),
        )

        reports = []
        for symbol in ["600519.SH", "000858.SZ"]:
            reports.append(research_service.generate_fundamental_report(symbol, date.today()))

        proposals = proposal_service.generate_from_portfolio_run(run_id, "quality_growth_v2")

        approved = 0
        rejected = 0
        fills = 0

        for proposal in proposals:
            decision = risk_service.evaluate(
                proposal.proposal_id,
                {
                    "max_position_weight": 0.08,
                    "max_sector_weight": 0.25,
                    "sector_exposure": {"食品饮料": 0.10},
                },
                {
                    "proposal_sector": "食品饮料",
                    "liquidity_ok": True,
                    "market_state": "normal",
                },
            )
            if decision.decision in {"approve", "degrade"}:
                approved += 1
                order_plan = execution_service.plan_paper_order(
                    {
                        "proposal_id": proposal.proposal_id,
                        "symbol": proposal.symbol,
                        "side": proposal.side,
                    },
                    decision.approved_weight or 0.0,
                )
                order = execution_service.submit_paper_order(order_plan)
                execution_service.simulate_fill(order)
                fills += 1
            else:
                rejected += 1

        summary = summary_builder.build(
            run_id=run_id,
            reports_count=len(reports),
            proposals_count=len(proposals),
            approved_count=approved,
            rejected_count=rejected,
            fills_count=fills,
        )
        print(summary)
        return summary
    finally:
        db.close()


if __name__ == "__main__":
    run_once()
