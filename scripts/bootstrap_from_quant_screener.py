from __future__ import annotations

from datetime import date

from app.db.session import SessionLocal
from app.services.portfolio_service import PortfolioService
from app.services.research_service import ResearchService
from app.services.proposal_service import ProposalService
from app.services.risk_service import RiskService
from app.services.execution_service import ExecutionService
from app.services.replay_import_service import ReplayImportService


def main():
    db = SessionLocal()
    try:
        research_service = ResearchService(db)
        portfolio_service = PortfolioService(db)
        proposal_service = ProposalService(db)
        risk_service = RiskService(db)
        replay_service = ReplayImportService(db)
        execution_service = ExecutionService()

        run_id = portfolio_service.import_portfolio_preview(
            strategy_name="quality_growth_v2",
            as_of_date=date.today(),
        )
        replay_service.record_imported_run(
            run_id=run_id,
            strategy_name="quality_growth_v2",
            symbols=["600519.SH", "000858.SZ"],
        )
        print("run_id:", run_id)

        for symbol in ["600519.SH", "000858.SZ"]:
            report = research_service.generate_fundamental_report(symbol, date.today())
            print("report:", report.symbol, report.report_id)

        proposals = proposal_service.generate_from_portfolio_run(run_id, "quality_growth_v2")
        print("proposals:", len(proposals))

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
            print("decision:", proposal.symbol, decision.decision, decision.approved_weight)

            if decision.decision in {"approve", "degrade"}:
                order_plan = execution_service.plan_paper_order(
                    {
                        "proposal_id": proposal.proposal_id,
                        "symbol": proposal.symbol,
                        "side": proposal.side,
                    },
                    decision.approved_weight or 0.0,
                )
                order = execution_service.submit_paper_order(order_plan)
                fill = execution_service.simulate_fill(order)
                print("fill:", fill["symbol"], fill["status"])
    finally:
        db.close()


if __name__ == "__main__":
    main()
