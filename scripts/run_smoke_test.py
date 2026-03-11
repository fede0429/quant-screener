from datetime import date

from app.db.session import SessionLocal
from app.services.portfolio_service import PortfolioService
from app.services.research_service import ResearchService
from app.services.proposal_service import ProposalService
from app.services.risk_service import RiskService
from app.services.execution_service import ExecutionService
from app.observability.run_logger import RunLogger
from app.observability.metrics import MetricsCollector

def main():
    db = SessionLocal()
    logger = RunLogger()
    metrics = MetricsCollector()
    try:
        portfolio_service = PortfolioService(db)
        research_service = ResearchService(db)
        proposal_service = ProposalService(db)
        risk_service = RiskService(db)
        execution_service = ExecutionService()

        logger.info("smoke_test_started")
        run_id = portfolio_service.import_portfolio_preview("quality_growth_v2", date.today())
        logger.info("portfolio_imported", run_id=run_id)
        metrics.inc("portfolio_runs")

        for symbol in ["600519.SH", "000858.SZ"]:
            research_service.generate_fundamental_report(symbol, date.today())
            metrics.inc("reports_generated")

        proposals = proposal_service.generate_from_portfolio_run(run_id, "quality_growth_v2")
        metrics.set("proposals_generated", len(proposals))

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
            metrics.inc(f"risk_{decision.decision}")
            if decision.decision in {"approve", "degrade"}:
                plan = execution_service.plan_paper_order(
                    {
                        "proposal_id": proposal.proposal_id,
                        "symbol": proposal.symbol,
                        "side": proposal.side,
                    },
                    decision.approved_weight or 0.0,
                )
                order = execution_service.submit_paper_order(plan)
                execution_service.simulate_fill(order)
                metrics.inc("fills")

        print({"metrics": metrics.snapshot(), "logs": logger.snapshot()[:3]})
    finally:
        db.close()

if __name__ == "__main__":
    main()
