from datetime import date
from app.db.session import SessionLocal
from app.services.research_service import ResearchService
from app.services.proposal_service import ProposalService
from app.services.risk_service import RiskService

RUN_ID = "11111111-1111-1111-1111-111111111111"

def main():
    db = SessionLocal()
    try:
        rs = ResearchService(db)
        ps = ProposalService(db)
        rks = RiskService(db)

        rs.generate_fundamental_report("600519.SH", date.today())
        rs.generate_fundamental_report("000858.SZ", date.today())

        proposals = ps.generate_from_portfolio_run(RUN_ID, "quality_growth_v2")
        print("proposals:", len(proposals))

        for p in proposals:
            decision = rks.evaluate(
                p.proposal_id,
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
            print("risk:", decision.proposal_id, decision.decision)
    finally:
        db.close()

if __name__ == "__main__":
    main()
