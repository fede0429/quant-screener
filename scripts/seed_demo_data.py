from uuid import uuid4
from datetime import date
from app.db.session import engine, SessionLocal
from app.db.base import Base
from app.models.portfolio_run import PortfolioRun, PortfolioHolding

def main():
    Base.metadata.create_all(bind=engine)
    db = SessionLocal()
    run_id = "11111111-1111-1111-1111-111111111111"
    existing = db.query(PortfolioRun).filter(PortfolioRun.run_id == run_id).first()
    if not existing:
        db.add(PortfolioRun(
            run_id=run_id,
            run_type="preview",
            strategy_name="quality_growth_v2",
            as_of_date=date.today(),
            config={"top_n": 3},
            summary={"note": "demo"},
        ))
        db.add(PortfolioHolding(
            run_id=run_id, symbol="600519.SH", weight_target=0.08, weight_actual=0.02,
            score_source=0.86, sector="食品饮料", industry="白酒", rebalance_action="add"
        ))
        db.add(PortfolioHolding(
            run_id=run_id, symbol="000858.SZ", weight_target=0.06, weight_actual=0.01,
            score_source=0.77, sector="食品饮料", industry="白酒", rebalance_action="add"
        ))
        db.commit()
    db.close()
    print(run_id)

if __name__ == "__main__":
    main()
