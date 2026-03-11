from datetime import date

from app.db.session import SessionLocal
from app.services.portfolio_service import PortfolioService
from app.services.research_service import ResearchService

def main():
    db = SessionLocal()
    try:
        portfolio_service = PortfolioService(db)
        research_service = ResearchService(db)

        run_id = portfolio_service.import_portfolio_preview(
            strategy_name="quality_growth_v2",
            as_of_date=date.today(),
        )
        print("imported run_id:", run_id)

        for symbol in ["600519.SH", "000858.SZ"]:
            report = research_service.generate_fundamental_report(symbol, date.today())
            print("report:", report.symbol, report.report_id)
    finally:
        db.close()

if __name__ == "__main__":
    main()
