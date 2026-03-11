from datetime import date
from uuid import uuid4

from app.models.portfolio_run import PortfolioRun, PortfolioHolding

class PortfolioService:
    def __init__(self, db):
        self.db = db

    def _try_load_quant_screener_portfolio_builder(self):
        # Replace these placeholder imports with your real project path.
        # Example:
        # from api.portfolio_lab import build_target_portfolio
        # return build_target_portfolio
        return None

    def import_portfolio_preview(self, strategy_name: str, as_of_date: date) -> str:
        builder = self._try_load_quant_screener_portfolio_builder()

        run_id = str(uuid4())
        run = PortfolioRun(
            run_id=run_id,
            run_type="preview",
            strategy_name=strategy_name,
            as_of_date=as_of_date,
            config={"source": "adapter"},
            summary={"status": "imported"},
        )
        self.db.add(run)

        holdings = []
        if builder is not None:
            rows = builder(as_of_date=as_of_date)
            for row in rows:
                holdings.append(
                    PortfolioHolding(
                        run_id=run_id,
                        symbol=row["symbol"],
                        weight_target=float(row.get("weight_target", 0.0)),
                        weight_actual=float(row.get("weight_actual", 0.0)),
                        score_source=float(row.get("score_source", 0.5)),
                        sector=row.get("sector"),
                        industry=row.get("industry"),
                        rebalance_action=row.get("rebalance_action", "add"),
                    )
                )
        else:
            holdings = [
                PortfolioHolding(
                    run_id=run_id,
                    symbol="600519.SH",
                    weight_target=0.08,
                    weight_actual=0.02,
                    score_source=0.84,
                    sector="食品饮料",
                    industry="白酒",
                    rebalance_action="add",
                ),
                PortfolioHolding(
                    run_id=run_id,
                    symbol="000858.SZ",
                    weight_target=0.06,
                    weight_actual=0.01,
                    score_source=0.77,
                    sector="食品饮料",
                    industry="白酒",
                    rebalance_action="add",
                ),
            ]

        for h in holdings:
            self.db.add(h)

        self.db.commit()
        return run_id
