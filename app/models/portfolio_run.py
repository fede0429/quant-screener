from sqlalchemy import Column, String, Date, Integer, Float, JSON
from app.db.session import Base

class PortfolioRun(Base):
    __tablename__ = "portfolio_runs"
    run_id = Column(String, primary_key=True)
    run_type = Column(String(32), nullable=False)
    strategy_name = Column(String(128), nullable=False)
    as_of_date = Column(Date, nullable=False)
    config = Column(JSON, nullable=False)
    summary = Column(JSON)

class PortfolioHolding(Base):
    __tablename__ = "portfolio_holdings"
    id = Column(Integer, primary_key=True, autoincrement=True)
    run_id = Column(String, nullable=False, index=True)
    symbol = Column(String(32), nullable=False, index=True)
    weight_target = Column(Float)
    weight_actual = Column(Float)
    score_source = Column(Float)
    sector = Column(String(64))
    industry = Column(String(128))
    rebalance_action = Column(String(32))
