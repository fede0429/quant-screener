from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from app.db.session import get_db
from app.schemas.risk import RiskEvaluateRequest, RiskDecisionRead
from app.services.risk_service import RiskService

router = APIRouter(prefix="/risk", tags=["risk"])

@router.post("/evaluate", response_model=RiskDecisionRead)
def evaluate_risk(payload: RiskEvaluateRequest, db: Session = Depends(get_db)):
    try:
        return RiskService(db).evaluate(payload.proposal_id, payload.portfolio_context, payload.market_context)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
