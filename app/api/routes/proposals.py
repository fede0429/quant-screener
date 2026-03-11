from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from app.db.session import get_db
from app.schemas.proposal import ProposalGenerateRequest, ProposalRead
from app.services.proposal_service import ProposalService

router = APIRouter(prefix="/proposals", tags=["proposals"])

@router.post("/generate", response_model=list[ProposalRead])
def generate_proposals(payload: ProposalGenerateRequest, db: Session = Depends(get_db)):
    try:
        return ProposalService(db).generate_from_portfolio_run(payload.run_id, payload.strategy_name)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
