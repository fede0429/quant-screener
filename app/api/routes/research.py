from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from app.db.session import get_db
from app.schemas.research import ResearchFundamentalRequest, ResearchReportRead
from app.services.research_service import ResearchService

router = APIRouter(prefix="/research", tags=["research"])

@router.post("/fundamentals", response_model=ResearchReportRead)
def generate_fundamentals(payload: ResearchFundamentalRequest, db: Session = Depends(get_db)):
    try:
        return ResearchService(db).generate_fundamental_report(payload.symbol, payload.as_of_date)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
