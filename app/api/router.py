from fastapi import APIRouter
from app.api.routes.research import router as research_router
from app.api.routes.proposals import router as proposals_router
from app.api.routes.risk import router as risk_router
from app.api.routes.replay import router as replay_router

api_router = APIRouter()
api_router.include_router(research_router)
api_router.include_router(proposals_router)
api_router.include_router(risk_router)
api_router.include_router(replay_router)
