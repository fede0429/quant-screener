from fastapi import FastAPI
from app.api.router import api_router

app = FastAPI(title="Quant Platform v2 Unified Baseline", version="0.1.0")
app.include_router(api_router, prefix="/api/v2")

@app.get("/health")
def health():
    return {"status": "ok"}
