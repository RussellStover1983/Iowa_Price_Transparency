"""FastAPI application entry point."""

from contextlib import asynccontextmanager

from fastapi import FastAPI

from api.routes import compare, payers, cpt
from db.init_db import init_database
from db.session import get_connection
from db.models import HealthResponse


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize database on startup."""
    await init_database()
    yield


app = FastAPI(
    title="Iowa Price Transparency API",
    description="Compare what Iowa facilities charge for medical procedures",
    version="0.1.0",
    lifespan=lifespan,
)

app.include_router(compare.router)
app.include_router(payers.router)
app.include_router(cpt.router)


@app.get("/health", response_model=HealthResponse)
async def health():
    """Health check — verifies the API and database are operational."""
    try:
        async with get_connection() as db:
            await db.execute("SELECT 1")
        return {"status": "ok", "database": "connected"}
    except Exception:
        return {"status": "degraded", "database": "disconnected"}
