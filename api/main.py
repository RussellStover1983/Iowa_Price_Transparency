"""FastAPI application entry point."""

import os
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles

from api.routes import admin, compare, cpt, export, payers, procedures, providers
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
    version="0.3.0",
    lifespan=lifespan,
)

# CORS — allow Next.js dev server in non-production environments
if os.getenv("ENVIRONMENT") != "production":
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["http://localhost:3000"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

app.include_router(admin.router)
app.include_router(compare.router)
app.include_router(cpt.router)
app.include_router(export.router)
app.include_router(payers.router)
app.include_router(procedures.router)
app.include_router(providers.router)

# Serve frontend static files
static_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "frontend")
if os.path.isdir(static_dir):
    app.mount("/app", StaticFiles(directory=static_dir, html=True), name="frontend")


@app.get("/", include_in_schema=False)
async def root():
    """Redirect root to frontend."""
    return RedirectResponse(url="/app")


@app.get("/health", response_model=HealthResponse)
async def health():
    """Health check — verifies the API and database are operational."""
    try:
        async with get_connection() as db:
            await db.execute("SELECT 1")
        return {"status": "ok", "database": "connected"}
    except Exception:
        return {"status": "degraded", "database": "disconnected"}
