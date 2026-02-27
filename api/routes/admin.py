"""Admin endpoints — coverage stats, metadata, and ETL triggers."""

import asyncio
import logging
import os

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query

import aiosqlite

from api.dependencies import get_db
from db.models import CoverageStats

router = APIRouter(prefix="/v1", tags=["admin"])

logger = logging.getLogger(__name__)
DATABASE_PATH = os.getenv("DATABASE_PATH", "./data/iowa_transparency.db")
ADMIN_TOKEN = os.getenv("ADMIN_TOKEN", "")


@router.get("/admin/stats", response_model=CoverageStats)
async def coverage_stats(
    db: aiosqlite.Connection = Depends(get_db),
):
    """Return high-level coverage statistics."""
    cursor = await db.execute(
        "SELECT "
        "  (SELECT COUNT(*) FROM providers WHERE active = 1) AS total_providers, "
        "  (SELECT COUNT(*) FROM payers WHERE active = 1) AS total_payers, "
        "  (SELECT COUNT(DISTINCT billing_code) FROM normalized_rates) AS total_procedures, "
        "  (SELECT COUNT(*) FROM normalized_rates) AS total_rates, "
        "  (SELECT MAX(created_at) FROM normalized_rates) AS last_updated"
    )
    row = await cursor.fetchone()

    db_path = os.getenv("DATABASE_PATH", DATABASE_PATH)
    db_size = 0
    if os.path.exists(db_path):
        db_size = os.path.getsize(db_path)

    return CoverageStats(
        total_providers=row[0],
        total_payers=row[1],
        total_procedures=row[2],
        total_rates=row[3],
        last_updated=row[4],
        db_size_bytes=db_size,
    )


def _verify_token(token: str) -> None:
    """Verify admin token for protected endpoints."""
    if not ADMIN_TOKEN:
        raise HTTPException(status_code=503, detail="ADMIN_TOKEN not configured")
    if token != ADMIN_TOKEN:
        raise HTTPException(status_code=403, detail="Invalid admin token")


def _run_etl_load_npis() -> None:
    """Run NPI loading in a background thread (sync subprocess)."""
    import subprocess
    logger.info("ETL: Starting load_iowa_npis...")
    result = subprocess.run(
        ["python", "-m", "etl.load_iowa_npis"],
        capture_output=True, text=True, timeout=300,
    )
    if result.returncode == 0:
        logger.info("ETL: load_iowa_npis completed successfully")
    else:
        logger.error("ETL: load_iowa_npis failed: %s", result.stderr)


def _run_etl_ingest(payer: str, limit: int) -> None:
    """Run MRF ingestion in a background thread (sync subprocess)."""
    import subprocess
    logger.info("ETL: Starting ingest_mrf payer=%s limit=%d...", payer, limit)
    result = subprocess.run(
        ["python", "-m", "etl.ingest_mrf", "--payer", payer, "--limit", str(limit), "-v"],
        capture_output=True, text=True, timeout=3600,
    )
    if result.returncode == 0:
        logger.info("ETL: ingest_mrf payer=%s completed successfully", payer)
    else:
        logger.error("ETL: ingest_mrf payer=%s failed: %s", payer, result.stderr[-500:])


@router.post("/admin/load-npis")
async def load_npis(
    token: str = Query(..., description="Admin token"),
    background_tasks: BackgroundTasks = None,
):
    """Trigger NPPES NPI loading in the background."""
    _verify_token(token)
    background_tasks.add_task(_run_etl_load_npis)
    return {"status": "started", "task": "load_iowa_npis"}


@router.post("/admin/ingest")
async def ingest_mrf(
    token: str = Query(..., description="Admin token"),
    payer: str = Query(..., description="Payer short_name (uhc, aetna, cigna, medica)"),
    limit: int = Query(1, ge=1, le=20, description="Max MRF files to process"),
    background_tasks: BackgroundTasks = None,
):
    """Trigger MRF ingestion for a payer in the background."""
    _verify_token(token)
    background_tasks.add_task(_run_etl_ingest, payer, limit)
    return {"status": "started", "task": "ingest_mrf", "payer": payer, "limit": limit}
