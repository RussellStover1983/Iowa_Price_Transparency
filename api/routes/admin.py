"""Admin endpoints — coverage stats, metadata, and ETL triggers."""

import logging
import os
import subprocess
from datetime import datetime, timezone

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query

import aiosqlite

from api.dependencies import get_db
from db.models import CoverageStats

router = APIRouter(prefix="/v1", tags=["admin"])

logger = logging.getLogger(__name__)
DATABASE_PATH = os.getenv("DATABASE_PATH", "./data/iowa_transparency.db")
ADMIN_TOKEN = os.getenv("ADMIN_TOKEN", "")

# Simple in-memory ETL job tracker
_etl_jobs: list[dict] = []


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


def _run_subprocess(cmd: list[str], job: dict, timeout: int = 3600) -> None:
    """Run a subprocess, stream output to logger, update job status."""
    job["status"] = "running"
    job["started_at"] = datetime.now(timezone.utc).isoformat()
    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=timeout,
        )
        job["exit_code"] = result.returncode
        job["stdout"] = result.stdout[-2000:] if result.stdout else ""
        job["stderr"] = result.stderr[-2000:] if result.stderr else ""
        if result.returncode == 0:
            job["status"] = "completed"
            logger.info("ETL job %s completed. stdout: %s", job["task"], result.stdout[-500:])
        else:
            job["status"] = "failed"
            logger.error("ETL job %s failed (exit %d): %s", job["task"], result.returncode, result.stderr[-500:])
    except subprocess.TimeoutExpired:
        job["status"] = "timeout"
        logger.error("ETL job %s timed out after %ds", job["task"], timeout)
    except Exception as e:
        job["status"] = "error"
        job["stderr"] = str(e)
        logger.error("ETL job %s error: %s", job["task"], e)
    finally:
        job["finished_at"] = datetime.now(timezone.utc).isoformat()


@router.post("/admin/load-npis")
async def load_npis(
    token: str = Query(..., description="Admin token"),
    background_tasks: BackgroundTasks = None,
):
    """Trigger NPPES NPI loading in the background."""
    _verify_token(token)
    job = {"task": "load_iowa_npis", "status": "queued", "created_at": datetime.now(timezone.utc).isoformat()}
    _etl_jobs.append(job)
    background_tasks.add_task(_run_subprocess, ["python", "-m", "etl.load_iowa_npis"], job, 300)
    return {"status": "started", "task": "load_iowa_npis", "job_index": len(_etl_jobs) - 1}


@router.post("/admin/ingest")
async def ingest_mrf(
    token: str = Query(..., description="Admin token"),
    payer: str = Query(..., description="Payer short_name (uhc, aetna, cigna, medica)"),
    limit: int = Query(1, ge=1, le=20, description="Max MRF files to process"),
    background_tasks: BackgroundTasks = None,
):
    """Trigger MRF ingestion for a payer in the background."""
    _verify_token(token)
    job = {"task": f"ingest_mrf:{payer}", "payer": payer, "limit": limit, "status": "queued", "created_at": datetime.now(timezone.utc).isoformat()}
    _etl_jobs.append(job)
    background_tasks.add_task(
        _run_subprocess,
        ["python", "-m", "etl.ingest_mrf", "--payer", payer, "--limit", str(limit), "-v"],
        job, 3600,
    )
    return {"status": "started", "task": "ingest_mrf", "payer": payer, "limit": limit, "job_index": len(_etl_jobs) - 1}


@router.get("/admin/jobs")
async def list_jobs(
    token: str = Query(..., description="Admin token"),
):
    """List all ETL jobs and their status."""
    _verify_token(token)
    return {"jobs": _etl_jobs}
