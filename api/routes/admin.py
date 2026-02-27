"""Admin endpoints — coverage stats and metadata."""

import os

from fastapi import APIRouter, Depends

import aiosqlite

from api.dependencies import get_db
from db.models import CoverageStats

router = APIRouter(prefix="/v1", tags=["admin"])

DATABASE_PATH = os.getenv("DATABASE_PATH", "./data/iowa_transparency.db")


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
