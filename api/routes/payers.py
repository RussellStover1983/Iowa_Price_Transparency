"""Payer endpoints — list Iowa insurance payers."""

from fastapi import APIRouter, Depends

import aiosqlite

from api.dependencies import get_db
from db.models import Payer

router = APIRouter(prefix="/v1", tags=["payers"])


@router.get("/payers", response_model=list[Payer])
async def list_payers(db: aiosqlite.Connection = Depends(get_db)):
    """Return all active Iowa payers."""
    cursor = await db.execute(
        "SELECT id, name, short_name, toc_url, state_filter, active, last_crawled, notes "
        "FROM payers WHERE active = 1 ORDER BY name"
    )
    rows = await cursor.fetchall()
    return [dict(row) for row in rows]
