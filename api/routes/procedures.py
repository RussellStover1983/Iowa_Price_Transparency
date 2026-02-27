"""Procedure stats endpoints — statewide price statistics per CPT code."""

import re
import statistics

from fastapi import APIRouter, Depends, HTTPException

import aiosqlite

from api.dependencies import get_db
from db.models import ProcedureStatsDetail

router = APIRouter(prefix="/v1", tags=["procedures"])


def _percentile(sorted_data: list[float], p: float) -> float:
    """Compute the p-th percentile (0-100) of sorted data."""
    if not sorted_data:
        return 0.0
    k = (len(sorted_data) - 1) * (p / 100.0)
    f = int(k)
    c = f + 1
    if c >= len(sorted_data):
        return sorted_data[f]
    return sorted_data[f] + (k - f) * (sorted_data[c] - sorted_data[f])


@router.get("/procedures/{code}/stats", response_model=ProcedureStatsDetail)
async def procedure_stats(
    code: str,
    db: aiosqlite.Connection = Depends(get_db),
):
    """Return statewide price statistics for a procedure code."""
    if not re.match(r"^\d{4,5}$", code):
        raise HTTPException(status_code=422, detail=f"Invalid CPT code format: {code}")

    # Fetch CPT info
    cursor = await db.execute(
        "SELECT description, category FROM cpt_lookup WHERE code = ?",
        (code,),
    )
    cpt_row = await cursor.fetchone()

    # Fetch all rates sorted
    cursor = await db.execute(
        "SELECT nr.negotiated_rate, nr.provider_id, nr.payer_id "
        "FROM normalized_rates nr "
        "WHERE nr.billing_code = ? "
        "ORDER BY nr.negotiated_rate",
        (code,),
    )
    rows = await cursor.fetchall()

    if not rows:
        raise HTTPException(status_code=404, detail=f"No rates found for code {code}")

    rates = [row[0] for row in rows]
    provider_ids = set(row[1] for row in rows if row[1] is not None)
    payer_ids = set(row[2] for row in rows)

    return ProcedureStatsDetail(
        billing_code=code,
        description=cpt_row[0] if cpt_row else None,
        category=cpt_row[1] if cpt_row else None,
        min_rate=min(rates),
        max_rate=max(rates),
        median_rate=round(statistics.median(rates), 2),
        avg_rate=round(statistics.mean(rates), 2),
        p25_rate=round(_percentile(rates, 25), 2),
        p75_rate=round(_percentile(rates, 75), 2),
        rate_count=len(rates),
        provider_count=len(provider_ids),
        payer_count=len(payer_ids),
        potential_savings=round(max(rates) - min(rates), 2),
    )
