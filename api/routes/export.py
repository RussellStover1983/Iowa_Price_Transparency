"""Export endpoint — download comparison data as CSV."""

import csv
import io
import re

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse

import aiosqlite

from api.dependencies import get_db

router = APIRouter(prefix="/v1", tags=["export"])


@router.get("/export")
async def export_csv(
    codes: str = Query(
        ..., description="Comma-separated CPT codes", min_length=1
    ),
    payer: str | None = Query(None, description="Filter by payer short_name"),
    city: str | None = Query(None, description="Filter by provider city"),
    county: str | None = Query(None, description="Filter by provider county"),
    format: str = Query("csv", description="Export format (csv)"),
    db: aiosqlite.Connection = Depends(get_db),
):
    """Export comparison data as a CSV download."""
    if format != "csv":
        raise HTTPException(status_code=400, detail="Only CSV format is supported")

    raw_codes = [c.strip() for c in codes.split(",") if c.strip()]
    if not raw_codes:
        raise HTTPException(status_code=422, detail="No valid codes provided")

    for code in raw_codes:
        if not re.match(r"^\d{4,5}$", code):
            raise HTTPException(
                status_code=422, detail=f"Invalid CPT code format: {code}"
            )

    # Deduplicate
    seen = set()
    unique_codes = []
    for code in raw_codes:
        if code not in seen:
            seen.add(code)
            unique_codes.append(code)

    # Build query
    placeholders = ",".join("?" for _ in unique_codes)
    params: list = list(unique_codes)
    where_clauses = [f"nr.billing_code IN ({placeholders})"]

    if payer:
        where_clauses.append("py.short_name = ?")
        params.append(payer)
    if city:
        where_clauses.append("LOWER(p.city) = LOWER(?)")
        params.append(city)
    if county:
        where_clauses.append("LOWER(p.county) = LOWER(?)")
        params.append(county)

    where_sql = " AND ".join(where_clauses)

    cursor = await db.execute(
        f"SELECT nr.billing_code, cl.description, "
        f"p.name AS provider_name, p.city, p.county, "
        f"py.name AS payer_name, "
        f"nr.negotiated_rate, nr.rate_type, nr.service_setting "
        f"FROM normalized_rates nr "
        f"JOIN providers p ON nr.provider_id = p.id "
        f"JOIN payers py ON nr.payer_id = py.id "
        f"LEFT JOIN cpt_lookup cl ON nr.billing_code = cl.code "
        f"WHERE {where_sql} "
        f"ORDER BY nr.billing_code, p.name, py.name",
        params,
    )
    rows = await cursor.fetchall()

    # Write CSV to buffer
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow([
        "CPT Code", "Description", "Provider", "City", "County",
        "Payer", "Negotiated Rate", "Rate Type", "Service Setting",
    ])
    for row in rows:
        writer.writerow(row)

    output.seek(0)
    codes_label = "_".join(unique_codes[:3])
    if len(unique_codes) > 3:
        codes_label += f"_plus{len(unique_codes) - 3}"

    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={
            "Content-Disposition": f'attachment; filename="iowa_prices_{codes_label}.csv"'
        },
    )
