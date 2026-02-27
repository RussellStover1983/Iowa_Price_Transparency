"""Provider endpoints — list, detail, and procedure catalog for Iowa facilities."""

import statistics
from collections import defaultdict

from fastapi import APIRouter, Depends, HTTPException, Query

import aiosqlite

from api.dependencies import get_db
from db.models import (
    PaginatedProvidersResponse,
    Provider,
    ProviderProcedure,
    ProviderProcedureRate,
    ProviderProceduresResponse,
    ProviderSummary,
)

router = APIRouter(prefix="/v1", tags=["providers"])


@router.get("/providers", response_model=PaginatedProvidersResponse)
async def list_providers(
    city: str | None = Query(None, description="Filter by city"),
    county: str | None = Query(None, description="Filter by county"),
    limit: int = Query(50, ge=1, le=500, description="Max results"),
    offset: int = Query(0, ge=0, description="Offset for pagination"),
    db: aiosqlite.Connection = Depends(get_db),
):
    """Return Iowa providers with summary counts (paginated)."""
    where_clauses = ["p.active = 1"]
    params: list = []

    if city:
        where_clauses.append("LOWER(p.city) = LOWER(?)")
        params.append(city)
    if county:
        where_clauses.append("LOWER(p.county) = LOWER(?)")
        params.append(county)

    where_sql = " AND ".join(where_clauses)

    # Get total count
    count_cursor = await db.execute(
        f"SELECT COUNT(*) FROM providers p WHERE {where_sql}", params
    )
    total = (await count_cursor.fetchone())[0]

    # Get paginated results
    cursor = await db.execute(
        f"SELECT p.id, p.name, p.city, p.county, p.facility_type, p.zip_code, "
        f"COUNT(DISTINCT nr.billing_code) AS procedure_count, "
        f"COUNT(DISTINCT nr.payer_id) AS payer_count "
        f"FROM providers p "
        f"LEFT JOIN normalized_rates nr ON p.id = nr.provider_id "
        f"WHERE {where_sql} "
        f"GROUP BY p.id "
        f"ORDER BY p.name "
        f"LIMIT ? OFFSET ?",
        params + [limit, offset],
    )
    rows = await cursor.fetchall()

    providers = [
        ProviderSummary(
            id=row[0],
            name=row[1],
            city=row[2],
            county=row[3],
            facility_type=row[4],
            zip_code=row[5],
            procedure_count=row[6],
            payer_count=row[7],
        )
        for row in rows
    ]

    return PaginatedProvidersResponse(
        count=len(providers),
        providers=providers,
        total=total,
        limit=limit,
        offset=offset,
    )


@router.get("/providers/{provider_id}", response_model=Provider)
async def get_provider(
    provider_id: int,
    db: aiosqlite.Connection = Depends(get_db),
):
    """Get details for a specific provider."""
    cursor = await db.execute(
        "SELECT id, npi, tin, name, facility_type, address, city, state, zip_code, county, active "
        "FROM providers WHERE id = ?",
        (provider_id,),
    )
    row = await cursor.fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="Provider not found")

    return Provider(
        id=row[0], npi=row[1], tin=row[2], name=row[3], facility_type=row[4],
        address=row[5], city=row[6], state=row[7], zip_code=row[8],
        county=row[9], active=bool(row[10]),
    )


@router.get(
    "/providers/{provider_id}/procedures",
    response_model=ProviderProceduresResponse,
)
async def provider_procedures(
    provider_id: int,
    limit: int = Query(50, ge=1, le=200, description="Max procedures"),
    offset: int = Query(0, ge=0, description="Offset for pagination"),
    db: aiosqlite.Connection = Depends(get_db),
):
    """Return all procedures available at a provider with rate breakdowns."""
    # Verify provider exists
    cursor = await db.execute(
        "SELECT id, name FROM providers WHERE id = ?", (provider_id,)
    )
    provider_row = await cursor.fetchone()
    if provider_row is None:
        raise HTTPException(status_code=404, detail="Provider not found")

    provider_name = provider_row[1]

    # Count total distinct billing codes at this provider
    count_cursor = await db.execute(
        "SELECT COUNT(DISTINCT billing_code) FROM normalized_rates WHERE provider_id = ?",
        (provider_id,),
    )
    total = (await count_cursor.fetchone())[0]

    # Get paginated distinct billing codes
    code_cursor = await db.execute(
        "SELECT DISTINCT nr.billing_code "
        "FROM normalized_rates nr "
        "WHERE nr.provider_id = ? "
        "ORDER BY nr.billing_code "
        "LIMIT ? OFFSET ?",
        (provider_id, limit, offset),
    )
    code_rows = await code_cursor.fetchall()
    codes = [row[0] for row in code_rows]

    if not codes:
        return ProviderProceduresResponse(
            provider_id=provider_id,
            provider_name=provider_name,
            procedures=[],
            total=total,
            limit=limit,
            offset=offset,
        )

    # Batch-fetch all rates for these codes at this provider
    placeholders = ",".join("?" for _ in codes)
    cursor = await db.execute(
        f"SELECT nr.billing_code, nr.negotiated_rate, nr.rate_type, nr.service_setting, "
        f"py.id AS payer_id, py.name AS payer_name, "
        f"cl.description, cl.category "
        f"FROM normalized_rates nr "
        f"JOIN payers py ON nr.payer_id = py.id "
        f"LEFT JOIN cpt_lookup cl ON nr.billing_code = cl.code "
        f"WHERE nr.provider_id = ? AND nr.billing_code IN ({placeholders}) "
        f"ORDER BY nr.billing_code, py.name",
        [provider_id] + codes,
    )
    rows = await cursor.fetchall()

    # Group by billing code
    code_data: dict[str, dict] = defaultdict(
        lambda: {"description": None, "category": None, "rates": []}
    )
    for row in rows:
        billing_code = row[0]
        entry = code_data[billing_code]
        if entry["description"] is None:
            entry["description"] = row[6]
            entry["category"] = row[7]
        entry["rates"].append(
            ProviderProcedureRate(
                payer_id=row[4],
                payer_name=row[5],
                negotiated_rate=row[1],
                rate_type=row[2],
                service_setting=row[3],
            )
        )

    procedures = []
    for code in codes:
        data = code_data[code]
        rates = data["rates"]
        rate_values = [r.negotiated_rate for r in rates]
        payer_ids = set(r.payer_id for r in rates)
        procedures.append(
            ProviderProcedure(
                billing_code=code,
                description=data["description"],
                category=data["category"],
                rates=rates,
                min_rate=min(rate_values) if rate_values else 0,
                max_rate=max(rate_values) if rate_values else 0,
                avg_rate=round(statistics.mean(rate_values), 2) if rate_values else 0,
                payer_count=len(payer_ids),
            )
        )

    return ProviderProceduresResponse(
        provider_id=provider_id,
        provider_name=provider_name,
        procedures=procedures,
        total=total,
        limit=limit,
        offset=offset,
    )
