"""Provider endpoints — list and detail for Iowa facilities."""

from fastapi import APIRouter, Depends, HTTPException, Query

import aiosqlite

from api.dependencies import get_db
from db.models import Provider, ProviderSummary, ProvidersResponse

router = APIRouter(prefix="/v1", tags=["providers"])


@router.get("/providers", response_model=ProvidersResponse)
async def list_providers(
    city: str | None = Query(None, description="Filter by city"),
    county: str | None = Query(None, description="Filter by county"),
    db: aiosqlite.Connection = Depends(get_db),
):
    """Return all Iowa providers with summary counts."""
    where_clauses = ["p.active = 1"]
    params: list = []

    if city:
        where_clauses.append("LOWER(p.city) = LOWER(?)")
        params.append(city)
    if county:
        where_clauses.append("LOWER(p.county) = LOWER(?)")
        params.append(county)

    where_sql = " AND ".join(where_clauses)

    cursor = await db.execute(
        f"SELECT p.id, p.name, p.city, p.county, p.facility_type, p.zip_code, "
        f"COUNT(DISTINCT nr.billing_code) AS procedure_count, "
        f"COUNT(DISTINCT nr.payer_id) AS payer_count "
        f"FROM providers p "
        f"LEFT JOIN normalized_rates nr ON p.id = nr.provider_id "
        f"WHERE {where_sql} "
        f"GROUP BY p.id "
        f"ORDER BY p.name",
        params,
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

    return ProvidersResponse(count=len(providers), providers=providers)


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
