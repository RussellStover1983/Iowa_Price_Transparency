"""Compare endpoint — prices for procedures across Iowa facilities."""

import re
from collections import defaultdict

from fastapi import APIRouter, Depends, HTTPException, Query

import aiosqlite

from api.dependencies import get_db
from db.models import (
    CompareResponse,
    ProcedureComparison,
    ProviderPricing,
    ProviderRate,
)

router = APIRouter(prefix="/v1", tags=["compare"])


@router.get("/compare", response_model=CompareResponse)
async def compare_prices(
    codes: str = Query(
        ..., description="Comma-separated CPT codes (max 10)", min_length=1
    ),
    db: aiosqlite.Connection = Depends(get_db),
):
    """Compare prices across providers for given procedures."""
    # Parse and validate codes
    raw_codes = [c.strip() for c in codes.split(",") if c.strip()]
    if not raw_codes:
        raise HTTPException(status_code=422, detail="No valid codes provided")

    for code in raw_codes:
        if not re.match(r"^\d{4,5}$", code):
            raise HTTPException(
                status_code=422, detail=f"Invalid CPT code format: {code}"
            )

    # Deduplicate while preserving order
    seen = set()
    unique_codes = []
    for code in raw_codes:
        if code not in seen:
            seen.add(code)
            unique_codes.append(code)

    if len(unique_codes) > 10:
        raise HTTPException(
            status_code=400, detail="Maximum 10 codes per request"
        )

    # Query rates with provider and payer info
    placeholders = ",".join("?" for _ in unique_codes)
    cursor = await db.execute(
        f"SELECT nr.billing_code, nr.negotiated_rate, nr.rate_type, nr.service_setting, "
        f"p.id AS provider_id, p.name AS provider_name, p.city, p.county, "
        f"py.id AS payer_id, py.name AS payer_name, "
        f"cl.description AS cpt_description, cl.category "
        f"FROM normalized_rates nr "
        f"JOIN providers p ON nr.provider_id = p.id "
        f"JOIN payers py ON nr.payer_id = py.id "
        f"LEFT JOIN cpt_lookup cl ON nr.billing_code = cl.code "
        f"WHERE nr.billing_code IN ({placeholders}) "
        f"ORDER BY nr.billing_code, p.name, py.name",
        unique_codes,
    )
    rows = await cursor.fetchall()

    # Group: code -> provider_id -> list of rates
    # Also collect CPT info per code
    code_info: dict[str, dict] = {}
    code_provider_rates: dict[str, dict[int, dict]] = defaultdict(
        lambda: defaultdict(lambda: {"info": None, "rates": []})
    )

    for row in rows:
        billing_code = row[0]
        negotiated_rate = row[1]
        rate_type = row[2]
        service_setting = row[3]
        provider_id = row[4]
        provider_name = row[5]
        city = row[6]
        county = row[7]
        payer_id = row[8]
        payer_name = row[9]
        cpt_description = row[10]
        category = row[11]

        if billing_code not in code_info:
            code_info[billing_code] = {
                "description": cpt_description,
                "category": category,
            }

        provider_data = code_provider_rates[billing_code][provider_id]
        if provider_data["info"] is None:
            provider_data["info"] = {
                "provider_id": provider_id,
                "provider_name": provider_name,
                "city": city,
                "county": county,
            }
        provider_data["rates"].append(
            ProviderRate(
                payer_id=payer_id,
                payer_name=payer_name,
                negotiated_rate=negotiated_rate,
                rate_type=rate_type,
                service_setting=service_setting,
            )
        )

    # Build response preserving requested code order
    all_provider_ids = set()
    procedures = []
    for code in unique_codes:
        providers_for_code = code_provider_rates.get(code, {})
        info = code_info.get(code, {"description": None, "category": None})

        provider_list = []
        for provider_id, pdata in providers_for_code.items():
            rates = pdata["rates"]
            rate_values = [r.negotiated_rate for r in rates]
            provider_list.append(
                ProviderPricing(
                    provider_id=pdata["info"]["provider_id"],
                    provider_name=pdata["info"]["provider_name"],
                    city=pdata["info"]["city"],
                    county=pdata["info"]["county"],
                    rates=rates,
                    min_rate=min(rate_values),
                    max_rate=max(rate_values),
                )
            )
            all_provider_ids.add(provider_id)

        procedures.append(
            ProcedureComparison(
                billing_code=code,
                description=info["description"],
                category=info["category"],
                providers=provider_list,
                provider_count=len(provider_list),
            )
        )

    return CompareResponse(
        codes_requested=unique_codes,
        procedures=procedures,
        total_providers=len(all_provider_ids),
    )
