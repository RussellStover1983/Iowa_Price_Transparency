"""Dashboard endpoints for health system payer negotiation analytics."""

import json
import statistics
from collections import defaultdict

from fastapi import APIRouter, Depends, Query

import aiosqlite

from api.dependencies import get_db

router = APIRouter(prefix="/v1/dashboard", tags=["dashboard"])


@router.get("/hospital-rates")
async def hospital_rates(
    provider_id: int = Query(..., description="Provider ID"),
    db: aiosqlite.Connection = Depends(get_db),
):
    """Get all rates for a single hospital, grouped by procedure and payer.

    Shows rate-to-Medicare ratios for each rate. This is the core
    "My Hospital" view for payer negotiation prep.
    """
    # Get provider info
    cursor = await db.execute(
        "SELECT id, name, city, county, facility_type FROM providers WHERE id = ?",
        (provider_id,),
    )
    provider = await cursor.fetchone()
    if not provider:
        return {"error": "Provider not found"}

    # Get all rates with Medicare benchmarks
    cursor = await db.execute(
        "SELECT nr.billing_code, nr.negotiated_rate, nr.rate_type, nr.service_setting, "
        "py.id AS payer_id, py.name AS payer_name, py.short_name, "
        "cl.description, cl.category, "
        "cl.medicare_facility_rate, cl.medicare_professional_rate, cl.medicare_opps_rate "
        "FROM normalized_rates nr "
        "JOIN payers py ON nr.payer_id = py.id "
        "LEFT JOIN cpt_lookup cl ON nr.billing_code = cl.code "
        "WHERE nr.provider_id = ? "
        "ORDER BY cl.category, nr.billing_code, py.name",
        (provider_id,),
    )
    rows = await cursor.fetchall()

    # Group by procedure
    procedures: dict[str, dict] = {}
    for row in rows:
        code = row[0]
        if code not in procedures:
            procedures[code] = {
                "billing_code": code,
                "description": row[7],
                "category": row[8],
                "medicare_facility_rate": row[9],
                "medicare_professional_rate": row[10],
                "medicare_opps_rate": row[11],
                "payer_rates": {},
            }

        payer_name = row[5]
        rate = row[1]
        rate_type = row[2]
        service_setting = row[3]

        if payer_name not in procedures[code]["payer_rates"]:
            procedures[code]["payer_rates"][payer_name] = {
                "payer_id": row[4],
                "payer_name": payer_name,
                "rates": [],
            }

        # Compute % of Medicare
        medicare_ref = None
        setting = (service_setting or "").lower()
        # Map service settings to Medicare reference:
        # institutional/outpatient/inpatient → OPPS (facility fee)
        # professional/ambulatory → MPFS (physician fee)
        if setting in ("institutional", "outpatient", "inpatient") and row[11]:
            medicare_ref = row[11]  # OPPS rate
        elif setting in ("professional", "ambulatory") and row[9]:
            medicare_ref = row[9]  # MPFS facility rate

        pct_medicare = round((rate / medicare_ref) * 100) if medicare_ref and medicare_ref > 0 else None

        procedures[code]["payer_rates"][payer_name]["rates"].append({
            "negotiated_rate": rate,
            "rate_type": rate_type,
            "service_setting": service_setting,
            "pct_medicare": pct_medicare,
        })

    # Convert payer_rates dicts to lists for JSON
    procedure_list = []
    for proc in procedures.values():
        proc["payer_rates"] = list(proc["payer_rates"].values())
        procedure_list.append(proc)

    return {
        "provider": {
            "id": provider[0],
            "name": provider[1],
            "city": provider[2],
            "county": provider[3],
            "facility_type": provider[4],
        },
        "procedures": procedure_list,
        "procedure_count": len(procedure_list),
        "payer_count": len({
            pn for proc in procedure_list
            for pr in proc["payer_rates"]
            for pn in [pr["payer_name"]]
        }),
    }


@router.get("/market-position")
async def market_position(
    billing_code: str = Query(..., description="CPT code"),
    payer: str | None = Query(None, description="Filter by payer short_name"),
    service_setting: str | None = Query(None, description="Filter: institutional or professional"),
    db: aiosqlite.Connection = Depends(get_db),
):
    """Show where every Iowa facility falls for a given procedure.

    Returns all facilities with their median rate, ranked by price,
    with market percentile and rate-to-Medicare ratio.
    """
    # Get Medicare baseline
    cursor = await db.execute(
        "SELECT description, category, medicare_facility_rate, medicare_professional_rate, medicare_opps_rate "
        "FROM cpt_lookup WHERE code = ?",
        (billing_code,),
    )
    cpt = await cursor.fetchone()

    # Build query
    params: list = [billing_code]
    where = ["nr.billing_code = ?"]
    if payer:
        where.append("py.short_name = ?")
        params.append(payer)
    if service_setting:
        where.append("LOWER(nr.service_setting) = LOWER(?)")
        params.append(service_setting)

    where_sql = " AND ".join(where)

    cursor = await db.execute(
        f"SELECT p.id, p.name, p.city, p.county, "
        f"nr.negotiated_rate, nr.rate_type, nr.service_setting, "
        f"py.name AS payer_name "
        f"FROM normalized_rates nr "
        f"JOIN providers p ON nr.provider_id = p.id "
        f"JOIN payers py ON nr.payer_id = py.id "
        f"WHERE {where_sql} "
        f"ORDER BY p.name",
        params,
    )
    rows = await cursor.fetchall()

    # Group by provider -> compute median
    provider_rates: dict[int, dict] = {}
    for row in rows:
        pid = row[0]
        if pid not in provider_rates:
            provider_rates[pid] = {
                "provider_id": pid,
                "name": row[1],
                "city": row[2],
                "county": row[3],
                "rates": [],
                "payers": set(),
            }
        provider_rates[pid]["rates"].append(row[4])
        provider_rates[pid]["payers"].add(row[7])

    # Compute medians and sort
    facilities = []
    for pdata in provider_rates.values():
        rates = pdata["rates"]
        med = round(statistics.median(rates), 2)

        # Pick Medicare reference based on whether we're filtering by setting
        medicare_ref = None
        ss = (service_setting or "").lower()
        if ss in ("institutional", "outpatient", "inpatient") and cpt and cpt[4]:
            medicare_ref = cpt[4]  # OPPS
        elif ss in ("professional", "ambulatory") and cpt and cpt[2]:
            medicare_ref = cpt[2]  # MPFS
        elif cpt and cpt[4]:
            medicare_ref = cpt[4]  # Default to OPPS for mixed

        facilities.append({
            "provider_id": pdata["provider_id"],
            "name": pdata["name"],
            "city": pdata["city"],
            "county": pdata["county"],
            "median_rate": med,
            "min_rate": round(min(rates), 2),
            "max_rate": round(max(rates), 2),
            "rate_count": len(rates),
            "payer_count": len(pdata["payers"]),
            "pct_medicare": round((med / medicare_ref) * 100) if medicare_ref and medicare_ref > 0 else None,
        })

    facilities.sort(key=lambda f: f["median_rate"])

    # Compute percentile for each facility
    n = len(facilities)
    for i, f in enumerate(facilities):
        f["percentile"] = round((i / max(n - 1, 1)) * 100) if n > 1 else 50

    # Market stats
    all_medians = [f["median_rate"] for f in facilities]
    market_stats = None
    if all_medians:
        market_stats = {
            "min": min(all_medians),
            "max": max(all_medians),
            "median": round(statistics.median(all_medians), 2),
            "mean": round(statistics.mean(all_medians), 2),
            "p25": round(all_medians[len(all_medians) // 4], 2) if len(all_medians) >= 4 else None,
            "p75": round(all_medians[3 * len(all_medians) // 4], 2) if len(all_medians) >= 4 else None,
        }

    return {
        "billing_code": billing_code,
        "description": cpt[0] if cpt else None,
        "category": cpt[1] if cpt else None,
        "medicare": {
            "facility_rate": cpt[2] if cpt else None,
            "professional_rate": cpt[3] if cpt else None,
            "opps_rate": cpt[4] if cpt else None,
        } if cpt else None,
        "market_stats": market_stats,
        "facilities": facilities,
        "facility_count": len(facilities),
    }


@router.get("/payer-scorecard")
async def payer_scorecard(
    provider_id: int = Query(..., description="Provider ID"),
    db: aiosqlite.Connection = Depends(get_db),
):
    """Rank payers by rate-to-Medicare ratio for a specific hospital.

    Shows which payers are paying above/below market for this facility.
    """
    # Get provider info
    cursor = await db.execute(
        "SELECT id, name, city, county FROM providers WHERE id = ?",
        (provider_id,),
    )
    provider = await cursor.fetchone()
    if not provider:
        return {"error": "Provider not found"}

    # Get all rates with Medicare benchmarks
    cursor = await db.execute(
        "SELECT nr.billing_code, nr.negotiated_rate, nr.rate_type, nr.service_setting, "
        "py.id AS payer_id, py.name AS payer_name, py.short_name, "
        "cl.medicare_facility_rate, cl.medicare_opps_rate "
        "FROM normalized_rates nr "
        "JOIN payers py ON nr.payer_id = py.id "
        "LEFT JOIN cpt_lookup cl ON nr.billing_code = cl.code "
        "WHERE nr.provider_id = ?",
        (provider_id,),
    )
    rows = await cursor.fetchall()

    # Group by payer -> collect all rate-to-Medicare ratios
    payer_data: dict[str, dict] = {}
    for row in rows:
        payer_name = row[5]
        rate = row[1]
        service_setting = (row[3] or "").lower()
        medicare_facility = row[7]  # MPFS facility
        medicare_opps = row[8]      # OPPS

        # Pick appropriate Medicare reference
        medicare_ref = None
        if service_setting in ("institutional", "outpatient", "inpatient") and medicare_opps:
            medicare_ref = medicare_opps
        elif service_setting in ("professional", "ambulatory") and medicare_facility:
            medicare_ref = medicare_facility

        if payer_name not in payer_data:
            payer_data[payer_name] = {
                "payer_id": row[4],
                "payer_name": payer_name,
                "short_name": row[6],
                "total_rates": 0,
                "rates_with_medicare": 0,
                "pct_medicare_values": [],
                "procedure_codes": set(),
                "rate_sum": 0.0,
            }

        pd = payer_data[payer_name]
        pd["total_rates"] += 1
        pd["rate_sum"] += rate
        pd["procedure_codes"].add(row[0])

        if medicare_ref and medicare_ref > 0:
            pct = (rate / medicare_ref) * 100
            pd["pct_medicare_values"].append(pct)
            pd["rates_with_medicare"] += 1

    # Build scorecard
    scorecard = []
    for pd in payer_data.values():
        pct_values = pd["pct_medicare_values"]
        avg_pct = round(statistics.mean(pct_values)) if pct_values else None
        median_pct = round(statistics.median(pct_values)) if pct_values else None

        scorecard.append({
            "payer_id": pd["payer_id"],
            "payer_name": pd["payer_name"],
            "short_name": pd["short_name"],
            "procedure_count": len(pd["procedure_codes"]),
            "total_rates": pd["total_rates"],
            "avg_pct_medicare": avg_pct,
            "median_pct_medicare": median_pct,
            "avg_rate": round(pd["rate_sum"] / pd["total_rates"], 2),
        })

    # Sort by median % of Medicare (lowest payers first = potential underpayers)
    scorecard.sort(key=lambda s: s["median_pct_medicare"] or 0)

    return {
        "provider": {
            "id": provider[0],
            "name": provider[1],
            "city": provider[2],
            "county": provider[3],
        },
        "payers": scorecard,
        "payer_count": len(scorecard),
    }
