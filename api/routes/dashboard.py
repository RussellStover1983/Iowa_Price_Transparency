"""Dashboard endpoints for health system payer negotiation analytics.

All views use CCN (CMS Certification Number) as the canonical facility
identifier. Each facility has a single primary NPI used for rate lookups.
"""

import statistics
from collections import defaultdict

from fastapi import APIRouter, Depends, Query

import aiosqlite

from api.dependencies import get_db

router = APIRouter(prefix="/v1/dashboard", tags=["dashboard"])


@router.get("/data-quality")
async def data_quality_summary(
    db: aiosqlite.Connection = Depends(get_db),
):
    """Summary of data quality issues from the data_quality_log table."""
    cursor = await db.execute(
        "SELECT category, COUNT(*) FROM data_quality_log GROUP BY category"
    )
    rows = await cursor.fetchall()
    summary = {row[0]: row[1] for row in rows}

    cursor = await db.execute("SELECT COUNT(*) FROM facilities WHERE active = 1")
    total_facilities = (await cursor.fetchone())[0]

    cursor = await db.execute("SELECT COUNT(*) FROM npi_ccn_map WHERE is_primary = 1")
    mapped_facilities = (await cursor.fetchone())[0]

    cursor = await db.execute(
        "SELECT COUNT(DISTINCT m.ccn) FROM npi_ccn_map m "
        "JOIN providers p ON m.npi = p.npi "
        "JOIN normalized_rates nr ON nr.provider_id = p.id "
        "WHERE m.is_primary = 1"
    )
    with_rates = (await cursor.fetchone())[0]

    return {
        "total_facilities": total_facilities,
        "facilities_with_npis": mapped_facilities,
        "facilities_with_rate_data": with_rates,
        "quality_issues": summary,
    }


def _medicare_ref(service_setting: str | None, opps: float | None, mpfs: float | None) -> float | None:
    """Pick the appropriate Medicare reference rate based on service setting."""
    setting = (service_setting or "").lower()
    if setting in ("institutional", "outpatient", "inpatient") and opps:
        return opps
    elif setting in ("professional", "ambulatory") and mpfs:
        return mpfs
    return None


def _pct_medicare(rate: float, medicare_ref: float | None) -> int | None:
    """Compute rate as % of Medicare."""
    if medicare_ref and medicare_ref > 0:
        return round((rate / medicare_ref) * 100)
    return None


async def _get_facility(db: aiosqlite.Connection, ccn: str) -> dict | None:
    """Look up facility by CCN, including its primary NPI and provider_id."""
    cursor = await db.execute(
        "SELECT f.ccn, f.facility_name, f.city, f.bed_count, "
        "f.ownership_type, f.hospital_type, "
        "m.npi, m.provider_id "
        "FROM facilities f "
        "LEFT JOIN npi_ccn_map m ON f.ccn = m.ccn AND m.is_primary = 1 "
        "WHERE f.ccn = ?",
        (ccn,),
    )
    row = await cursor.fetchone()
    if not row:
        return None
    return {
        "ccn": row[0],
        "facility_name": row[1],
        "city": row[2],
        "bed_count": row[3],
        "ownership_type": row[4],
        "hospital_type": row[5],
        "primary_npi": row[6],
        "provider_id": row[7],
    }


@router.get("/facilities")
async def list_facilities(
    db: aiosqlite.Connection = Depends(get_db),
):
    """List all Iowa facilities with their primary NPI and rate availability."""
    cursor = await db.execute(
        "SELECT f.ccn, f.facility_name, f.city, f.bed_count, "
        "f.ownership_type, f.hospital_type, "
        "m.npi, m.provider_id, "
        "(SELECT COUNT(*) FROM normalized_rates nr "
        " WHERE nr.provider_id = m.provider_id) AS rate_count "
        "FROM facilities f "
        "LEFT JOIN npi_ccn_map m ON f.ccn = m.ccn AND m.is_primary = 1 "
        "WHERE f.active = 1 "
        "ORDER BY f.facility_name"
    )
    rows = await cursor.fetchall()

    facilities = []
    for row in rows:
        facilities.append({
            "ccn": row[0],
            "facility_name": row[1],
            "city": row[2],
            "bed_count": row[3],
            "ownership_type": row[4],
            "hospital_type": row[5],
            "has_rate_data": (row[8] or 0) > 0,
            "rate_count": row[8] or 0,
        })

    return {
        "facilities": facilities,
        "total": len(facilities),
        "with_data": sum(1 for f in facilities if f["has_rate_data"]),
    }


@router.get("/hospital-rates")
async def hospital_rates(
    ccn: str = Query(..., description="CMS Certification Number"),
    db: aiosqlite.Connection = Depends(get_db),
):
    """Get all rates for a single hospital, grouped by procedure and payer.

    Uses the facility's primary NPI only. Shows rate-to-Medicare ratios
    for each rate. This is the core "My Hospital" view.
    """
    facility = await _get_facility(db, ccn)
    if not facility:
        return {"error": "Facility not found"}

    provider_id = facility["provider_id"]
    if not provider_id:
        return {
            "facility": facility,
            "procedures": [],
            "procedure_count": 0,
            "payer_count": 0,
            "error": "No primary NPI mapped for this facility",
        }

    # Get all FFS rates with Medicare benchmarks (primary NPI only)
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

        medicare_ref = _medicare_ref(service_setting, row[11], row[9])

        procedures[code]["payer_rates"][payer_name]["rates"].append({
            "negotiated_rate": rate,
            "rate_type": rate_type,
            "service_setting": service_setting,
            "pct_medicare": _pct_medicare(rate, medicare_ref),
        })

    # Convert payer_rates dicts to lists for JSON
    procedure_list = []
    for proc in procedures.values():
        proc["payer_rates"] = list(proc["payer_rates"].values())
        procedure_list.append(proc)

    return {
        "facility": {
            "ccn": facility["ccn"],
            "name": facility["facility_name"],
            "city": facility["city"],
            "bed_count": facility["bed_count"],
            "ownership_type": facility["ownership_type"],
            "hospital_type": facility["hospital_type"],
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
    service_setting: str | None = Query(None, description="Filter: outpatient, inpatient, or ambulatory"),
    db: aiosqlite.Connection = Depends(get_db),
):
    """Show where every Iowa facility falls for a given procedure.

    Uses primary NPI per facility only — one row per CCN.
    Returns facilities ranked by median rate with percentile and % of Medicare.
    """
    # Get Medicare baseline
    cursor = await db.execute(
        "SELECT description, category, medicare_facility_rate, "
        "medicare_professional_rate, medicare_opps_rate "
        "FROM cpt_lookup WHERE code = ?",
        (billing_code,),
    )
    cpt = await cursor.fetchone()

    # Build query — only primary NPIs
    params: list = [billing_code]
    where = ["nr.billing_code = ?", "m.is_primary = 1"]
    if payer:
        where.append("py.short_name = ?")
        params.append(payer)
    if service_setting:
        where.append("LOWER(nr.service_setting) = LOWER(?)")
        params.append(service_setting)

    where_sql = " AND ".join(where)

    cursor = await db.execute(
        f"SELECT f.ccn, f.facility_name, f.city, f.bed_count, "
        f"f.ownership_type, f.hospital_type, "
        f"nr.negotiated_rate, nr.rate_type, nr.service_setting, "
        f"py.name AS payer_name "
        f"FROM normalized_rates nr "
        f"JOIN providers p ON nr.provider_id = p.id "
        f"JOIN npi_ccn_map m ON p.npi = m.npi "
        f"JOIN facilities f ON m.ccn = f.ccn "
        f"JOIN payers py ON nr.payer_id = py.id "
        f"WHERE {where_sql} "
        f"ORDER BY f.facility_name",
        params,
    )
    rows = await cursor.fetchall()

    # Group by CCN -> compute median
    facility_rates: dict[str, dict] = {}
    for row in rows:
        ccn = row[0]
        if ccn not in facility_rates:
            facility_rates[ccn] = {
                "ccn": ccn,
                "name": row[1],
                "city": row[2],
                "bed_count": row[3],
                "ownership_type": row[4],
                "hospital_type": row[5],
                "rates": [],
                "payers": set(),
            }
        facility_rates[ccn]["rates"].append(row[6])
        facility_rates[ccn]["payers"].add(row[9])

    # Compute medians and sort
    facilities = []
    for fdata in facility_rates.values():
        rates = fdata["rates"]
        med = round(statistics.median(rates), 2)

        # Pick Medicare reference
        medicare_ref = None
        ss = (service_setting or "").lower()
        if ss in ("institutional", "outpatient", "inpatient") and cpt and cpt[4]:
            medicare_ref = cpt[4]
        elif ss in ("professional", "ambulatory") and cpt and cpt[2]:
            medicare_ref = cpt[2]
        elif cpt and cpt[4]:
            medicare_ref = cpt[4]  # Default to OPPS for mixed

        facilities.append({
            "ccn": fdata["ccn"],
            "name": fdata["name"],
            "city": fdata["city"],
            "bed_count": fdata["bed_count"],
            "ownership_type": fdata["ownership_type"],
            "hospital_type": fdata["hospital_type"],
            "median_rate": med,
            "min_rate": round(min(rates), 2),
            "max_rate": round(max(rates), 2),
            "rate_count": len(rates),
            "payer_count": len(fdata["payers"]),
            "pct_medicare": _pct_medicare(med, medicare_ref),
        })

    facilities.sort(key=lambda f: f["median_rate"])

    # Compute percentile
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
    ccn: str = Query(..., description="CMS Certification Number"),
    db: aiosqlite.Connection = Depends(get_db),
):
    """Rank payers by rate-to-Medicare ratio for a specific hospital.

    Uses primary NPI only. Payer overall ratio is the MEDIAN of
    per-procedure ratios (not ratio of sums).
    """
    facility = await _get_facility(db, ccn)
    if not facility:
        return {"error": "Facility not found"}

    provider_id = facility["provider_id"]
    if not provider_id:
        return {
            "facility": facility,
            "payers": [],
            "payer_count": 0,
            "error": "No primary NPI mapped for this facility",
        }

    # Get all rates with Medicare benchmarks (primary NPI only)
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

    # Group by payer -> collect rate-to-Medicare ratios
    payer_data: dict[str, dict] = {}
    for row in rows:
        payer_name = row[5]
        rate = row[1]
        service_setting = row[3]
        medicare_facility = row[7]
        medicare_opps = row[8]

        medicare_ref = _medicare_ref(service_setting, medicare_opps, medicare_facility)

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

    # Sort by median % of Medicare (lowest first = potential underpayers)
    scorecard.sort(key=lambda s: s["median_pct_medicare"] or 0)

    return {
        "facility": {
            "ccn": facility["ccn"],
            "name": facility["facility_name"],
            "city": facility["city"],
            "bed_count": facility["bed_count"],
            "ownership_type": facility["ownership_type"],
            "hospital_type": facility["hospital_type"],
        },
        "payers": scorecard,
        "payer_count": len(scorecard),
    }
