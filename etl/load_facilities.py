"""Load Iowa hospital facilities from CMS Provider of Services (POS) file
and build NPI→CCN mapping from NPPES.

Downloads the POS file if not cached, filters to Iowa acute care and
critical access hospitals, populates the facilities table, then queries
NPPES to map NPIs to CCNs and selects a primary NPI per facility.

Usage: python -m etl.load_facilities [-v] [--skip-nppes]
"""

import argparse
import asyncio
import csv
import io
import os
import re
import sys
import time

import aiosqlite
import httpx
from dotenv import load_dotenv

load_dotenv()

DATABASE_PATH = os.getenv("DATABASE_PATH", "./data/iowa_transparency.db")

POS_URL = (
    "https://data.cms.gov/sites/default/files/2026-01/"
    "c500f848-83b3-4f29-a677-562243a2f23b/"
    "Hospital_and_other.DATA.Q4_2025.csv"
)
POS_CACHE = os.path.join("data", "cms", "pos_hospital_q4_2025.csv")

NPPES_API = "https://npiregistry.cms.hhs.gov/api/"

# Ownership type mapping from GNRL_CNTL_TYPE_CD
OWNERSHIP_MAP = {
    "01": "Nonprofit (Church)",
    "02": "Nonprofit",
    "03": "For-profit",
    "04": "For-profit",
    "05": "For-profit",
    "06": "For-profit",
    "07": "Government",
    "08": "Government",
    "09": "Government",
    "1A": "Government",
    "1B": "Government",
    "1C": "Government (Federal)",
    "2A": "Tribal",
    "2B": "Nonprofit",
    "2C": "Nonprofit",
}

# Hospital taxonomy codes we include (282N = General Acute Care)
HOSPITAL_TAXONOMIES = {
    "282N00000X",  # General Acute Care Hospital
    "282NC0060X",  # General Acute Care Hospital, Critical Access
    "282NC2000X",  # General Acute Care Hospital, Children
    "282NR1301X",  # General Acute Care Hospital, Rural
    "282NW0100X",  # General Acute Care Hospital, Women
}

# POS names differ from NPPES names. These aliases help match.
# Format: POS name substring -> list of NPPES search terms to try.
NAME_ALIASES: dict[str, list[str]] = {
    "MERCYONE": ["MERCY", "TRINITY", "GENESIS"],
    "UNITYPOINT": ["UNITY POINT", "IOWA METHODIST", "ALLEN HOSPITAL"],
    "CHI HEALTH": ["MERCY", "ALEGENT"],
    "TRINITY": ["UNITY POINT", "TRINITY"],
    "COMPASS MEMORIAL": ["MARENGO MEMORIAL"],
    "WINNMED": ["WINNESHIEK MEDICAL"],
    "HANSEN FAMILY": ["HANSEN HEALTH", "ELLSWORTH MUNICIPAL"],
    "SANFORD SHELDON": ["SANFORD", "SHELBY"],
    "MITCHELL COUNTY REGIONAL": ["MITCHELL COUNTY"],
    "GEORGE C GRAPE": ["GRAPE COMMUNITY"],
    "AVERA MERRILL PIONEER": ["AVERA", "MERRILL PIONEER"],
    "VAN DIEST": ["VAN DIEST"],
    "CHEROKEE REGIONAL": ["CHEROKEE"],
    "FLOYD COUNTY MEDICAL": ["FLOYD COUNTY"],
    "OSCEOLA COMMUNITY": ["OSCEOLA"],
    "OTTUMWA REGIONAL": ["OTTUMWA"],
    "HORN MEMORIAL": ["HORN MEMORIAL"],
    "MAHASKA": ["MAHASKA"],
    "BUCHANAN COUNTY HEALTH": ["BUCHANAN COUNTY"],
    "JONES REGIONAL": ["JONES REGIONAL"],
    "COMMUNITY MEMORIAL HOSPITAL MEDICAL": ["COMMUNITY MEMORIAL", "SUMNER"],
    "REGIONAL MEDICAL CENTER": ["DELAWARE COUNTY", "REGIONAL MEDICAL"],
    "UNIVERSITY OF IOWA HEALTH CARE MEDICAL CENTER DOWN": ["UNIVERSITY OF IOWA"],
    "ST LUKES HOSPITAL": ["ST LUKE", "SAINT LUKE"],
    "MERCY MEDICAL CENTER - CEDAR RAPIDS": ["MERCY MEDICAL CENTER"],
    "UNITYPOINT HEALTH - DES MOINES": ["IOWA METHODIST", "IOWA LUTHERAN"],
    "MERCYONE GENESIS": ["GENESIS HEALTH", "GENESIS MEDICAL"],
    "CHI HEALTH - MERCY CORNING": ["MERCY HOSPITAL", "CORNING"],
    "JONES REGIONAL MEDICAL CENTER": ["JONES REGIONAL", "ANAMOSA"],
    "MERCYONE GENESIS DEWITT": ["GENESIS", "DEWITT"],
    "GEORGE C GRAPE": ["GRAPE COMMUNITY", "GRAPE HOSPITAL"],
    "MERCYONE NEW HAMPTON": ["MERCY", "NEW HAMPTON"],
    "OSCEOLA COMMUNITY HOSPITAL": ["OSCEOLA", "SIBLEY"],
    "FLOYD COUNTY MEDICAL CENTER": ["FLOYD COUNTY", "CHARLES CITY"],
    "VAN DIEST MEDICAL CENTER": ["VAN DIEST"],
    "CHEROKEE REGIONAL MEDICAL": ["CHEROKEE"],
    "MAHASKA HEALTH": ["MAHASKA"],
    "ST ANTHONY REGIONAL": ["ST ANTHONY", "SAINT ANTHONY"],
}


def _download_pos_file(verbose: bool = False) -> str:
    """Download POS file if not cached. Returns path to local CSV."""
    if os.path.exists(POS_CACHE):
        if verbose:
            print(f"Using cached POS file: {POS_CACHE}")
        return POS_CACHE

    os.makedirs(os.path.dirname(POS_CACHE), exist_ok=True)
    if verbose:
        print(f"Downloading POS file from CMS...")

    with httpx.Client(timeout=120) as client:
        r = client.get(POS_URL)
        r.raise_for_status()

    with open(POS_CACHE, "w", encoding="utf-8") as f:
        f.write(r.text)

    if verbose:
        print(f"  Saved to {POS_CACHE} ({len(r.text)} bytes)")
    return POS_CACHE


def parse_pos_file(path: str, verbose: bool = False) -> list[dict]:
    """Parse POS CSV and return Iowa acute care + critical access hospitals."""
    hospitals = []

    with open(path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get("STATE_CD", "").strip() != "IA":
                continue
            # Only active (termination code 00)
            if row.get("PGM_TRMNTN_CD", "").strip() != "00":
                continue

            ccn = row["PRVDR_NUM"].strip()
            # Filter to hospital CCN ranges:
            # 160001-160899 = Short-term acute care
            # 161300-161399 = Critical Access Hospitals
            if not re.match(r"^16\d{4}$", ccn):
                continue
            num = int(ccn[2:])
            if not ((1 <= num <= 899) or (1300 <= num <= 1399)):
                continue

            hospital_type = (
                "Critical Access" if num >= 1300 else "Acute Care"
            )
            ownership = OWNERSHIP_MAP.get(
                row.get("GNRL_CNTL_TYPE_CD", ""), "Unknown"
            )
            bed_count = int(row.get("BED_CNT", "0") or "0")

            hospitals.append({
                "ccn": ccn,
                "facility_name": row["FAC_NAME"].strip(),
                "address": row.get("ST_ADR", "").strip(),
                "city": row.get("CITY_NAME", "").strip(),
                "zip_code": row.get("ZIP_CD", "").strip()[:5],
                "bed_count": bed_count,
                "ownership_type": ownership,
                "hospital_type": hospital_type,
            })

    if verbose:
        print(f"Parsed {len(hospitals)} Iowa hospitals from POS file")
        acute = sum(1 for h in hospitals if h["hospital_type"] == "Acute Care")
        cah = sum(1 for h in hospitals if h["hospital_type"] == "Critical Access")
        print(f"  Acute Care: {acute}, Critical Access: {cah}")

    return hospitals


async def _load_facilities(
    db: aiosqlite.Connection, hospitals: list[dict], verbose: bool = False
) -> None:
    """Insert/update facilities table from POS data."""
    for h in hospitals:
        await db.execute(
            "INSERT INTO facilities "
            "(ccn, facility_name, address, city, zip_code, bed_count, "
            " ownership_type, hospital_type, active) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1) "
            "ON CONFLICT(ccn) DO UPDATE SET "
            "facility_name=excluded.facility_name, "
            "address=excluded.address, city=excluded.city, "
            "zip_code=excluded.zip_code, bed_count=excluded.bed_count, "
            "ownership_type=excluded.ownership_type, "
            "hospital_type=excluded.hospital_type, active=1",
            (
                h["ccn"], h["facility_name"], h["address"], h["city"],
                h["zip_code"], h["bed_count"], h["ownership_type"],
                h["hospital_type"],
            ),
        )
    await db.commit()
    if verbose:
        print(f"Loaded {len(hospitals)} facilities into DB")


def _normalize_name(name: str) -> str:
    """Normalize a facility name for fuzzy matching."""
    name = name.upper().strip()
    # Remove common suffixes/noise
    for noise in [
        "INC", "LLC", "CORP", "CORPORATION",
        "D/B/A", "DBA", "THE", "OF",
        ",", ".", "'", '"', "(", ")",
    ]:
        name = name.replace(noise, " ")
    # Collapse whitespace
    return " ".join(name.split())


def _normalize_address(addr: str) -> str:
    """Normalize street address for matching."""
    addr = addr.upper().strip()
    replacements = {
        "STREET": "ST", "AVENUE": "AVE", "BOULEVARD": "BLVD",
        "DRIVE": "DR", "ROAD": "RD", "LANE": "LN",
        "NORTH": "N", "SOUTH": "S", "EAST": "E", "WEST": "W",
    }
    for full, abbr in replacements.items():
        addr = addr.replace(full, abbr)
    return " ".join(addr.split())


async def _query_nppes_for_facility(
    client: httpx.AsyncClient,
    facility: dict,
    verbose: bool = False,
) -> list[dict]:
    """Query NPPES API for NPIs matching a facility.

    Searches by organization name + state + city. Falls back to alias
    names and wildcard city searches. Returns list of matching NPI records.
    """
    results = []

    # Build list of search names: exact name first, then aliases
    search_names = [facility["facility_name"]]

    # Add alias-based alternatives
    fac_upper = facility["facility_name"].upper()
    for prefix, aliases in NAME_ALIASES.items():
        if prefix.upper() in fac_upper:
            search_names.extend(aliases)

    all_raw_results = []
    for name in search_names:
        params = {
            "version": "2.1",
            "organization_name": name,
            "state": "IA",
            "city": facility["city"],
            "limit": 50,
        }
        try:
            r = await client.get(NPPES_API, params=params)
            r.raise_for_status()
            data = r.json()
            all_raw_results.extend(data.get("results", []))
        except Exception:
            pass
        await asyncio.sleep(0.3)

    # Also try city-only search with wildcard name if nothing found yet
    if not all_raw_results:
        # Search for any hospital-type org in this city
        params = {
            "version": "2.1",
            "organization_name": "*",
            "state": "IA",
            "city": facility["city"],
            "taxonomy_description": "hospital",
            "limit": 100,
        }
        try:
            r = await client.get(NPPES_API, params=params)
            r.raise_for_status()
            data = r.json()
            all_raw_results.extend(data.get("results", []))
        except Exception:
            pass

    # Deduplicate by NPI
    seen_npis: set[str] = set()
    for result in all_raw_results:
        npi_num = str(result.get("number", ""))
        if npi_num in seen_npis:
            continue
        seen_npis.add(npi_num)

        basic = result.get("basic", {})
        taxonomies = result.get("taxonomies", [])
        addresses = result.get("addresses", [])

        npi = npi_num
        org_name = basic.get("organization_name", "")
        enum_date = basic.get("enumeration_date", "")
        is_subpart = basic.get("organizational_subpart", "").upper() == "YES"

        # Get primary taxonomy
        primary_tax = ""
        is_hospital_taxonomy = False
        for t in taxonomies:
            code = t.get("code", "")
            if t.get("primary", False):
                primary_tax = code
            if code in HOSPITAL_TAXONOMIES or code[:4] == "282N":
                is_hospital_taxonomy = True
                if not primary_tax:
                    primary_tax = code

        # Get location address for matching
        loc_addr = next(
            (a for a in addresses if a.get("address_purpose") == "LOCATION"),
            {},
        )

        results.append({
            "npi": npi,
            "organization_name": org_name,
            "taxonomy_code": primary_tax,
            "is_hospital_taxonomy": is_hospital_taxonomy,
            "is_subpart": is_subpart,
            "enumeration_date": enum_date,
            "city": loc_addr.get("city", ""),
            "address": loc_addr.get("address_1", ""),
            "zip": loc_addr.get("postal_code", "")[:5],
        })

    return results


def _match_npi_to_facility(
    npi_record: dict, facility: dict
) -> tuple[bool, float]:
    """Score how well an NPI record matches a facility.

    Returns (is_match, score). Higher score = better match.
    """
    score = 0.0

    # Must be in the same city
    npi_city = npi_record["city"].upper().strip()
    fac_city = facility["city"].upper().strip()
    if npi_city != fac_city:
        return False, 0.0

    # Taxonomy match is critical
    if npi_record["is_hospital_taxonomy"]:
        score += 50.0
    else:
        # Non-hospital taxonomy (clinic, nurse practitioner, etc.)
        # Still allow matching but with much lower score
        score += 5.0

    # Name similarity
    n1 = _normalize_name(npi_record["organization_name"])
    n2 = _normalize_name(facility["facility_name"])
    if n1 == n2:
        score += 30.0
    elif n1 in n2 or n2 in n1:
        score += 20.0
    else:
        # Check word overlap
        words1 = set(n1.split())
        words2 = set(n2.split())
        overlap = words1 & words2
        # Common noise words that don't indicate a real match
        noise = {"HOSPITAL", "MEDICAL", "CENTER", "HEALTH", "COUNTY",
                 "REGIONAL", "COMMUNITY", "MEMORIAL", "CARE", "SYSTEM"}
        meaningful_overlap = overlap - noise
        if meaningful_overlap:
            score += 15.0 + 5.0 * len(meaningful_overlap)
        elif len(overlap) >= 2:
            score += 10.0 * (len(overlap) / max(len(words1), len(words2)))
        else:
            # For hospitals with very different names (MercyOne vs Genesis),
            # still match if it's the ONLY hospital-taxonomy org in that city
            if npi_record["is_hospital_taxonomy"]:
                score += 5.0  # weak name match but correct city + taxonomy
            else:
                return False, 0.0

    # Address similarity bonus
    a1 = _normalize_address(npi_record["address"])
    a2 = _normalize_address(facility["address"])
    if a1 and a2:
        if a1 == a2:
            score += 10.0
        elif a1[:10] == a2[:10]:
            score += 5.0

    # ZIP match bonus
    if npi_record["zip"] and facility["zip_code"]:
        if npi_record["zip"] == facility["zip_code"]:
            score += 5.0

    return score >= 20.0, score


async def _build_npi_mapping(
    db: aiosqlite.Connection,
    hospitals: list[dict],
    verbose: bool = False,
) -> dict[str, list[dict]]:
    """Query NPPES for each facility and build NPI→CCN mapping.

    Returns dict of ccn -> list of NPI records.
    """
    # Also check existing providers table for NPIs we already have
    cursor = await db.execute(
        "SELECT npi, id, name, city FROM providers WHERE state = 'IA' AND npi IS NOT NULL"
    )
    existing_providers = {row[0]: row for row in await cursor.fetchall()}

    ccn_npis: dict[str, list[dict]] = {}
    total_mapped = 0
    unmapped_count = 0

    async with httpx.AsyncClient(timeout=30) as client:
        for i, facility in enumerate(hospitals):
            ccn = facility["ccn"]
            if verbose and i % 10 == 0:
                print(f"  Querying NPPES: {i+1}/{len(hospitals)}...")

            # Query NPPES
            npi_records = await _query_nppes_for_facility(
                client, facility, verbose
            )

            # Rate limit: CMS allows ~2 req/sec
            await asyncio.sleep(0.5)

            # Score and filter matches
            matches = []
            for rec in npi_records:
                is_match, score = _match_npi_to_facility(rec, facility)
                if is_match:
                    # Look up existing provider_id
                    existing = existing_providers.get(rec["npi"])
                    rec["provider_id"] = existing[1] if existing else None
                    rec["match_score"] = score
                    matches.append(rec)

            if matches:
                ccn_npis[ccn] = matches
                total_mapped += len(matches)
            else:
                unmapped_count += 1
                if verbose:
                    print(
                        f"  WARNING: No NPI match for {ccn} "
                        f"{facility['facility_name']} ({facility['city']})"
                    )

    if verbose:
        print(f"\nNPI mapping complete:")
        print(f"  Facilities with NPIs: {len(ccn_npis)}/{len(hospitals)}")
        print(f"  Total NPIs mapped: {total_mapped}")
        print(f"  Facilities with no match: {unmapped_count}")

    return ccn_npis


async def _select_primary_npis(
    db: aiosqlite.Connection,
    ccn_npis: dict[str, list[dict]],
    verbose: bool = False,
) -> None:
    """For each CCN, select the primary NPI and store the mapping.

    Priority:
    1. Non-subpart (parent org NPI) with hospital taxonomy
    2. Most rate records in existing MRF data
    3. Earliest enumeration date
    """
    # Clear existing mapping
    await db.execute("DELETE FROM npi_ccn_map")

    for ccn, npis in ccn_npis.items():
        # Get rate counts for each NPI from existing data
        for npi_rec in npis:
            cursor = await db.execute(
                "SELECT COUNT(*) FROM normalized_rates nr "
                "JOIN providers p ON nr.provider_id = p.id "
                "WHERE p.npi = ?",
                (npi_rec["npi"],),
            )
            npi_rec["rate_count"] = (await cursor.fetchone())[0]

        # Sort by priority:
        # 1. Hospital taxonomy first
        # 2. Non-subpart first
        # 3. Most rates
        # 4. Earliest enumeration date
        npis.sort(
            key=lambda n: (
                -int(n["is_hospital_taxonomy"]),
                int(n["is_subpart"]),
                -n["rate_count"],
                n.get("enumeration_date", "9999"),
            )
        )

        primary_npi = npis[0]["npi"]

        for npi_rec in npis:
            is_primary = 1 if npi_rec["npi"] == primary_npi else 0
            await db.execute(
                "INSERT OR REPLACE INTO npi_ccn_map "
                "(npi, ccn, taxonomy_code, is_subpart, is_primary, "
                " enumeration_date, provider_id) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    npi_rec["npi"],
                    ccn,
                    npi_rec["taxonomy_code"],
                    1 if npi_rec["is_subpart"] else 0,
                    is_primary,
                    npi_rec.get("enumeration_date"),
                    npi_rec.get("provider_id"),
                ),
            )

    await db.commit()

    if verbose:
        # Report primary NPI selection
        cursor = await db.execute(
            "SELECT COUNT(*) FROM npi_ccn_map WHERE is_primary = 1"
        )
        primary_count = (await cursor.fetchone())[0]
        cursor = await db.execute("SELECT COUNT(*) FROM npi_ccn_map")
        total_count = (await cursor.fetchone())[0]
        cursor = await db.execute(
            "SELECT COUNT(*) FROM npi_ccn_map "
            "WHERE is_primary = 1 AND provider_id IS NOT NULL"
        )
        with_rates = (await cursor.fetchone())[0]
        print(f"\nPrimary NPI selection:")
        print(f"  Primary NPIs: {primary_count}")
        print(f"  Total NPI mappings: {total_count}")
        print(f"  Primary NPIs with existing rate data: {with_rates}")


async def _run_data_quality_checks(
    db: aiosqlite.Connection, verbose: bool = False
) -> None:
    """Run data quality checks and log issues."""
    # Clear old logs
    await db.execute("DELETE FROM data_quality_log")

    # 1. Unmapped NPIs: providers in our DB not linked to any facility
    cursor = await db.execute(
        "SELECT p.npi, p.name, p.city FROM providers p "
        "WHERE p.state = 'IA' AND p.npi IS NOT NULL "
        "AND p.npi NOT IN (SELECT npi FROM npi_ccn_map)"
    )
    unmapped = await cursor.fetchall()
    for row in unmapped:
        await db.execute(
            "INSERT INTO data_quality_log (category, npi, detail) "
            "VALUES ('unmapped_npi', ?, ?)",
            (row[0], f"Provider '{row[1]}' in {row[2]} not mapped to any CCN"),
        )

    # 2. Facilities with no MRF data
    cursor = await db.execute(
        "SELECT f.ccn, f.facility_name FROM facilities f "
        "WHERE f.ccn NOT IN ("
        "  SELECT DISTINCT m.ccn FROM npi_ccn_map m "
        "  JOIN providers p ON m.npi = p.npi "
        "  JOIN normalized_rates nr ON nr.provider_id = p.id"
        ")"
    )
    no_data = await cursor.fetchall()
    for row in no_data:
        await db.execute(
            "INSERT INTO data_quality_log (category, ccn, detail) "
            "VALUES ('no_mrf_data', ?, ?)",
            (row[0], f"Facility '{row[1]}' has no MRF rate data"),
        )

    # 3. Conflicting rates: non-primary NPI has rates >20% different
    cursor = await db.execute(
        "SELECT m1.ccn, m1.npi AS primary_npi, m2.npi AS secondary_npi, "
        "nr1.billing_code, nr1.negotiated_rate AS primary_rate, "
        "nr2.negotiated_rate AS secondary_rate, py.name AS payer_name "
        "FROM npi_ccn_map m1 "
        "JOIN npi_ccn_map m2 ON m1.ccn = m2.ccn AND m2.is_primary = 0 "
        "JOIN providers p1 ON m1.npi = p1.npi "
        "JOIN providers p2 ON m2.npi = p2.npi "
        "JOIN normalized_rates nr1 ON nr1.provider_id = p1.id "
        "JOIN normalized_rates nr2 ON nr2.provider_id = p2.id "
        "  AND nr1.billing_code = nr2.billing_code "
        "  AND nr1.payer_id = nr2.payer_id "
        "JOIN payers py ON nr1.payer_id = py.id "
        "WHERE m1.is_primary = 1 "
        "AND ABS(nr1.negotiated_rate - nr2.negotiated_rate) "
        "    > 0.20 * nr1.negotiated_rate "
        "LIMIT 500"
    )
    conflicts = await cursor.fetchall()
    for row in conflicts:
        await db.execute(
            "INSERT INTO data_quality_log "
            "(category, ccn, npi, billing_code, payer_name, detail) "
            "VALUES ('conflicting_rate', ?, ?, ?, ?, ?)",
            (
                row[0], row[2], row[3], row[6],
                f"Primary NPI {row[1]} rate=${row[4]:.2f} vs "
                f"secondary NPI {row[2]} rate=${row[5]:.2f}",
            ),
        )

    # 4. Missing primary rates: secondary NPI has rates but primary doesn't
    cursor = await db.execute(
        "SELECT m1.ccn, m2.npi, nr.billing_code, py.name "
        "FROM npi_ccn_map m1 "
        "JOIN npi_ccn_map m2 ON m1.ccn = m2.ccn AND m2.is_primary = 0 "
        "JOIN providers p2 ON m2.npi = p2.npi "
        "JOIN normalized_rates nr ON nr.provider_id = p2.id "
        "JOIN payers py ON nr.payer_id = py.id "
        "WHERE m1.is_primary = 1 "
        "AND NOT EXISTS ("
        "  SELECT 1 FROM providers p1 "
        "  JOIN normalized_rates nr1 ON nr1.provider_id = p1.id "
        "  WHERE p1.npi = m1.npi "
        "  AND nr1.billing_code = nr.billing_code "
        "  AND nr1.payer_id = nr.payer_id"
        ") "
        "LIMIT 500"
    )
    missing = await cursor.fetchall()
    for row in missing:
        await db.execute(
            "INSERT INTO data_quality_log "
            "(category, ccn, npi, billing_code, payer_name, detail) "
            "VALUES ('missing_primary_rate', ?, ?, ?, ?, ?)",
            (
                row[0], row[1], row[2], row[3],
                f"Secondary NPI {row[1]} has rate for {row[2]}/{row[3]} "
                f"but primary NPI does not",
            ),
        )

    await db.commit()

    if verbose:
        cursor = await db.execute(
            "SELECT category, COUNT(*) FROM data_quality_log GROUP BY category"
        )
        rows = await cursor.fetchall()
        print(f"\nData quality summary:")
        for row in rows:
            print(f"  {row[0]}: {row[1]} issues")


async def load_facilities(
    db_path: str | None = None,
    verbose: bool = False,
    skip_nppes: bool = False,
) -> None:
    """Main entry point: load POS data, build NPI mapping, run QA checks."""
    path = db_path or DATABASE_PATH

    # Step 1: Download and parse POS file
    pos_path = _download_pos_file(verbose)
    hospitals = parse_pos_file(pos_path, verbose)

    if not hospitals:
        print("ERROR: No Iowa hospitals found in POS file")
        return

    # Step 2: Load into DB
    db = await aiosqlite.connect(path)
    try:
        await db.execute("PRAGMA journal_mode=WAL")
        await db.execute("PRAGMA foreign_keys=ON")

        await _load_facilities(db, hospitals, verbose)

        if not skip_nppes:
            # Step 3: Build NPI→CCN mapping
            ccn_npis = await _build_npi_mapping(db, hospitals, verbose)

            # Step 4: Select primary NPIs
            await _select_primary_npis(db, ccn_npis, verbose)

            # Step 5: Data quality checks
            await _run_data_quality_checks(db, verbose)
        else:
            if verbose:
                print("Skipping NPPES queries (--skip-nppes)")

    finally:
        await db.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Load Iowa hospital facilities from CMS POS file"
    )
    parser.add_argument("-v", "--verbose", action="store_true")
    parser.add_argument(
        "--skip-nppes",
        action="store_true",
        help="Skip NPPES API queries (just load POS data)",
    )
    args = parser.parse_args()
    asyncio.run(load_facilities(verbose=args.verbose, skip_nppes=args.skip_nppes))
