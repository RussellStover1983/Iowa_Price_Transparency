"""Load real Iowa hospital NPIs from the NPPES NPI Registry API.

Queries the NPPES API (v2.1) for Iowa organizations across multiple hospital
taxonomy codes, deduplicates by NPI, and inserts into the providers table.

Run directly: python -m etl.load_iowa_npis
"""

from __future__ import annotations

import asyncio
import logging
import os

import aiosqlite
import httpx
from dotenv import load_dotenv

load_dotenv()

DATABASE_PATH = os.getenv("DATABASE_PATH", "./data/iowa_transparency.db")

logger = logging.getLogger(__name__)

NPPES_API_URL = "https://npiregistry.cms.hhs.gov/api/?version=2.1"

# Hospital taxonomy codes
TAXONOMY_CODES = [
    ("282N00000X", "General Acute Care Hospital"),
    ("282NC0060X", "Critical Access Hospital"),
    ("283X00000X", "Rehabilitation Hospital"),
    ("283Q00000X", "Psychiatric Hospital"),
    ("282E00000X", "Long Term Care Hospital"),
]

PAGE_LIMIT = 200  # NPPES max per request


async def _fetch_npis_for_taxonomy(
    client: httpx.AsyncClient,
    taxonomy_code: str,
    taxonomy_label: str,
) -> list[dict]:
    """Paginate through NPPES API for a single taxonomy code in Iowa."""
    results = []
    skip = 0

    while True:
        params = {
            "enumeration_type": "NPI-2",  # Organizations only
            "state": "IA",
            "taxonomy_description": taxonomy_code,
            "limit": PAGE_LIMIT,
            "skip": skip,
        }
        try:
            resp = await client.get(NPPES_API_URL, params=params)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            logger.error(
                "NPPES API error for %s (skip=%d): %s", taxonomy_code, skip, e
            )
            break

        result_count = data.get("result_count", 0)
        if result_count == 0:
            break

        for item in data.get("results", []):
            npi = str(item.get("number", ""))
            basic = item.get("basic", {})
            org_name = basic.get("organization_name", basic.get("name", "Unknown"))

            # Get the primary practice address
            addresses = item.get("addresses", [])
            practice_addr = None
            for addr in addresses:
                if addr.get("address_purpose") == "LOCATION":
                    practice_addr = addr
                    break
            if not practice_addr and addresses:
                practice_addr = addresses[0]

            city = ""
            state = "IA"
            zip_code = ""
            address_line = ""
            if practice_addr:
                address_line = practice_addr.get("address_1", "")
                city = practice_addr.get("city", "")
                state = practice_addr.get("state", "IA")
                zip_code = practice_addr.get("postal_code", "")[:5]

            results.append({
                "npi": npi,
                "name": org_name,
                "facility_type": taxonomy_label,
                "address": address_line,
                "city": city,
                "state": state,
                "zip_code": zip_code,
            })

        logger.debug(
            "  %s: fetched %d (skip=%d, total so far=%d)",
            taxonomy_code, result_count, skip, len(results),
        )

        if result_count < PAGE_LIMIT:
            break
        skip += PAGE_LIMIT
        if skip >= 1200:  # NPPES hard cap
            logger.warning("Hit NPPES pagination cap for %s", taxonomy_code)
            break

    return results


async def load_iowa_npis(db_path: str | None = None) -> int:
    """Fetch Iowa hospital NPIs from NPPES and insert into providers table.

    Returns the number of new providers inserted.
    """
    path = db_path or DATABASE_PATH

    # Fetch from NPPES API
    all_providers: dict[str, dict] = {}  # keyed by NPI for dedup

    async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
        for taxonomy_code, taxonomy_label in TAXONOMY_CODES:
            logger.info("Querying NPPES for %s (%s)...", taxonomy_label, taxonomy_code)
            results = await _fetch_npis_for_taxonomy(
                client, taxonomy_code, taxonomy_label
            )
            for provider in results:
                npi = provider["npi"]
                if npi not in all_providers:
                    all_providers[npi] = provider
            logger.info(
                "  Found %d NPIs for %s (total unique: %d)",
                len(results), taxonomy_label, len(all_providers),
            )

    logger.info("Total unique Iowa hospital NPIs: %d", len(all_providers))

    # Insert into database
    db = await aiosqlite.connect(path)
    try:
        await db.execute("PRAGMA foreign_keys=ON")

        inserted = 0
        for provider in all_providers.values():
            cursor = await db.execute(
                "INSERT OR IGNORE INTO providers "
                "(npi, name, facility_type, address, city, state, zip_code) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    provider["npi"],
                    provider["name"],
                    provider["facility_type"],
                    provider["address"],
                    provider["city"],
                    provider["state"],
                    provider["zip_code"],
                ),
            )
            if cursor.rowcount > 0:
                inserted += 1

        await db.commit()

        cursor = await db.execute("SELECT COUNT(*) FROM providers WHERE state = 'IA'")
        total = (await cursor.fetchone())[0]
        print(f"Inserted {inserted} new Iowa providers ({total} total in DB)")
        return inserted

    finally:
        await db.close()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )
    asyncio.run(load_iowa_npis())
