"""Extract TINs for Iowa providers from MRF data.

Payers like Aetna use Type 2 (organizational) NPIs that match our providers
table. The (NPI, TIN) pairs from their MRF files let us populate providers.tin,
which is then used for TIN-based matching against UHC data.

Strategy:
  1. Try stored MRF URLs from mrf_files table (fast, no adapter needed).
  2. If all stored URLs are expired, re-discover a fresh URL via the TOC adapter.

Usage: python -m etl.extract_tins [--source aetna] [--dry-run] [-v]
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import tempfile
import zlib

import aiosqlite
import httpx
import ijson
from dotenv import load_dotenv

load_dotenv()

DATABASE_PATH = os.getenv("DATABASE_PATH", "./data/iowa_transparency.db")

logger = logging.getLogger(__name__)


async def _download_and_parse_tins(
    mrf_url: str,
    iowa_npis: set[str],
    timeout: httpx.Timeout,
) -> dict[str, str]:
    """Download an MRF file and extract {npi: tin} for Iowa NPIs.

    Parses only provider_references (Phase 1), then stops.
    Raises on download/parse failure.
    """
    npi_tin_map: dict[str, str] = {}
    buf = tempfile.SpooledTemporaryFile(max_size=256 * 1024 * 1024)

    try:
        downloaded_bytes = 0
        async with httpx.AsyncClient(follow_redirects=True, timeout=timeout) as client:
            async with client.stream("GET", mrf_url) as response:
                response.raise_for_status()

                content_type = response.headers.get("content-type", "")
                if "text/html" in content_type:
                    raise ValueError(
                        f"Expected JSON but got HTML (content-type: {content_type}). "
                        f"The download URL may have expired."
                    )

                url_path = mrf_url.split("?")[0]
                if url_path.endswith(".gz"):
                    decompressor = zlib.decompressobj(zlib.MAX_WBITS | 16)
                    async for chunk in response.aiter_bytes(chunk_size=65536):
                        downloaded_bytes += len(chunk)
                        data = chunk
                        while data:
                            buf.write(decompressor.decompress(data))
                            if decompressor.eof:
                                data = decompressor.unused_data
                                decompressor = zlib.decompressobj(zlib.MAX_WBITS | 16)
                            else:
                                break
                    buf.write(decompressor.flush())
                else:
                    async for chunk in response.aiter_bytes(chunk_size=65536):
                        downloaded_bytes += len(chunk)
                        buf.write(chunk)

        buf.seek(0)
        logger.info("Downloaded %d MB", downloaded_bytes // (1024 * 1024))

        current_entry_npis: list[str] = []
        current_tin: str = ""

        for prefix, event, value in ijson.parse(buf):
            if prefix == "provider_references.item.provider_groups.item" and event == "start_map":
                current_entry_npis = []
                current_tin = ""
            elif prefix == "provider_references.item.provider_groups.item.npi.item":
                current_entry_npis.append(str(int(value)))
            elif prefix == "provider_references.item.provider_groups.item.tin.value":
                current_tin = str(value)
            elif prefix == "provider_references.item.provider_groups.item" and event == "end_map":
                if current_tin:
                    for npi in current_entry_npis:
                        if npi in iowa_npis and npi not in npi_tin_map:
                            npi_tin_map[npi] = current_tin
            elif prefix == "in_network" and event == "start_array":
                break
    finally:
        buf.close()

    return npi_tin_map


async def extract_tins(
    db_path: str | None = None,
    source_payer: str = "aetna",
    dry_run: bool = False,
) -> dict:
    """Extract TINs from MRF data and populate providers.tin.

    Returns summary dict with counts.
    """
    path = db_path or DATABASE_PATH

    db = await aiosqlite.connect(path)
    db.row_factory = aiosqlite.Row
    await db.execute("PRAGMA journal_mode=WAL")
    await db.execute("PRAGMA foreign_keys=ON")

    try:
        # 1. Load Iowa NPIs from providers table
        cursor = await db.execute(
            "SELECT npi, id, name FROM providers WHERE state = 'IA' AND npi IS NOT NULL"
        )
        providers = await cursor.fetchall()
        npi_to_provider = {str(row["npi"]): (int(row["id"]), row["name"]) for row in providers}
        iowa_npis = set(npi_to_provider.keys())
        logger.info("Loaded %d Iowa NPIs to match", len(iowa_npis))

        # 2. Build candidate URLs: stored URLs first, then fresh discovery as fallback
        candidate_urls: list[str] = []

        # 2a. Stored URLs from mrf_files (may be expired)
        cursor = await db.execute(
            "SELECT mf.url FROM mrf_files mf "
            "JOIN payers p ON mf.payer_id = p.id "
            "WHERE p.short_name = ? AND mf.status = 'completed' "
            "AND mf.records_extracted > 0 "
            "ORDER BY mf.records_extracted DESC LIMIT 5",
            (source_payer,),
        )
        stored_rows = await cursor.fetchall()
        candidate_urls.extend(row["url"] for row in stored_rows)
        logger.info("Found %d stored MRF URLs to try", len(candidate_urls))

        # 3. Try each candidate URL
        npi_tin_map: dict[str, str] = {}
        timeout = httpx.Timeout(connect=60.0, read=120.0, write=30.0, pool=None)
        all_stored_failed = True

        for url_idx, mrf_url in enumerate(candidate_urls, 1):
            logger.info("[%d/%d] Trying stored URL: %s", url_idx, len(candidate_urls), mrf_url[:120])
            try:
                npi_tin_map = await _download_and_parse_tins(mrf_url, iowa_npis, timeout)
                all_stored_failed = False
                break
            except (httpx.HTTPStatusError, httpx.TimeoutException, ValueError, zlib.error) as e:
                logger.warning("  Failed: %s", e)
                continue

        # 4. If all stored URLs failed, re-discover fresh URLs via the TOC adapter
        if all_stored_failed:
            logger.info("All stored URLs expired. Re-discovering via %s adapter...", source_payer)
            try:
                from etl.toc_adapters import get_mrf_file_list

                cursor = await db.execute(
                    "SELECT id, name, short_name, toc_url FROM payers WHERE short_name = ?",
                    (source_payer,),
                )
                payer_row = await cursor.fetchone()
                if not payer_row:
                    return {"error": f"Unknown payer: {source_payer}"}

                payer = {
                    "id": payer_row["id"],
                    "name": payer_row["name"],
                    "short_name": payer_row["short_name"],
                    "toc_url": payer_row["toc_url"],
                }

                fresh_files = await get_mrf_file_list(payer)
                if not fresh_files:
                    return {"error": f"No MRF files discovered for {source_payer}"}

                # Try the first 3 fresh URLs (smallest files first = fastest)
                fresh_files_sorted = sorted(fresh_files, key=lambda f: len(f.url))
                for url_idx, mrf_info in enumerate(fresh_files_sorted[:3], 1):
                    logger.info(
                        "[fresh %d/3] Trying: %s", url_idx, mrf_info.url[:120]
                    )
                    try:
                        npi_tin_map = await _download_and_parse_tins(
                            mrf_info.url, iowa_npis, timeout
                        )
                        break
                    except (httpx.HTTPStatusError, httpx.TimeoutException, ValueError, zlib.error) as e:
                        logger.warning("  Failed: %s", e)
                        continue

            except Exception as e:
                logger.error("Adapter re-discovery failed: %s", e)
                return {"error": f"All stored URLs expired and adapter failed: {e}"}

        if not npi_tin_map:
            return {"error": f"Could not extract any TINs for {source_payer}"}

        logger.info("Found TINs for %d / %d Iowa NPIs", len(npi_tin_map), len(iowa_npis))

        # 5. Update providers table
        updated = 0
        for npi, tin in npi_tin_map.items():
            provider_id, provider_name = npi_to_provider[npi]
            if dry_run:
                logger.info("  [DRY RUN] %s (NPI %s) → TIN %s", provider_name, npi, tin)
                updated += 1
            else:
                cursor = await db.execute(
                    "UPDATE providers SET tin = ? WHERE id = ? AND (tin IS NULL OR tin = '')",
                    (tin, provider_id),
                )
                if cursor.rowcount > 0:
                    updated += 1
                    logger.info("  Updated %s (NPI %s) → TIN %s", provider_name, npi, tin)

        if not dry_run:
            await db.commit()

        summary = {
            "source_payer": source_payer,
            "iowa_npis_total": len(iowa_npis),
            "tins_found": len(npi_tin_map),
            "providers_updated": updated,
            "dry_run": dry_run,
        }
        logger.info("TIN extraction complete: %s", summary)
        return summary

    finally:
        await db.close()


def main():
    parser = argparse.ArgumentParser(
        description="Extract TINs for Iowa providers from MRF data"
    )
    parser.add_argument(
        "--source", default="aetna",
        help="Source payer to extract TINs from (default: aetna)",
    )
    parser.add_argument("--dry-run", action="store_true", help="Show what would be updated")
    parser.add_argument("-v", "--verbose", action="store_true", help="Verbose logging")

    args = parser.parse_args()

    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    summary = asyncio.run(extract_tins(source_payer=args.source, dry_run=args.dry_run))

    print(f"\nTIN extraction summary:")
    for k, v in summary.items():
        print(f"  {k}: {v}")


if __name__ == "__main__":
    main()
