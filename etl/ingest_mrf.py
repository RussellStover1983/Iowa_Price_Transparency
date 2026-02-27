"""CLI orchestrator for MRF ingestion pipeline.

Ties together provider_match, toc_parser, and mrf_stream to download,
parse, and load real MRF data into the database.

Usage:
    python -m etl.ingest_mrf --payer uhc [--limit 5] [--url URL] [--dry-run] [--list-payers] [-v]
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
from datetime import datetime, timezone

import aiosqlite
from dotenv import load_dotenv

from etl.mrf_stream import MrfStreamProcessor
from etl.provider_match import ProviderMatcher
from etl.toc_adapters import get_mrf_file_list
from etl.toc_parser import MrfFileInfo, compute_url_hash

load_dotenv()

DATABASE_PATH = os.getenv("DATABASE_PATH", "./data/iowa_transparency.db")

logger = logging.getLogger(__name__)


async def get_payer(db: aiosqlite.Connection, short_name: str) -> dict:
    """Look up a payer by short_name. Raises ValueError if not found."""
    cursor = await db.execute(
        "SELECT id, name, short_name, toc_url FROM payers WHERE short_name = ?",
        (short_name,),
    )
    row = await cursor.fetchone()
    if not row:
        raise ValueError(f"Unknown payer: {short_name!r}")
    return {"id": row[0], "name": row[1], "short_name": row[2], "toc_url": row[3]}


async def get_target_cpt_codes(db: aiosqlite.Connection) -> set[str]:
    """Load all CPT codes from cpt_lookup."""
    cursor = await db.execute("SELECT code FROM cpt_lookup")
    rows = await cursor.fetchall()
    return {str(row[0]) for row in rows}


async def list_payers(db_path: str | None = None) -> None:
    """Print all payers and their TOC URLs."""
    path = db_path or DATABASE_PATH
    db = await aiosqlite.connect(path)
    db.row_factory = aiosqlite.Row
    try:
        cursor = await db.execute(
            "SELECT short_name, name, toc_url FROM payers ORDER BY short_name"
        )
        rows = await cursor.fetchall()
        print(f"\n{'Short Name':<16} {'Name':<45} {'TOC URL'}")
        print("-" * 100)
        for row in rows:
            url = row["toc_url"] or "(none)"
            print(f"{row['short_name']:<16} {row['name']:<45} {url}")
        print()
    finally:
        await db.close()


async def ingest_payer(
    payer_short_name: str,
    db_path: str | None = None,
    limit: int | None = None,
    url: str | None = None,
    dry_run: bool = False,
) -> dict:
    """Full ingestion pipeline for a single payer.

    Returns a summary dict with counts.
    """
    path = db_path or DATABASE_PATH

    db = await aiosqlite.connect(path)
    db.row_factory = aiosqlite.Row
    await db.execute("PRAGMA journal_mode=WAL")
    await db.execute("PRAGMA foreign_keys=ON")

    try:
        # 1. Look up payer
        payer = await get_payer(db, payer_short_name)
        logger.info("Ingesting payer: %s (%s)", payer["name"], payer["short_name"])

        # 2. Load Iowa NPI cache
        matcher = ProviderMatcher()
        await matcher.load_cache(db)
        logger.info("Loaded %d Iowa NPIs", matcher.npi_count)

        # 3. Load target CPT codes
        target_codes = await get_target_cpt_codes(db)
        logger.info("Loaded %d target CPT codes", len(target_codes))

        # 4. Get MRF file list (via payer-specific adapters)
        if url:
            mrf_files = [MrfFileInfo(
                url=url,
                url_hash=compute_url_hash(url),
                description="Manual URL",
            )]
        else:
            logger.info("Discovering MRF files for %s...", payer["short_name"])
            mrf_files = await get_mrf_file_list(payer)

        if limit:
            mrf_files = mrf_files[:limit]

        logger.info("Processing %d MRF files", len(mrf_files))

        # 5. Process each MRF file
        total_rates = 0
        files_processed = 0
        files_skipped = 0
        files_errored = 0

        for i, mrf_info in enumerate(mrf_files, 1):
            logger.info(
                "[%d/%d] Processing: %s...", i, len(mrf_files), mrf_info.url[:80]
            )

            processor = MrfStreamProcessor(
                iowa_npis=matcher.npi_set,
                target_cpt_codes=target_codes,
            )

            try:
                # For process_mrf_file, we need to handle provider_id mapping
                # Use a modified approach: stream directly and insert with matcher
                inserted = await _ingest_mrf_with_matcher(
                    db=db,
                    payer_id=payer["id"],
                    mrf_info=mrf_info,
                    processor=processor,
                    matcher=matcher,
                    dry_run=dry_run,
                )

                if inserted == -1:
                    files_skipped += 1
                else:
                    total_rates += inserted
                    files_processed += 1

                logger.info(
                    "  Result: %d rates | items=%d, cpt_matches=%d, iowa_rates=%d",
                    inserted if inserted >= 0 else 0,
                    processor.result.total_in_network_items,
                    processor.result.matched_cpt_items,
                    processor.result.iowa_rates_extracted,
                )

            except Exception as e:
                files_errored += 1
                logger.error("  Error: %s", e)

        summary = {
            "payer": payer_short_name,
            "mrf_files_found": len(mrf_files),
            "files_processed": files_processed,
            "files_skipped": files_skipped,
            "files_errored": files_errored,
            "total_rates_inserted": total_rates,
            "dry_run": dry_run,
        }
        logger.info("Ingestion complete: %s", summary)
        return summary

    finally:
        await db.close()


async def _ingest_mrf_with_matcher(
    db: aiosqlite.Connection,
    payer_id: int,
    mrf_info: MrfFileInfo,
    processor: MrfStreamProcessor,
    matcher: ProviderMatcher,
    dry_run: bool = False,
) -> int:
    """Process a single MRF file with provider_id resolution via matcher.

    Returns -1 if skipped (already completed), otherwise the count of inserted rates.
    """
    # Check idempotency
    cursor = await db.execute(
        "SELECT id, status FROM mrf_files WHERE payer_id = ? AND file_hash = ?",
        (payer_id, mrf_info.url_hash),
    )
    existing = await cursor.fetchone()
    if existing and existing[1] == "completed":
        return -1

    # Register file as processing
    mrf_file_id = None
    if not dry_run:
        now = datetime.now(timezone.utc).isoformat()
        if existing:
            mrf_file_id = existing[0]
            await db.execute(
                "UPDATE mrf_files SET status = 'processing', error_message = NULL WHERE id = ?",
                (mrf_file_id,),
            )
        else:
            cursor = await db.execute(
                "INSERT INTO mrf_files (payer_id, url, filename, file_hash, status, downloaded_at) "
                "VALUES (?, ?, ?, ?, 'processing', ?)",
                (payer_id, mrf_info.url, mrf_info.url[:200], mrf_info.url_hash, now),
            )
            mrf_file_id = cursor.lastrowid
        await db.commit()

    total_inserted = 0
    try:
        async for batch in processor.stream_rates_from_url(mrf_info.url):
            if dry_run:
                total_inserted += len(batch)
                continue

            rows = [
                (
                    payer_id,
                    matcher.get_provider_id(r.npi),
                    mrf_file_id,
                    r.billing_code,
                    r.billing_code_type,
                    r.description,
                    r.negotiated_rate,
                    r.negotiated_type,
                    r.billing_class or None,
                )
                for r in batch
            ]
            await db.executemany(
                "INSERT INTO normalized_rates "
                "(payer_id, provider_id, mrf_file_id, billing_code, billing_code_type, "
                "description, negotiated_rate, rate_type, service_setting) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                rows,
            )
            await db.commit()
            total_inserted += len(batch)

        # Check for parse errors recorded by the stream processor
        if processor.result.errors:
            raise RuntimeError(
                f"MRF parse errors: {'; '.join(processor.result.errors)}"
            )

        if not dry_run and mrf_file_id:
            now = datetime.now(timezone.utc).isoformat()
            await db.execute(
                "UPDATE mrf_files SET status = 'completed', records_extracted = ?, processed_at = ? "
                "WHERE id = ?",
                (total_inserted, now, mrf_file_id),
            )
            await db.commit()

    except Exception as e:
        logger.error("Error processing MRF file %s: %s", mrf_info.url_hash, e)
        if not dry_run and mrf_file_id:
            await db.execute(
                "UPDATE mrf_files SET status = 'error', error_message = ? WHERE id = ?",
                (str(e)[:500], mrf_file_id),
            )
            await db.commit()
        raise

    return total_inserted


async def _ingest_mrf_from_bytes(
    db: aiosqlite.Connection,
    payer_id: int,
    mrf_info: MrfFileInfo,
    processor: MrfStreamProcessor,
    matcher: ProviderMatcher,
    byte_source,
    dry_run: bool = False,
) -> int:
    """Process a single MRF from an async byte source (for testing).

    Returns -1 if skipped, otherwise count of inserted rates.
    """
    # Check idempotency
    cursor = await db.execute(
        "SELECT id, status FROM mrf_files WHERE payer_id = ? AND file_hash = ?",
        (payer_id, mrf_info.url_hash),
    )
    existing = await cursor.fetchone()
    if existing and existing[1] == "completed":
        return -1

    # Register file as processing
    mrf_file_id = None
    if not dry_run:
        now = datetime.now(timezone.utc).isoformat()
        if existing:
            mrf_file_id = existing[0]
            await db.execute(
                "UPDATE mrf_files SET status = 'processing', error_message = NULL WHERE id = ?",
                (mrf_file_id,),
            )
        else:
            cursor = await db.execute(
                "INSERT INTO mrf_files (payer_id, url, filename, file_hash, status, downloaded_at) "
                "VALUES (?, ?, ?, ?, 'processing', ?)",
                (payer_id, mrf_info.url, mrf_info.url[:200], mrf_info.url_hash, now),
            )
            mrf_file_id = cursor.lastrowid
        await db.commit()

    total_inserted = 0
    try:
        async for batch in processor.stream_rates_from_bytes(byte_source):
            if dry_run:
                total_inserted += len(batch)
                continue

            rows = [
                (
                    payer_id,
                    matcher.get_provider_id(r.npi),
                    mrf_file_id,
                    r.billing_code,
                    r.billing_code_type,
                    r.description,
                    r.negotiated_rate,
                    r.negotiated_type,
                    r.billing_class or None,
                )
                for r in batch
            ]
            await db.executemany(
                "INSERT INTO normalized_rates "
                "(payer_id, provider_id, mrf_file_id, billing_code, billing_code_type, "
                "description, negotiated_rate, rate_type, service_setting) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                rows,
            )
            await db.commit()
            total_inserted += len(batch)

        # Check for parse errors recorded by the stream processor
        if processor.result.errors:
            raise RuntimeError(
                f"MRF parse errors: {'; '.join(processor.result.errors)}"
            )

        if not dry_run and mrf_file_id:
            now = datetime.now(timezone.utc).isoformat()
            await db.execute(
                "UPDATE mrf_files SET status = 'completed', records_extracted = ?, processed_at = ? "
                "WHERE id = ?",
                (total_inserted, now, mrf_file_id),
            )
            await db.commit()

    except Exception as e:
        logger.error("Error processing MRF bytes %s: %s", mrf_info.url_hash, e)
        if not dry_run and mrf_file_id:
            await db.execute(
                "UPDATE mrf_files SET status = 'error', error_message = ? WHERE id = ?",
                (str(e)[:500], mrf_file_id),
            )
            await db.commit()
        raise

    return total_inserted


def main():
    parser = argparse.ArgumentParser(
        description="Ingest MRF data for an Iowa payer"
    )
    parser.add_argument("--payer", help="Payer short_name (e.g., uhc, wellmark)")
    parser.add_argument("--limit", type=int, help="Max number of MRF files to process")
    parser.add_argument("--url", help="Process a specific MRF file URL")
    parser.add_argument("--dry-run", action="store_true", help="Parse without inserting into DB")
    parser.add_argument("--list-payers", action="store_true", help="List all payers and exit")
    parser.add_argument("-v", "--verbose", action="store_true", help="Verbose logging")

    args = parser.parse_args()

    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    if args.list_payers:
        asyncio.run(list_payers())
        return

    if not args.payer:
        parser.error("--payer is required (or use --list-payers)")

    summary = asyncio.run(
        ingest_payer(
            payer_short_name=args.payer,
            limit=args.limit,
            url=args.url,
            dry_run=args.dry_run,
        )
    )

    print(f"\nIngestion summary:")
    for k, v in summary.items():
        print(f"  {k}: {v}")


if __name__ == "__main__":
    main()
