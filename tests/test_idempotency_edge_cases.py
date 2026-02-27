"""Tests for idempotency edge cases — error recovery, partial commits, duplicate handling.

Tests re-processing after error/processing status, duplicate file hashes,
triple-run skipping, and error message content.
"""

import pathlib

import aiosqlite
import pytest
import pytest_asyncio

from etl.ingest_mrf import _ingest_mrf_from_bytes, get_payer, get_target_cpt_codes
from etl.mrf_stream import MrfStreamProcessor
from etl.provider_match import ProviderMatcher
from etl.toc_parser import MrfFileInfo, compute_url_hash

FIXTURE_DIR = pathlib.Path(__file__).parent / "fixtures"


async def _bytes_from_file(path: pathlib.Path):
    data = path.read_bytes()
    for i in range(0, len(data), 4096):
        yield data[i : i + 4096]


@pytest_asyncio.fixture
async def pipeline_db(seeded_db):
    """Open DB connection with full seed data for pipeline tests."""
    db = await aiosqlite.connect(seeded_db)
    db.row_factory = aiosqlite.Row
    await db.execute("PRAGMA journal_mode=WAL")
    await db.execute("PRAGMA foreign_keys=ON")
    try:
        yield db
    finally:
        await db.close()


async def _make_helpers(pipeline_db):
    """Create payer, matcher, and target_codes for test convenience."""
    payer = await get_payer(pipeline_db, "uhc")
    target_codes = await get_target_cpt_codes(pipeline_db)
    matcher = ProviderMatcher()
    await matcher.load_cache(pipeline_db)
    return payer, matcher, target_codes


@pytest.mark.asyncio
async def test_reprocess_after_error_status(pipeline_db, sample_mrf_path):
    """File with status=error is reprocessed (not skipped), becomes completed."""
    payer, matcher, target_codes = await _make_helpers(pipeline_db)

    url = "https://example.com/error-reprocess.json"
    url_hash = compute_url_hash(url)

    # Manually insert an mrf_files row with status=error
    await pipeline_db.execute(
        "INSERT INTO mrf_files (payer_id, url, filename, file_hash, status, error_message) "
        "VALUES (?, ?, ?, ?, 'error', 'Previous error')",
        (payer["id"], url, url[:200], url_hash),
    )
    await pipeline_db.commit()

    mrf_info = MrfFileInfo(url=url, url_hash=url_hash)
    processor = MrfStreamProcessor(
        iowa_npis=matcher.npi_set, target_cpt_codes=target_codes
    )

    inserted = await _ingest_mrf_from_bytes(
        db=pipeline_db,
        payer_id=payer["id"],
        mrf_info=mrf_info,
        processor=processor,
        matcher=matcher,
        byte_source=_bytes_from_file(sample_mrf_path),
    )

    # Should process successfully (not skip)
    assert inserted == 3

    cursor = await pipeline_db.execute(
        "SELECT status, error_message, records_extracted FROM mrf_files WHERE file_hash = ?",
        (url_hash,),
    )
    row = await cursor.fetchone()
    assert row["status"] == "completed"
    assert row["error_message"] is None  # cleared on reprocess
    assert row["records_extracted"] == 3


@pytest.mark.asyncio
async def test_reprocess_after_processing_status(pipeline_db, sample_mrf_path):
    """File with status=processing (crashed run) is reprocessed."""
    payer, matcher, target_codes = await _make_helpers(pipeline_db)

    url = "https://example.com/processing-reprocess.json"
    url_hash = compute_url_hash(url)

    # Manually insert an mrf_files row with status=processing (simulating a crash)
    await pipeline_db.execute(
        "INSERT INTO mrf_files (payer_id, url, filename, file_hash, status) "
        "VALUES (?, ?, ?, ?, 'processing')",
        (payer["id"], url, url[:200], url_hash),
    )
    await pipeline_db.commit()

    mrf_info = MrfFileInfo(url=url, url_hash=url_hash)
    processor = MrfStreamProcessor(
        iowa_npis=matcher.npi_set, target_cpt_codes=target_codes
    )

    inserted = await _ingest_mrf_from_bytes(
        db=pipeline_db,
        payer_id=payer["id"],
        mrf_info=mrf_info,
        processor=processor,
        matcher=matcher,
        byte_source=_bytes_from_file(sample_mrf_path),
    )

    assert inserted == 3

    cursor = await pipeline_db.execute(
        "SELECT status, records_extracted FROM mrf_files WHERE file_hash = ?",
        (url_hash,),
    )
    row = await cursor.fetchone()
    assert row["status"] == "completed"
    assert row["records_extracted"] == 3


@pytest.mark.asyncio
async def test_duplicate_rates_different_files(pipeline_db, sample_mrf_path):
    """Same data, different file_hash → both insert (documents current behavior)."""
    payer, matcher, target_codes = await _make_helpers(pipeline_db)

    # First file
    mrf_info1 = MrfFileInfo(
        url="https://example.com/dup-file-1.json",
        url_hash=compute_url_hash("https://example.com/dup-file-1.json"),
    )
    processor1 = MrfStreamProcessor(
        iowa_npis=matcher.npi_set, target_cpt_codes=target_codes
    )
    result1 = await _ingest_mrf_from_bytes(
        db=pipeline_db,
        payer_id=payer["id"],
        mrf_info=mrf_info1,
        processor=processor1,
        matcher=matcher,
        byte_source=_bytes_from_file(sample_mrf_path),
    )

    # Second file with different hash but same content
    mrf_info2 = MrfFileInfo(
        url="https://example.com/dup-file-2.json",
        url_hash=compute_url_hash("https://example.com/dup-file-2.json"),
    )
    processor2 = MrfStreamProcessor(
        iowa_npis=matcher.npi_set, target_cpt_codes=target_codes
    )
    result2 = await _ingest_mrf_from_bytes(
        db=pipeline_db,
        payer_id=payer["id"],
        mrf_info=mrf_info2,
        processor=processor2,
        matcher=matcher,
        byte_source=_bytes_from_file(sample_mrf_path),
    )

    # Both should succeed (not skip) since they have different file hashes
    assert result1 == 3
    assert result2 == 3


@pytest.mark.asyncio
async def test_triple_run_all_skip(pipeline_db, sample_mrf_path):
    """2nd and 3rd runs with same file_hash both return -1."""
    payer, matcher, target_codes = await _make_helpers(pipeline_db)

    url = "https://example.com/triple-run.json"
    mrf_info = MrfFileInfo(url=url, url_hash=compute_url_hash(url))

    # First run
    processor1 = MrfStreamProcessor(
        iowa_npis=matcher.npi_set, target_cpt_codes=target_codes
    )
    result1 = await _ingest_mrf_from_bytes(
        db=pipeline_db,
        payer_id=payer["id"],
        mrf_info=mrf_info,
        processor=processor1,
        matcher=matcher,
        byte_source=_bytes_from_file(sample_mrf_path),
    )
    assert result1 == 3

    # Second run — should skip
    processor2 = MrfStreamProcessor(
        iowa_npis=matcher.npi_set, target_cpt_codes=target_codes
    )
    result2 = await _ingest_mrf_from_bytes(
        db=pipeline_db,
        payer_id=payer["id"],
        mrf_info=mrf_info,
        processor=processor2,
        matcher=matcher,
        byte_source=_bytes_from_file(sample_mrf_path),
    )
    assert result2 == -1

    # Third run — should also skip
    processor3 = MrfStreamProcessor(
        iowa_npis=matcher.npi_set, target_cpt_codes=target_codes
    )
    result3 = await _ingest_mrf_from_bytes(
        db=pipeline_db,
        payer_id=payer["id"],
        mrf_info=mrf_info,
        processor=processor3,
        matcher=matcher,
        byte_source=_bytes_from_file(sample_mrf_path),
    )
    assert result3 == -1


@pytest.mark.asyncio
async def test_error_message_has_content(pipeline_db):
    """Bad JSON → error_message is non-null and substantive."""
    payer, matcher, target_codes = await _make_helpers(pipeline_db)

    url = "https://example.com/bad-content.json"
    mrf_info = MrfFileInfo(url=url, url_hash=compute_url_hash(url))

    async def _bad_bytes():
        yield b"this is not valid json at all {"

    processor = MrfStreamProcessor(
        iowa_npis=matcher.npi_set, target_cpt_codes=target_codes
    )

    with pytest.raises(Exception):
        await _ingest_mrf_from_bytes(
            db=pipeline_db,
            payer_id=payer["id"],
            mrf_info=mrf_info,
            processor=processor,
            matcher=matcher,
            byte_source=_bad_bytes(),
        )

    cursor = await pipeline_db.execute(
        "SELECT status, error_message FROM mrf_files WHERE file_hash = ?",
        (mrf_info.url_hash,),
    )
    row = await cursor.fetchone()
    assert row["status"] == "error"
    assert row["error_message"] is not None
    assert len(row["error_message"]) > 10  # substantive, not just empty or "None"
