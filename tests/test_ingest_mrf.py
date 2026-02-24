"""Tests for etl/ingest_mrf.py — CLI orchestrator / pipeline integration."""

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
        yield data[i:i + 4096]


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


@pytest.mark.asyncio
async def test_full_pipeline_inserts_rates(pipeline_db, sample_mrf_path):
    """Full pipeline: parse fixture MRF → mrf_files row + normalized_rates rows."""
    payer = await get_payer(pipeline_db, "uhc")
    target_codes = await get_target_cpt_codes(pipeline_db)

    matcher = ProviderMatcher()
    await matcher.load_cache(pipeline_db)

    mrf_info = MrfFileInfo(
        url="https://example.com/test-mrf.json",
        url_hash=compute_url_hash("https://example.com/test-mrf.json"),
    )

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

    assert inserted == 3  # 2 for 27447 + 1 for 99213

    # Verify mrf_files row
    cursor = await pipeline_db.execute(
        "SELECT status, records_extracted FROM mrf_files WHERE file_hash = ?",
        (mrf_info.url_hash,),
    )
    row = await cursor.fetchone()
    assert row["status"] == "completed"
    assert row["records_extracted"] == 3

    # Verify rates were inserted with provider_ids
    cursor = await pipeline_db.execute(
        "SELECT COUNT(*) FROM normalized_rates WHERE mrf_file_id IS NOT NULL"
    )
    count = (await cursor.fetchone())[0]
    assert count >= 3


@pytest.mark.asyncio
async def test_idempotent_skip(pipeline_db, sample_mrf_path):
    """Second run with same file_hash should skip (return -1)."""
    payer = await get_payer(pipeline_db, "uhc")
    target_codes = await get_target_cpt_codes(pipeline_db)

    matcher = ProviderMatcher()
    await matcher.load_cache(pipeline_db)

    mrf_info = MrfFileInfo(
        url="https://example.com/idempotent-test.json",
        url_hash=compute_url_hash("https://example.com/idempotent-test.json"),
    )

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


@pytest.mark.asyncio
async def test_bad_bytes_marks_error(pipeline_db):
    """Bad input data → mrf_files.status=error with error_message."""
    payer = await get_payer(pipeline_db, "uhc")
    target_codes = await get_target_cpt_codes(pipeline_db)

    matcher = ProviderMatcher()
    await matcher.load_cache(pipeline_db)

    mrf_info = MrfFileInfo(
        url="https://example.com/bad-file.json",
        url_hash=compute_url_hash("https://example.com/bad-file.json"),
    )

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

    # Verify error status in mrf_files
    cursor = await pipeline_db.execute(
        "SELECT status, error_message FROM mrf_files WHERE file_hash = ?",
        (mrf_info.url_hash,),
    )
    row = await cursor.fetchone()
    assert row["status"] == "error"
    assert row["error_message"] is not None


@pytest.mark.asyncio
async def test_unknown_payer_raises(pipeline_db):
    """Unknown payer short_name should raise ValueError."""
    with pytest.raises(ValueError, match="Unknown payer"):
        await get_payer(pipeline_db, "nonexistent_payer")
