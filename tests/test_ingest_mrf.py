"""Tests for etl/ingest_mrf.py — CLI orchestrator / pipeline integration."""

import pathlib

import aiosqlite
import pytest
import pytest_asyncio

from etl.ingest_mrf import _build_deduped_rows, _ingest_mrf_from_bytes, get_payer, get_target_cpt_codes
from etl.mrf_stream import MrfStreamProcessor, RateRecord
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


# --- _build_deduped_rows unit tests ---


def _make_matcher(npi_map: dict[str, int], tin_map: dict[str, list[int]]) -> ProviderMatcher:
    """Create a ProviderMatcher with pre-set lookup dicts (no DB needed)."""
    m = ProviderMatcher()
    m._npi_to_id = npi_map
    m._tin_to_ids = tin_map
    return m


def _make_record(npi="555", tin="421234567", code="27447", rate=42000.0,
                 neg_type="negotiated", billing_class="institutional") -> RateRecord:
    return RateRecord(
        npi=npi, tin=tin, billing_code=code, billing_code_type="CPT",
        negotiated_rate=rate, negotiated_type=neg_type,
        billing_class=billing_class, description="test",
    )


def test_dedup_multiple_physician_npis_same_hospital():
    """Multiple physician NPIs with the same TIN collapse to one row per provider."""
    matcher = _make_matcher(
        npi_map={},  # no NPI matches — force TIN fallback
        tin_map={"421234567": [10]},  # one hospital
    )
    batch = [
        _make_record(npi="5551111111", tin="421234567", code="27447", rate=42000.0),
        _make_record(npi="5552222222", tin="421234567", code="27447", rate=42000.0),
        _make_record(npi="5553333333", tin="421234567", code="27447", rate=42000.0),
    ]
    rows, _ = _build_deduped_rows(batch, payer_id=1, mrf_file_id=100, matcher=matcher)
    # All 3 records resolve to provider_id=10 with same (code, rate, type) → 1 row
    assert len(rows) == 1
    assert rows[0][1] == 10  # provider_id


def test_dedup_cross_batch_with_shared_seen():
    """Shared seen set deduplicates across batches."""
    matcher = _make_matcher(
        npi_map={},
        tin_map={"421234567": [10]},
    )
    record = _make_record(npi="5551111111", tin="421234567")
    seen: set[tuple] = set()

    rows1, _ = _build_deduped_rows([record], payer_id=1, mrf_file_id=100, matcher=matcher, seen=seen)
    rows2, _ = _build_deduped_rows([record], payer_id=1, mrf_file_id=100, matcher=matcher, seen=seen)

    assert len(rows1) == 1
    assert len(rows2) == 0  # duplicate caught across batches


def test_dedup_without_shared_seen_duplicates():
    """Without a shared seen set, each batch deduplicates independently."""
    matcher = _make_matcher(
        npi_map={},
        tin_map={"421234567": [10]},
    )
    record = _make_record(npi="5551111111", tin="421234567")

    rows1, _ = _build_deduped_rows([record], payer_id=1, mrf_file_id=100, matcher=matcher)
    rows2, _ = _build_deduped_rows([record], payer_id=1, mrf_file_id=100, matcher=matcher)

    assert len(rows1) == 1
    assert len(rows2) == 1  # no shared seen → duplicate not caught


def test_dedup_npi_priority_over_tin():
    """NPI-resolved provider_id takes priority over TIN-resolved."""
    matcher = _make_matcher(
        npi_map={"1234567890": 5},  # NPI match → provider 5
        tin_map={"421234567": [10, 11]},  # TIN match → providers 10, 11
    )
    batch = [_make_record(npi="1234567890", tin="421234567")]
    rows, _ = _build_deduped_rows(batch, payer_id=1, mrf_file_id=100, matcher=matcher)

    assert len(rows) == 1
    assert rows[0][1] == 5  # NPI match wins, not TIN


def test_dedup_tin_expands_to_all_providers():
    """TIN matching expands to all providers sharing that TIN."""
    matcher = _make_matcher(
        npi_map={},
        tin_map={"421234567": [10, 11]},  # hospital + outpatient clinic
    )
    batch = [_make_record(npi="5551111111", tin="421234567")]
    rows, _ = _build_deduped_rows(batch, payer_id=1, mrf_file_id=100, matcher=matcher)

    assert len(rows) == 2
    provider_ids = {r[1] for r in rows}
    assert provider_ids == {10, 11}


def test_dedup_no_match_dropped():
    """Records with no NPI or TIN match are silently dropped."""
    matcher = _make_matcher(
        npi_map={},
        tin_map={},
    )
    batch = [_make_record(npi="9999999999", tin="000000000")]
    rows, _ = _build_deduped_rows(batch, payer_id=1, mrf_file_id=100, matcher=matcher)

    assert len(rows) == 0


def test_dedup_different_rates_not_collapsed():
    """Same provider+code but different rates should NOT be collapsed."""
    matcher = _make_matcher(
        npi_map={},
        tin_map={"421234567": [10]},
    )
    batch = [
        _make_record(npi="555", tin="421234567", code="27447", rate=42000.0),
        _make_record(npi="555", tin="421234567", code="27447", rate=45000.0),
    ]
    rows, _ = _build_deduped_rows(batch, payer_id=1, mrf_file_id=100, matcher=matcher)

    assert len(rows) == 2  # different rates → not duplicates


def test_dedup_discovers_tins_from_npi_matched_records():
    """NPI-matched records with TINs are captured for backfill."""
    matcher = _make_matcher(
        npi_map={"1234567890": 5, "2345678901": 6},
        tin_map={},
    )
    batch = [
        _make_record(npi="1234567890", tin="421234567", code="27447", rate=42000.0),
        _make_record(npi="2345678901", tin="422345678", code="27447", rate=42000.0),
    ]
    rows, discovered_tins = _build_deduped_rows(batch, payer_id=1, mrf_file_id=100, matcher=matcher)

    assert len(rows) == 2
    assert discovered_tins == {5: "421234567", 6: "422345678"}


def test_dedup_no_tins_from_tin_matched_records():
    """TIN-matched records (no NPI match) don't produce spurious TIN discoveries."""
    matcher = _make_matcher(
        npi_map={},
        tin_map={"421234567": [10]},
    )
    batch = [_make_record(npi="5551111111", tin="421234567")]
    _, discovered_tins = _build_deduped_rows(batch, payer_id=1, mrf_file_id=100, matcher=matcher)

    assert discovered_tins == {}  # no NPI match → no discovery


def test_dedup_empty_tin_not_discovered():
    """Records with empty/zero TINs are not captured."""
    matcher = _make_matcher(
        npi_map={"1234567890": 5},
        tin_map={},
    )
    batch = [
        _make_record(npi="1234567890", tin="", code="27447", rate=42000.0),
        _make_record(npi="1234567890", tin="0", code="99213", rate=175.0),
    ]
    _, discovered_tins = _build_deduped_rows(batch, payer_id=1, mrf_file_id=100, matcher=matcher)

    assert discovered_tins == {}
