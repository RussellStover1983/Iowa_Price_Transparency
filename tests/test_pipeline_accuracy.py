"""Tests for pipeline data accuracy — DB insertion, provider_id mapping,
mrf_file_id traceability, and aggregation math.

Uses the pipeline_db pattern from test_ingest_mrf.py with complex_mrf.json.
"""

import pathlib
import statistics

import aiosqlite
import pytest
import pytest_asyncio

from etl.ingest_mrf import _ingest_mrf_from_bytes, get_payer, get_target_cpt_codes
from etl.mrf_stream import MrfStreamProcessor
from etl.provider_match import ProviderMatcher
from etl.toc_parser import MrfFileInfo, compute_url_hash

FIXTURE_DIR = pathlib.Path(__file__).parent / "fixtures"
COMPLEX_MRF_URL = "https://example.com/complex-mrf-test.json"


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


@pytest_asyncio.fixture
async def ingested_complex(pipeline_db, complex_mrf_path):
    """Ingest complex_mrf.json and return (db, mrf_info, inserted_count)."""
    payer = await get_payer(pipeline_db, "uhc")
    target_codes = await get_target_cpt_codes(pipeline_db)

    matcher = ProviderMatcher()
    await matcher.load_cache(pipeline_db)

    mrf_info = MrfFileInfo(
        url=COMPLEX_MRF_URL,
        url_hash=compute_url_hash(COMPLEX_MRF_URL),
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
        byte_source=_bytes_from_file(complex_mrf_path),
    )

    return pipeline_db, mrf_info, inserted


# --- Provider ID mapping tests ---


@pytest.mark.asyncio
async def test_provider_id_correct_per_npi(ingested_complex):
    """Each row's provider_id maps to the correct NPI in the providers table."""
    db, mrf_info, _ = ingested_complex

    cursor = await db.execute(
        "SELECT nr.provider_id, p.npi "
        "FROM normalized_rates nr "
        "JOIN providers p ON nr.provider_id = p.id "
        "WHERE nr.mrf_file_id IS NOT NULL "
        "AND nr.mrf_file_id = ("
        "  SELECT id FROM mrf_files WHERE file_hash = ?"
        ")",
        (mrf_info.url_hash,),
    )
    rows = await cursor.fetchall()
    assert len(rows) == 9

    # Verify known NPI→provider_id consistency
    npi_to_provider_ids = {}
    for row in rows:
        npi = row["npi"]
        pid = row["provider_id"]
        if npi in npi_to_provider_ids:
            assert npi_to_provider_ids[npi] == pid, (
                f"NPI {npi} mapped to multiple provider_ids"
            )
        else:
            npi_to_provider_ids[npi] = pid


@pytest.mark.asyncio
async def test_no_null_provider_ids(ingested_complex):
    """No rows with NULL provider_id when all NPIs are known Iowa providers."""
    db, mrf_info, _ = ingested_complex

    cursor = await db.execute(
        "SELECT COUNT(*) FROM normalized_rates "
        "WHERE mrf_file_id = (SELECT id FROM mrf_files WHERE file_hash = ?) "
        "AND provider_id IS NULL",
        (mrf_info.url_hash,),
    )
    count = (await cursor.fetchone())[0]
    assert count == 0


# --- mrf_file_id traceability tests ---


@pytest.mark.asyncio
async def test_mrf_file_id_links_all_rates(ingested_complex):
    """All 9 rates reference the same mrf_file_id; records_extracted = 9."""
    db, mrf_info, inserted = ingested_complex
    assert inserted == 9

    # Check mrf_files row
    cursor = await db.execute(
        "SELECT id, status, records_extracted FROM mrf_files WHERE file_hash = ?",
        (mrf_info.url_hash,),
    )
    mrf_row = await cursor.fetchone()
    assert mrf_row["status"] == "completed"
    assert mrf_row["records_extracted"] == 9

    # Count rates linked to this mrf_file_id
    cursor = await db.execute(
        "SELECT COUNT(*) FROM normalized_rates WHERE mrf_file_id = ?",
        (mrf_row["id"],),
    )
    count = (await cursor.fetchone())[0]
    assert count == 9


@pytest.mark.asyncio
async def test_mrf_file_completed_status(ingested_complex):
    """After success: status=completed, processed_at not null."""
    db, mrf_info, _ = ingested_complex

    cursor = await db.execute(
        "SELECT status, processed_at FROM mrf_files WHERE file_hash = ?",
        (mrf_info.url_hash,),
    )
    row = await cursor.fetchone()
    assert row["status"] == "completed"
    assert row["processed_at"] is not None


# --- Field mapping in DB tests ---


@pytest.mark.asyncio
async def test_rate_type_and_service_setting(ingested_complex):
    """negotiated_type → rate_type, billing_class → service_setting correctly mapped."""
    db, mrf_info, _ = ingested_complex

    cursor = await db.execute(
        "SELECT rate_type, service_setting FROM normalized_rates "
        "WHERE mrf_file_id = (SELECT id FROM mrf_files WHERE file_hash = ?)",
        (mrf_info.url_hash,),
    )
    rows = await cursor.fetchall()

    rate_types = [r["rate_type"] for r in rows]
    service_settings = [r["service_setting"] for r in rows]

    # 2 records are "fee schedule", 7 are "negotiated"
    assert rate_types.count("fee schedule") == 2
    assert rate_types.count("negotiated") == 7

    # billing_class maps to service_setting: "institutional" and "professional"
    # Empty billing_class maps to None
    institutional_count = service_settings.count("institutional")
    professional_count = service_settings.count("professional")
    assert institutional_count == 7
    assert professional_count == 2


@pytest.mark.asyncio
async def test_description_preserved_in_db(ingested_complex):
    """Description from MRF item stored verbatim in normalized_rates."""
    db, mrf_info, _ = ingested_complex

    cursor = await db.execute(
        "SELECT DISTINCT description FROM normalized_rates "
        "WHERE mrf_file_id = (SELECT id FROM mrf_files WHERE file_hash = ?)",
        (mrf_info.url_hash,),
    )
    rows = await cursor.fetchall()
    descriptions = {r["description"] for r in rows}

    expected = {
        "Total knee replacement (arthroplasty)",
        "Total knee replacement alternative rate",
        "Office visit established patient level 3",
        "Colonoscopy diagnostic Iowa provider",
    }
    assert descriptions == expected


# --- Aggregation math tests ---


@pytest.mark.asyncio
async def test_aggregation_even_count(ingested_complex):
    """4 known 27447 rates from item 1: verify exact min/max/avg/median."""
    db, mrf_info, _ = ingested_complex

    # Get all 27447 rates from item 1 (description: "Total knee replacement (arthroplasty)")
    cursor = await db.execute(
        "SELECT negotiated_rate FROM normalized_rates "
        "WHERE mrf_file_id = (SELECT id FROM mrf_files WHERE file_hash = ?) "
        "AND billing_code = '27447' AND description = 'Total knee replacement (arthroplasty)'",
        (mrf_info.url_hash,),
    )
    rows = await cursor.fetchall()
    rates = sorted([r["negotiated_rate"] for r in rows])
    # 2 NPIs x 2 prices: [38000, 38000, 45000, 45000]
    assert len(rates) == 4
    assert rates == [38000.0, 38000.0, 45000.0, 45000.0]

    assert min(rates) == 38000.0
    assert max(rates) == 45000.0
    assert round(statistics.mean(rates), 2) == 41500.0
    assert round(statistics.median(rates), 2) == 41500.0
    assert round(max(rates) - min(rates), 2) == 7000.0


@pytest.mark.asyncio
async def test_aggregation_odd_count(ingested_complex):
    """3 known rates (all 27447 from both items for NPI 1234567890): median = middle."""
    db, mrf_info, _ = ingested_complex

    # NPI 1234567890 appears in items 1 and 2 for code 27447
    # Item 1: rates 45000, 38000; Item 2: rate 42000 → [38000, 42000, 45000]
    cursor = await db.execute(
        "SELECT nr.negotiated_rate FROM normalized_rates nr "
        "JOIN providers p ON nr.provider_id = p.id "
        "WHERE nr.mrf_file_id = (SELECT id FROM mrf_files WHERE file_hash = ?) "
        "AND nr.billing_code = '27447' AND p.npi = '1234567890'",
        (mrf_info.url_hash,),
    )
    rows = await cursor.fetchall()
    rates = sorted([r["negotiated_rate"] for r in rows])
    assert len(rates) == 3
    assert rates == [38000.0, 42000.0, 45000.0]
    assert statistics.median(rates) == 42000.0


@pytest.mark.asyncio
async def test_aggregation_single_rate(ingested_complex):
    """1 rate for colonoscopy → min == max == avg == median, savings = 0."""
    db, mrf_info, _ = ingested_complex

    cursor = await db.execute(
        "SELECT negotiated_rate FROM normalized_rates "
        "WHERE mrf_file_id = (SELECT id FROM mrf_files WHERE file_hash = ?) "
        "AND billing_code = '45378'",
        (mrf_info.url_hash,),
    )
    rows = await cursor.fetchall()
    rates = [r["negotiated_rate"] for r in rows]
    assert len(rates) == 1
    assert rates[0] == 1800.0
    assert min(rates) == max(rates) == statistics.mean(rates) == statistics.median(rates)
    assert max(rates) - min(rates) == 0


# --- End-to-end: MRF → DB → API ---


@pytest.mark.asyncio
async def test_end_to_end_mrf_to_api(seeded_db, complex_mrf_path):
    """Ingest complex_mrf → hit /v1/compare → verify providers and rates match fixture."""
    db = await aiosqlite.connect(seeded_db)
    db.row_factory = aiosqlite.Row
    await db.execute("PRAGMA journal_mode=WAL")
    await db.execute("PRAGMA foreign_keys=ON")

    try:
        payer = await get_payer(db, "uhc")
        target_codes = await get_target_cpt_codes(db)
        matcher = ProviderMatcher()
        await matcher.load_cache(db)

        mrf_info = MrfFileInfo(
            url="https://example.com/e2e-test.json",
            url_hash=compute_url_hash("https://example.com/e2e-test.json"),
        )
        processor = MrfStreamProcessor(
            iowa_npis=matcher.npi_set, target_cpt_codes=target_codes
        )
        inserted = await _ingest_mrf_from_bytes(
            db=db,
            payer_id=payer["id"],
            mrf_info=mrf_info,
            processor=processor,
            matcher=matcher,
            byte_source=_bytes_from_file(complex_mrf_path),
        )
        assert inserted == 9
    finally:
        await db.close()

    # Now query the API
    from httpx import ASGITransport, AsyncClient
    from api.main import app

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get(
            "/v1/compare", params={"codes": "27447,99213,45378", "payer": "uhc"}
        )

    assert response.status_code == 200
    data = response.json()
    assert data["codes_requested"] == ["27447", "99213", "45378"]

    # All 3 procedures should have data from our MRF ingestion
    codes_with_providers = [
        p["billing_code"]
        for p in data["procedures"]
        if p["provider_count"] > 0
    ]
    assert "27447" in codes_with_providers
    assert "99213" in codes_with_providers
    assert "45378" in codes_with_providers

    # Stats should exist for all 3 codes
    stat_codes = {s["billing_code"] for s in data["stats"]}
    assert stat_codes == {"27447", "99213", "45378"}

    # Verify 27447 stats include our known rates
    knee_stats = next(s for s in data["stats"] if s["billing_code"] == "27447")
    assert knee_stats["rate_count"] >= 5  # negotiated rates from complex MRF (fee schedule filtered out)
    assert knee_stats["potential_savings"] >= 0
