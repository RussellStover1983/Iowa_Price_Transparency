"""Tests for etl/provider_match.py — Iowa NPI cache."""

import pytest
import pytest_asyncio
import aiosqlite


@pytest_asyncio.fixture
async def matcher_db(seeded_db):
    """Return an open DB connection with seeded data for matcher tests."""
    db = await aiosqlite.connect(seeded_db)
    db.row_factory = aiosqlite.Row
    await db.execute("PRAGMA foreign_keys=ON")
    try:
        yield db
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_cache_loads_all_iowa_npis(matcher_db):
    from etl.provider_match import ProviderMatcher
    matcher = ProviderMatcher()
    await matcher.load_cache(matcher_db)
    assert matcher.npi_count == 12


@pytest.mark.asyncio
async def test_known_npi_returns_correct_provider_id(matcher_db):
    from etl.provider_match import ProviderMatcher
    matcher = ProviderMatcher()
    await matcher.load_cache(matcher_db)
    # NPI 1234567890 = University of Iowa Hospitals and Clinics (first provider)
    pid = matcher.get_provider_id("1234567890")
    assert pid is not None
    # Verify it's the right provider
    cursor = await matcher_db.execute(
        "SELECT name FROM providers WHERE id = ?", (pid,)
    )
    row = await cursor.fetchone()
    assert "Iowa" in row[0]


@pytest.mark.asyncio
async def test_unknown_npi_returns_none(matcher_db):
    from etl.provider_match import ProviderMatcher
    matcher = ProviderMatcher()
    await matcher.load_cache(matcher_db)
    assert matcher.get_provider_id("0000000000") is None


@pytest.mark.asyncio
async def test_is_iowa_npi(matcher_db):
    from etl.provider_match import ProviderMatcher
    matcher = ProviderMatcher()
    await matcher.load_cache(matcher_db)
    assert matcher.is_iowa_npi("1234567890") is True
    assert matcher.is_iowa_npi("9999999999") is False


@pytest_asyncio.fixture
async def tin_db(initialized_db):
    """DB with providers that have TINs set."""
    db = await aiosqlite.connect(initialized_db)
    await db.execute("PRAGMA foreign_keys=ON")
    # Insert providers with TINs
    await db.execute(
        "INSERT INTO providers (npi, tin, name, state) VALUES (?, ?, ?, ?)",
        ("1111111111", "421234567", "Hospital A", "IA"),
    )
    await db.execute(
        "INSERT INTO providers (npi, tin, name, state) VALUES (?, ?, ?, ?)",
        ("2222222222", "422345678", "Hospital B", "IA"),
    )
    # Two providers sharing the same TIN (e.g. hospital + outpatient clinic)
    await db.execute(
        "INSERT INTO providers (npi, tin, name, state) VALUES (?, ?, ?, ?)",
        ("3333333333", "421234567", "Hospital A Outpatient", "IA"),
    )
    # Provider with no TIN
    await db.execute(
        "INSERT INTO providers (npi, tin, name, state) VALUES (?, ?, ?, ?)",
        ("4444444444", None, "Hospital C", "IA"),
    )
    await db.commit()
    try:
        yield db
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_tin_lookup_loads_tins(tin_db):
    """ProviderMatcher loads TINs and resolves correctly."""
    from etl.provider_match import ProviderMatcher
    matcher = ProviderMatcher()
    await matcher.load_cache(tin_db)
    assert matcher.tin_count == 2  # 2 unique TINs
    assert "421234567" in matcher.tin_set
    assert "422345678" in matcher.tin_set


@pytest.mark.asyncio
async def test_tin_to_multiple_providers(tin_db):
    """One TIN mapping to multiple provider IDs."""
    from etl.provider_match import ProviderMatcher
    matcher = ProviderMatcher()
    await matcher.load_cache(tin_db)
    ids = matcher.get_provider_ids_by_tin("421234567")
    assert len(ids) == 2  # Hospital A + Hospital A Outpatient


@pytest.mark.asyncio
async def test_tin_single_provider(tin_db):
    """TIN mapping to a single provider."""
    from etl.provider_match import ProviderMatcher
    matcher = ProviderMatcher()
    await matcher.load_cache(tin_db)
    ids = matcher.get_provider_ids_by_tin("422345678")
    assert len(ids) == 1


@pytest.mark.asyncio
async def test_tin_unknown_returns_empty(tin_db):
    """Unknown TIN returns empty list."""
    from etl.provider_match import ProviderMatcher
    matcher = ProviderMatcher()
    await matcher.load_cache(tin_db)
    assert matcher.get_provider_ids_by_tin("999999999") == []


@pytest.mark.asyncio
async def test_tin_null_not_loaded(tin_db):
    """Providers with NULL TIN are not included in TIN lookup."""
    from etl.provider_match import ProviderMatcher
    matcher = ProviderMatcher()
    await matcher.load_cache(tin_db)
    # Hospital C has no TIN — should not appear
    assert matcher.npi_count == 4  # all 4 providers loaded by NPI
    assert matcher.tin_count == 2  # only 2 unique TINs
