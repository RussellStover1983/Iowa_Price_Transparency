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
