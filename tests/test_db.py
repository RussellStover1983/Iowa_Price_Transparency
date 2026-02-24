"""Tests for database schema, FTS, and seed data."""

import pytest
import aiosqlite

from db.init_db import init_database
from etl.load_cpt import load_cpt_codes
from etl.seed_payers import seed_payers


@pytest.mark.asyncio
async def test_all_tables_exist(initialized_db):
    """Verify all expected tables are created."""
    async with aiosqlite.connect(initialized_db) as db:
        cursor = await db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        )
        tables = {row[0] for row in await cursor.fetchall()}

    expected = {"payers", "providers", "mrf_files", "normalized_rates", "cpt_lookup"}
    assert expected.issubset(tables), f"Missing tables: {expected - tables}"


@pytest.mark.asyncio
async def test_providers_table_schema(initialized_db):
    """Verify providers table has all expected columns."""
    async with aiosqlite.connect(initialized_db) as db:
        cursor = await db.execute("PRAGMA table_info(providers)")
        columns = {row[1] for row in await cursor.fetchall()}

    expected_columns = {
        "id", "npi", "tin", "name", "facility_type",
        "address", "city", "state", "zip_code", "county",
        "active", "created_at",
    }
    assert expected_columns == columns


@pytest.mark.asyncio
async def test_normalized_rates_has_provider_id(initialized_db):
    """Verify normalized_rates includes provider_id column."""
    async with aiosqlite.connect(initialized_db) as db:
        cursor = await db.execute("PRAGMA table_info(normalized_rates)")
        columns = {row[1] for row in await cursor.fetchall()}

    assert "provider_id" in columns
    assert "payer_id" in columns


@pytest.mark.asyncio
async def test_cpt_seed_data(initialized_db):
    """Verify CPT codes can be loaded."""
    await load_cpt_codes(initialized_db)

    async with aiosqlite.connect(initialized_db) as db:
        cursor = await db.execute("SELECT COUNT(*) FROM cpt_lookup")
        count = (await cursor.fetchone())[0]

    assert count >= 80, f"Expected >=80 CPT codes, got {count}"


@pytest.mark.asyncio
async def test_fts_search(initialized_db):
    """Verify FTS5 search works on CPT codes."""
    await load_cpt_codes(initialized_db)

    async with aiosqlite.connect(initialized_db) as db:
        cursor = await db.execute(
            "SELECT code, description FROM cpt_fts WHERE cpt_fts MATCH 'knee replacement'"
        )
        results = await cursor.fetchall()

    assert len(results) > 0
    codes = [r[0] for r in results]
    assert "27447" in codes, "Expected total knee replacement code 27447"


@pytest.mark.asyncio
async def test_payer_seed_data(initialized_db):
    """Verify payers can be seeded."""
    await seed_payers(initialized_db)

    async with aiosqlite.connect(initialized_db) as db:
        cursor = await db.execute("SELECT COUNT(*) FROM payers")
        count = (await cursor.fetchone())[0]

    assert count == 8


@pytest.mark.asyncio
async def test_payer_has_wellmark(initialized_db):
    """Verify Wellmark is in the seed data."""
    await seed_payers(initialized_db)

    async with aiosqlite.connect(initialized_db) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT * FROM payers WHERE short_name = 'wellmark'"
        )
        row = await cursor.fetchone()

    assert row is not None
    assert "Wellmark" in row["name"]
    assert row["state_filter"] == "IA"
