"""Idempotent database schema creation.

Run directly: python -m db.init_db
"""

import asyncio
import os
import sys

import aiosqlite
from dotenv import load_dotenv

load_dotenv()

DATABASE_PATH = os.getenv("DATABASE_PATH", "./data/iowa_transparency.db")

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS payers (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    short_name TEXT NOT NULL UNIQUE,
    toc_url TEXT,
    state_filter TEXT DEFAULT 'IA',
    active INTEGER DEFAULT 1,
    last_crawled TEXT,
    notes TEXT
);

CREATE TABLE IF NOT EXISTS providers (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    npi TEXT UNIQUE,
    tin TEXT,
    name TEXT NOT NULL,
    facility_type TEXT,
    address TEXT,
    city TEXT,
    state TEXT DEFAULT 'IA',
    zip_code TEXT,
    county TEXT,
    active INTEGER DEFAULT 1,
    created_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS mrf_files (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    payer_id INTEGER NOT NULL REFERENCES payers(id),
    url TEXT NOT NULL,
    filename TEXT,
    file_hash TEXT,
    status TEXT DEFAULT 'pending',
    records_extracted INTEGER DEFAULT 0,
    downloaded_at TEXT,
    processed_at TEXT,
    error_message TEXT,
    UNIQUE(payer_id, file_hash)
);

CREATE TABLE IF NOT EXISTS normalized_rates (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    payer_id INTEGER NOT NULL REFERENCES payers(id),
    provider_id INTEGER REFERENCES providers(id),
    mrf_file_id INTEGER REFERENCES mrf_files(id),
    billing_code TEXT NOT NULL,
    billing_code_type TEXT NOT NULL DEFAULT 'CPT',
    description TEXT,
    negotiated_rate REAL NOT NULL,
    rate_type TEXT,
    service_setting TEXT,
    created_at TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_rates_billing_provider
    ON normalized_rates(billing_code, provider_id);
CREATE INDEX IF NOT EXISTS idx_rates_billing_payer
    ON normalized_rates(billing_code, payer_id);
CREATE INDEX IF NOT EXISTS idx_rates_billing_provider_payer
    ON normalized_rates(billing_code, provider_id, payer_id);
CREATE INDEX IF NOT EXISTS idx_rates_provider_billing
    ON normalized_rates(provider_id, billing_code);
CREATE UNIQUE INDEX IF NOT EXISTS idx_rates_dedup
    ON normalized_rates(payer_id, provider_id, billing_code, negotiated_rate, rate_type);

CREATE TABLE IF NOT EXISTS cpt_lookup (
    code TEXT PRIMARY KEY,
    description TEXT NOT NULL,
    category TEXT,
    common_names TEXT
);

-- FTS5 virtual table for CPT search
CREATE VIRTUAL TABLE IF NOT EXISTS cpt_fts USING fts5(
    code,
    description,
    common_names,
    content='cpt_lookup',
    content_rowid='rowid'
);

-- Triggers to keep FTS in sync with cpt_lookup
CREATE TRIGGER IF NOT EXISTS cpt_lookup_ai AFTER INSERT ON cpt_lookup BEGIN
    INSERT INTO cpt_fts(rowid, code, description, common_names)
    VALUES (new.rowid, new.code, new.description, new.common_names);
END;

CREATE TRIGGER IF NOT EXISTS cpt_lookup_ad AFTER DELETE ON cpt_lookup BEGIN
    INSERT INTO cpt_fts(cpt_fts, rowid, code, description, common_names)
    VALUES ('delete', old.rowid, old.code, old.description, old.common_names);
END;

CREATE TRIGGER IF NOT EXISTS cpt_lookup_au AFTER UPDATE ON cpt_lookup BEGIN
    INSERT INTO cpt_fts(cpt_fts, rowid, code, description, common_names)
    VALUES ('delete', old.rowid, old.code, old.description, old.common_names);
    INSERT INTO cpt_fts(rowid, code, description, common_names)
    VALUES (new.rowid, new.code, new.description, new.common_names);
END;
"""


async def _migrate_dedup_index(db: aiosqlite.Connection) -> None:
    """Remove duplicate rates before creating the unique index.

    Only runs if the index doesn't exist yet AND there are duplicates.
    """
    # Check if the unique index already exists
    cursor = await db.execute(
        "SELECT name FROM sqlite_master WHERE type='index' AND name='idx_rates_dedup'"
    )
    if await cursor.fetchone():
        return  # index already exists

    # Check if the table exists and has data
    cursor = await db.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='normalized_rates'"
    )
    if not await cursor.fetchone():
        return  # table doesn't exist yet — schema creation will handle it

    cursor = await db.execute("SELECT COUNT(*) FROM normalized_rates")
    count = (await cursor.fetchone())[0]
    if count == 0:
        return  # empty table — no migration needed

    # Delete duplicates, keeping the row with the lowest id
    result = await db.execute(
        "DELETE FROM normalized_rates WHERE id NOT IN ("
        "  SELECT MIN(id) FROM normalized_rates "
        "  GROUP BY payer_id, provider_id, billing_code, negotiated_rate, rate_type"
        ")"
    )
    deleted = result.rowcount
    if deleted > 0:
        await db.commit()
        print(f"  Deduplicated normalized_rates: removed {deleted} duplicate rows")


async def init_database(db_path: str | None = None):
    """Create all tables and indexes idempotently."""
    path = db_path or DATABASE_PATH
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)

    db = await aiosqlite.connect(path)
    try:
        await db.execute("PRAGMA journal_mode=WAL")
        await db.execute("PRAGMA foreign_keys=ON")

        # Migrate: deduplicate before creating unique index on existing DBs
        await _migrate_dedup_index(db)

        await db.executescript(SCHEMA_SQL)
        await db.commit()
        print(f"Database initialized at {path}")
    finally:
        await db.close()


if __name__ == "__main__":
    asyncio.run(init_database())
