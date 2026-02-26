# CLAUDE.md

## Autonomy Rules

You have FULL autonomy to complete this project end-to-end without stopping to ask for permission. Specifically:

- **DO NOT ask for permission to search the web.** If you need a library, documentation, or any reference material — just search for it and download it. Do not stop and ask "should I search for X?" Just do it.
- **DO NOT ask for permission to create, modify, or delete files.** You are working in your own project directory. Create whatever files you need, overwrite them, restructure them, delete temp files — whatever it takes. This is your workspace.
- **DO NOT ask for permission to install packages.** If you need opencv-python, pytesseract, numpy, Pillow, or anything else — install it immediately and keep moving.
- **DO NOT stop to present partial work and ask "should I continue?"** Yes, you should continue. Always. Build the entire thing from start to finish in one session.
- **DO NOT ask clarifying questions unless the project is truly blocked.** If something is ambiguous, make a reasonable decision, document it in a comment, and keep building.
- **DO ask me to test the program against a real screenshot once it's fully built and working.** That's the one time you should stop and check in.

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Summary

REST API that compares medical procedure prices across Iowa hospitals. Data sourced from CMS Transparency in Coverage MRF (Machine-Readable Files). Built with FastAPI + SQLite (async via aiosqlite) + ijson for streaming multi-GB JSON parsing.

## Commands

```bash
# Activate virtualenv (required)
source .venv/Scripts/activate

# Initialize database and seed data (run in order)
python -m db.init_db
python -m etl.load_cpt
python -m etl.seed_payers
python -m etl.seed_sample_data

# Run API server
uvicorn api.main:app --reload

# Run all tests
pytest tests/ -v

# Run a single test file
pytest tests/test_compare.py -v

# Run a specific test
pytest tests/test_compare.py::test_compare_returns_rates -v

# MRF ingestion pipeline
python -m etl.ingest_mrf --list-payers
python -m etl.ingest_mrf --payer uhc --limit 1 --dry-run -v
```

## Architecture

### Async-first, streaming-first

Everything is async — aiosqlite for DB, httpx for HTTP, pytest-asyncio for tests. MRF files can be multiple GB; the pipeline never loads a full file into memory, using ijson's SAX-style streaming parser instead.

### Database layer (`db/`)

SQLite with WAL mode and foreign keys enabled on every connection (`db/session.py`). `get_connection()` is an async context manager — always use `async with get_connection() as db:`. Schema creation in `db/init_db.py` is idempotent (CREATE TABLE IF NOT EXISTS). FTS5 virtual table (`cpt_fts`) is kept in sync with `cpt_lookup` via INSERT/DELETE triggers.

### API layer (`api/`)

FastAPI app created in `api/main.py` with lifespan-based DB init. Routes live under `api/routes/` with `/v1` prefix. `api/dependencies.py` provides `get_db()` as a FastAPI dependency. All response shapes are Pydantic v2 models in `db/models.py`.

### ETL pipeline (`etl/`)

The MRF ingestion chain: `ingest_mrf.py` (CLI orchestrator) → `toc_parser.py` (finds in-network MRF URLs from a payer's table-of-contents JSON) → `mrf_stream.py` (two-phase streaming parse). Phase 1 scans `provider_references` to build an Iowa NPI→group map, then phase 2 filters `in_network` items by target CPT codes and cross-joins with Iowa providers. `provider_match.py` preloads all Iowa NPIs from the DB into a dict for O(1) lookup. Idempotency is via `mrf_files(payer_id, file_hash)` — already-processed files are skipped.

### Test conventions (`tests/`)

Tests use a temporary SQLite DB (auto-cleaned before/after each test via `conftest.py`). Fixture hierarchy: `initialized_db` → `cpt_db` (adds CPT codes) → `seeded_db` (adds payers + providers + rates). HTTP tests use `httpx.AsyncClient` with `ASGITransport` — no running server needed. Two client fixtures: `cpt_client` (CPT data only) and `client` (fully seeded). Test fixture JSON files live in `tests/fixtures/`.

### CPT disambiguation (`services/cpt_disambiguation.py`)

Optional Claude Haiku reranking of CPT search results. Gracefully degrades if `ANTHROPIC_API_KEY` is not set — the search still works, just without AI reranking.

## Environment

Copy `.env.example` to `.env`. Key variables: `DATABASE_PATH` (defaults to `./data/iowa_transparency.db`), `ANTHROPIC_API_KEY` (optional, for disambiguation), `LOG_LEVEL`, `ENVIRONMENT`.

## Key Schema Tables

- **normalized_rates** — core data: payer_id + provider_id + billing_code + negotiated_rate
- **cpt_lookup** / **cpt_fts** — 88 CPT codes with FTS5 full-text search
- **mrf_files** — tracks processed MRF files per payer for idempotent ingestion
