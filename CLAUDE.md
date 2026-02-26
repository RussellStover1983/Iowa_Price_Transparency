# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Autonomy Rules

You have FULL autonomy to complete this project end-to-end without stopping to ask for permission. Specifically:

- **DO NOT ask for permission to search the web.** If you need a library, documentation, or any reference material — just search for it and download it. Do not stop and ask "should I search for X?" Just do it.
- **DO NOT ask for permission to create, modify, or delete files.** You are working in your own project directory. Create whatever files you need, overwrite them, restructure them, delete temp files — whatever it takes. This is your workspace.
- **DO NOT ask for permission to install packages.** If you need opencv-python, pytesseract, numpy, Pillow, or anything else — install it immediately and keep moving.
- **DO NOT stop to present partial work and ask "should I continue?"** Yes, you should continue. Always. Build the entire thing from start to finish in one session.
- **DO NOT ask clarifying questions unless the project is truly blocked.** If something is ambiguous, make a reasonable decision, document it in a comment, and keep building.
- **DO ask me to test the program against a real screenshot once it's fully built and working.** That's the one time you should stop and check in.

## Project Summary

REST API that compares medical procedure prices across Iowa hospitals. Data sourced from CMS Transparency in Coverage MRF (Machine-Readable Files). Built with Python 3.12 + FastAPI + SQLite (async via aiosqlite) + ijson for streaming multi-GB JSON parsing. Vanilla JS frontend (no build step) served by FastAPI's `StaticFiles` at `/app`.

## Commands

```bash
# Activate virtualenv (required)
source .venv/Scripts/activate

# Initialize database and seed data (run in order)
python -m db.init_db
python -m etl.load_cpt
python -m etl.seed_payers
python -m etl.seed_sample_data

# Run API server (localhost:8000, root redirects to /app)
uvicorn api.main:app --reload

# Run all tests (45 tests)
pytest tests/ -v

# Run a single test file
pytest tests/test_compare.py -v

# Run a specific test
pytest tests/test_compare.py::test_compare_returns_rates -v

# MRF ingestion pipeline
python -m etl.ingest_mrf --list-payers
python -m etl.ingest_mrf --payer uhc --limit 1 --dry-run -v

# Docker
docker build -t iowa-price . && docker run -p 8000:8000 iowa-price
```

## Architecture

### Async-first, streaming-first

Everything is async — aiosqlite for DB, httpx for HTTP, pytest-asyncio for tests. MRF files can be multiple GB; the HTTP download is streamed and decompressed in chunks, then buffered into `io.BytesIO` for ijson's SAX-style streaming parser.

### Database layer (`db/`)

SQLite with WAL mode and foreign keys enabled on every connection (`db/session.py`). `get_connection()` is an async context manager — always use `async with get_connection() as db:`. Schema creation in `db/init_db.py` is idempotent (CREATE TABLE IF NOT EXISTS). FTS5 virtual table (`cpt_fts`) is a content table synced with `cpt_lookup` via INSERT/DELETE/UPDATE triggers. All rows use `row_factory = aiosqlite.Row` (supports both `row["col"]` and `row[0]`).

### API layer (`api/`)

FastAPI app created in `api/main.py` with lifespan-based DB init. Routes live under `api/routes/` with `/v1` prefix. `api/dependencies.py` provides `get_db()` as a FastAPI dependency (wraps `get_connection()`). All response shapes are Pydantic v2 models in `db/models.py`. Routes use raw SQL with `await db.execute()` — no ORM. Input validation is manual regex, not Pydantic input models.

### ETL pipeline (`etl/`)

The MRF ingestion chain: `ingest_mrf.py` (CLI orchestrator) → `toc_parser.py` (finds in-network MRF URLs from a payer's table-of-contents JSON) → `mrf_stream.py` (two-phase streaming parse). Phase 1 scans `provider_references` to build an Iowa NPI→group map, then phase 2 filters `in_network` items by target CPT codes and cross-joins with Iowa providers. `provider_match.py` preloads all Iowa NPIs from the DB into a dict for O(1) lookup. Idempotency is via `mrf_files(payer_id, file_hash)` — already-processed files are skipped. Every ETL script is dual-mode: importable as a function (takes optional `db_path`) and runnable via `python -m etl.<module>`.

### Test conventions (`tests/`)

Tests use a temporary SQLite DB (auto-cleaned before/after each test via `conftest.py`). Fixture hierarchy: `initialized_db` → `cpt_db` (adds CPT codes) → `seeded_db` (adds payers + providers + rates). HTTP tests use `httpx.AsyncClient` with `ASGITransport` — no running server needed. Two client fixtures: `cpt_client` (CPT data only) and `client` (fully seeded). Test fixture JSON files live in `tests/fixtures/`.

**pytest-asyncio is in strict mode** (no `asyncio_mode = auto` configured): every async test needs `@pytest.mark.asyncio` and every async fixture needs `@pytest_asyncio.fixture` (not `@pytest.fixture`). Mixing these up causes tests to silently not run as async.

### CPT disambiguation (`services/cpt_disambiguation.py`)

Optional Claude Haiku reranking of CPT search results. Gracefully degrades (broad `except Exception`) if `ANTHROPIC_API_KEY` is not set — the search still works, just without AI reranking.

## Environment

Copy `.env.example` to `.env`. Key variables: `DATABASE_PATH` (defaults to `./data/iowa_transparency.db`), `ANTHROPIC_API_KEY` (optional, for disambiguation), `LOG_LEVEL`, `ENVIRONMENT`.

## Key Schema Tables

- **normalized_rates** — core data: payer_id + provider_id + billing_code + negotiated_rate
- **cpt_lookup** / **cpt_fts** — 88 CPT codes with FTS5 full-text search; `common_names` stored as JSON string (must `json.loads()` on read)
- **payers** — 8 Iowa insurers; `short_name` is UNIQUE
- **providers** — 12 Iowa hospitals; NPI `"1234567890"` is always UIHC (used in test fixtures)
- **mrf_files** — tracks processed MRF files per payer for idempotent ingestion

## Gotchas

- **DB env var timing in tests**: `conftest.py` sets `os.environ["DATABASE_PATH"]` at module top level *before* any app imports. New test files must not import app code at the top level or they'll get the production DB path.
- **`seed_sample_data` is NOT idempotent for rates**: providers use `INSERT OR IGNORE`, but rates use plain `INSERT`. Running it twice on the same DB doubles rate rows.
- **`common_names` is a JSON string**: Every read from `cpt_lookup` must `json.loads(row["common_names"])` — forgetting this returns a raw string to the API client.
- **Seed data is deterministic**: `seed_sample_data.py` uses `random.Random(42)` (instance-level, not global seed), so test assertions can rely on reproducible data.
