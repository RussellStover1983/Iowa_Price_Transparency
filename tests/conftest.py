"""Shared test fixtures."""

import os
import pathlib
import tempfile

import pytest
import pytest_asyncio

# Use a temp database for tests
TEST_DB_PATH = os.path.join(tempfile.gettempdir(), "test_iowa_transparency.db")
os.environ["DATABASE_PATH"] = TEST_DB_PATH


@pytest.fixture(autouse=True)
def _clean_db():
    """Remove test database before each test for isolation."""
    if os.path.exists(TEST_DB_PATH):
        os.remove(TEST_DB_PATH)
    yield
    if os.path.exists(TEST_DB_PATH):
        os.remove(TEST_DB_PATH)


@pytest.fixture
def test_db_path():
    return TEST_DB_PATH


@pytest_asyncio.fixture
async def initialized_db(test_db_path):
    """Return path to a freshly initialized database."""
    from db.init_db import init_database
    await init_database(test_db_path)
    return test_db_path


@pytest_asyncio.fixture
async def cpt_db(initialized_db):
    """Initialized DB with CPT codes loaded (no payers/providers)."""
    from etl.load_cpt import load_cpt_codes
    await load_cpt_codes(initialized_db)
    return initialized_db


@pytest_asyncio.fixture
async def seeded_db(initialized_db):
    """Initialized DB with CPT codes, payers, and sample data."""
    from etl.load_cpt import load_cpt_codes
    from etl.seed_payers import seed_payers
    from etl.seed_sample_data import seed_sample_data
    await load_cpt_codes(initialized_db)
    await seed_payers(initialized_db)
    await seed_sample_data(initialized_db)
    return initialized_db


@pytest.fixture
def fixture_dir():
    """Path to tests/fixtures/ directory."""
    return pathlib.Path(__file__).parent / "fixtures"


@pytest.fixture
def sample_mrf_path(fixture_dir):
    return fixture_dir / "sample_mrf.json"


@pytest.fixture
def sample_toc_path(fixture_dir):
    return fixture_dir / "sample_toc.json"


@pytest.fixture
def complex_mrf_path(fixture_dir):
    return fixture_dir / "complex_mrf.json"


@pytest.fixture
def edge_case_mrf_path(fixture_dir):
    return fixture_dir / "edge_case_mrf.json"


@pytest.fixture
def inline_providers_mrf_path(fixture_dir):
    return fixture_dir / "inline_providers_mrf.json"


@pytest.fixture
def async_file_bytes(fixture_dir):
    """Factory: returns an async byte-chunk iterator from a fixture file."""
    async def _reader(path: pathlib.Path, chunk_size: int = 8192):
        data = path.read_bytes()
        for i in range(0, len(data), chunk_size):
            yield data[i:i + chunk_size]
    return _reader


@pytest_asyncio.fixture
async def cpt_client(cpt_db):
    """AsyncClient against a DB with CPT codes only."""
    from httpx import ASGITransport, AsyncClient
    from api.main import app
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


@pytest_asyncio.fixture
async def client(seeded_db):
    """AsyncClient against a fully seeded DB."""
    from httpx import ASGITransport, AsyncClient
    from api.main import app
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client
