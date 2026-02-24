"""Shared test fixtures."""

import os
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
