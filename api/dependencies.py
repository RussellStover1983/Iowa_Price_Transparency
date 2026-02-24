"""FastAPI dependencies for database access."""

from db.session import get_connection


async def get_db():
    """Dependency that yields an aiosqlite connection."""
    async with get_connection() as db:
        yield db
