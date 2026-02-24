"""Database connection manager using aiosqlite with WAL mode and foreign keys."""

import os
from contextlib import asynccontextmanager

import aiosqlite
from dotenv import load_dotenv

load_dotenv()

DATABASE_PATH = os.getenv("DATABASE_PATH", "./data/iowa_transparency.db")


@asynccontextmanager
async def get_connection():
    """Yield an aiosqlite connection with WAL mode and foreign keys enabled."""
    db = await aiosqlite.connect(DATABASE_PATH)
    db.row_factory = aiosqlite.Row
    await db.execute("PRAGMA journal_mode=WAL")
    await db.execute("PRAGMA foreign_keys=ON")
    try:
        yield db
    finally:
        await db.close()
