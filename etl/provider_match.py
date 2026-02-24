"""NPI cache for Iowa providers — O(1) lookup during MRF stream processing.

Usage:
    matcher = ProviderMatcher()
    await matcher.load_cache(db)
    if matcher.is_iowa_npi("1234567890"):
        pid = matcher.get_provider_id("1234567890")
"""

from __future__ import annotations

import aiosqlite


class ProviderMatcher:
    """Preloads Iowa provider NPIs for fast lookup during MRF streaming."""

    def __init__(self) -> None:
        self._npi_to_id: dict[str, int] = {}

    async def load_cache(self, db: aiosqlite.Connection) -> None:
        """Load all Iowa NPIs from the providers table into memory."""
        cursor = await db.execute(
            "SELECT npi, id FROM providers WHERE state = 'IA' AND npi IS NOT NULL"
        )
        rows = await cursor.fetchall()
        self._npi_to_id = {str(row[0]): int(row[1]) for row in rows}

    @property
    def npi_count(self) -> int:
        return len(self._npi_to_id)

    @property
    def npi_set(self) -> set[str]:
        return set(self._npi_to_id.keys())

    def is_iowa_npi(self, npi: str) -> bool:
        return npi in self._npi_to_id

    def get_provider_id(self, npi: str) -> int | None:
        return self._npi_to_id.get(npi)
