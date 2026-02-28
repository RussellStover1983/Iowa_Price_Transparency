"""NPI and TIN cache for Iowa providers — O(1) lookup during MRF stream processing.

Usage:
    matcher = ProviderMatcher()
    await matcher.load_cache(db)
    if matcher.is_iowa_npi("1234567890"):
        pid = matcher.get_provider_id("1234567890")
    # TIN-based fallback (for UHC-style Type 1 NPI data):
    tin_ids = matcher.get_provider_ids_by_tin("421234567")
"""

from __future__ import annotations

import aiosqlite


class ProviderMatcher:
    """Preloads Iowa provider NPIs and TINs for fast lookup during MRF streaming."""

    def __init__(self) -> None:
        self._npi_to_id: dict[str, int] = {}
        self._tin_to_ids: dict[str, list[int]] = {}

    async def load_cache(self, db: aiosqlite.Connection) -> None:
        """Load all Iowa NPIs and TINs from the providers table into memory."""
        cursor = await db.execute(
            "SELECT npi, id FROM providers WHERE state = 'IA' AND npi IS NOT NULL"
        )
        rows = await cursor.fetchall()
        self._npi_to_id = {str(row[0]): int(row[1]) for row in rows}

        # Load TINs — one TIN can map to multiple providers
        cursor = await db.execute(
            "SELECT tin, id FROM providers WHERE state = 'IA' AND tin IS NOT NULL AND tin != ''"
        )
        rows = await cursor.fetchall()
        for row in rows:
            tin = str(row[0])
            pid = int(row[1])
            self._tin_to_ids.setdefault(tin, []).append(pid)

    @property
    def npi_count(self) -> int:
        return len(self._npi_to_id)

    @property
    def npi_set(self) -> set[str]:
        return set(self._npi_to_id.keys())

    @property
    def tin_set(self) -> set[str]:
        return set(self._tin_to_ids.keys())

    @property
    def tin_count(self) -> int:
        return len(self._tin_to_ids)

    def is_iowa_npi(self, npi: str) -> bool:
        return npi in self._npi_to_id

    def get_provider_id(self, npi: str) -> int | None:
        return self._npi_to_id.get(npi)

    def get_provider_ids_by_tin(self, tin: str) -> list[int]:
        return self._tin_to_ids.get(tin, [])
