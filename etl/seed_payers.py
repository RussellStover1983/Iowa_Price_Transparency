"""Seed the payers table with major Iowa insurance payers.

Run directly: python -m etl.seed_payers
"""

import asyncio
import os

import aiosqlite
from dotenv import load_dotenv

load_dotenv()

DATABASE_PATH = os.getenv("DATABASE_PATH", "./data/iowa_transparency.db")

IOWA_PAYERS = [
    {
        "name": "Wellmark Blue Cross Blue Shield",
        "short_name": "wellmark",
        "toc_url": "https://www.wellmark.com/transparency-in-coverage",
        "notes": "Largest Iowa commercial payer; dominant market share",
    },
    {
        "name": "UnitedHealthcare",
        "short_name": "uhc",
        "toc_url": "https://transparency-in-coverage.uhc.com/",
        "notes": "Major national payer with significant Iowa presence",
    },
    {
        "name": "Medica",
        "short_name": "medica",
        "toc_url": "https://www.medica.com/transparency-in-coverage",
        "notes": "Regional payer; strong in Iowa and Minnesota",
    },
    {
        "name": "Aetna",
        "short_name": "aetna",
        "toc_url": "https://health1.aetna.com/app/public/#/one/insurerCode=AETNA_I&brandCode=ALICSI/machine-readable-transparency-in-coverage",
        "notes": "National payer; CVS Health subsidiary",
    },
    {
        "name": "Cigna",
        "short_name": "cigna",
        "toc_url": "https://www.cigna.com/legal/compliance/machine-readable-files",
        "notes": "National payer; The Cigna Group",
    },
    {
        "name": "Delta Dental of Iowa",
        "short_name": "delta_dental",
        "toc_url": "https://www.deltadentalia.com/transparency-in-coverage",
        "notes": "Dental-only payer; included for dental procedure pricing",
    },
    {
        "name": "Iowa Medicaid (Managed Care)",
        "short_name": "ia_medicaid",
        "toc_url": None,
        "notes": "Iowa Medicaid managed care organizations (MCOs); rates via state data",
    },
    {
        "name": "CMS Medicare",
        "short_name": "cms_medicare",
        "toc_url": None,
        "notes": "Medicare fee schedules; publicly available baseline rates",
    },
]


async def seed_payers(db_path: str | None = None):
    """Insert Iowa payer records (idempotent via INSERT OR IGNORE)."""
    path = db_path or DATABASE_PATH

    db = await aiosqlite.connect(path)
    try:
        await db.execute("PRAGMA foreign_keys=ON")

        for payer in IOWA_PAYERS:
            await db.execute(
                "INSERT OR IGNORE INTO payers (name, short_name, toc_url, state_filter, notes) "
                "VALUES (?, ?, ?, 'IA', ?)",
                (payer["name"], payer["short_name"], payer["toc_url"], payer["notes"]),
            )

        await db.commit()

        cursor = await db.execute("SELECT COUNT(*) FROM payers")
        count = (await cursor.fetchone())[0]
        print(f"Payers table now has {count} records")
    finally:
        await db.close()


if __name__ == "__main__":
    asyncio.run(seed_payers())
