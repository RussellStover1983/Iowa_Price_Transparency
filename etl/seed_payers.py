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
        "toc_url": None,
        "notes": "Largest Iowa commercial payer; no confirmed direct JSON TOC URL (HealthSparq portal only)",
    },
    {
        "name": "UnitedHealthcare",
        "short_name": "uhc",
        "toc_url": "https://transparency-in-coverage.uhc.com/api/v1/uhc/blobs/",
        "notes": "Azure blob API; adapter handles two-step fetch (list blobs then download URL)",
    },
    {
        "name": "Medica",
        "short_name": "medica",
        "toc_url": "https://mrf.healthsparq.com/medica-egress.nophi.kyruushsq.com/prd/mrf/MEDICA_I/MEDICA",
        "notes": "HealthSparq GCS bucket; adapter probes known Iowa plan files; ZIP format",
    },
    {
        "name": "Aetna",
        "short_name": "aetna",
        "toc_url": "https://mrf.healthsparq.com/aetnacvs-egress.nophi.kyruushsq.com/prd/mrf/AETNACVS_I/ALICFI/{YYYY-MM-DD}/tableOfContents/{YYYY-MM-DD}_Aetna-Life-Insurance-Company_index.json.gz",
        "notes": "Adapter uses latest_metadata.json; falls back to date-templated URL",
    },
    {
        "name": "Cigna",
        "short_name": "cigna",
        "toc_url": "https://www.cigna.com/legal/compliance/machine-readable-files",
        "notes": "Signed CloudFront URLs; adapter scrapes fresh TOC URL from compliance page",
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
            # Update existing rows with latest TOC URL and notes
            await db.execute(
                "UPDATE payers SET toc_url = ?, notes = ? WHERE short_name = ?",
                (payer["toc_url"], payer["notes"], payer["short_name"]),
            )

        await db.commit()

        cursor = await db.execute("SELECT COUNT(*) FROM payers")
        count = (await cursor.fetchone())[0]
        print(f"Payers table now has {count} records")
    finally:
        await db.close()


if __name__ == "__main__":
    asyncio.run(seed_payers())
