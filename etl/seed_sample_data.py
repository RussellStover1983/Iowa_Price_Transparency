"""Seed sample providers and negotiated rates for development/testing.

Run directly: python -m etl.seed_sample_data

Generates deterministic synthetic data (random.seed(42)) for 10 Iowa providers
with ~500-800 rate rows across 3-5 payers per provider.
"""

import asyncio
import os
import random

import aiosqlite
from dotenv import load_dotenv

load_dotenv()

DATABASE_PATH = os.getenv("DATABASE_PATH", "./data/iowa_transparency.db")

IOWA_PROVIDERS = [
    {
        "npi": "1234567890",
        "tin": "421234567",
        "name": "University of Iowa Hospitals and Clinics",
        "facility_type": "hospital",
        "city": "Iowa City",
        "county": "Johnson",
        "zip_code": "52242",
    },
    {
        "npi": "2345678901",
        "tin": "422345678",
        "name": "MercyOne Des Moines Medical Center",
        "facility_type": "hospital",
        "city": "Des Moines",
        "county": "Polk",
        "zip_code": "50314",
    },
    {
        "npi": "3456789012",
        "tin": "423456789",
        "name": "UnityPoint Health Iowa Methodist Medical Center",
        "facility_type": "hospital",
        "city": "Des Moines",
        "county": "Polk",
        "zip_code": "50309",
    },
    {
        "npi": "4567890123",
        "tin": "424567890",
        "name": "UnityPoint Health St. Luke's Hospital",
        "facility_type": "hospital",
        "city": "Cedar Rapids",
        "county": "Linn",
        "zip_code": "52402",
    },
    {
        "npi": "5678901234",
        "tin": "425678901",
        "name": "MercyOne Cedar Rapids Medical Center",
        "facility_type": "hospital",
        "city": "Cedar Rapids",
        "county": "Linn",
        "zip_code": "52403",
    },
    {
        "npi": "6789012345",
        "tin": "426789012",
        "name": "Broadlawns Medical Center",
        "facility_type": "hospital",
        "city": "Des Moines",
        "county": "Polk",
        "zip_code": "50314",
    },
    {
        "npi": "7890123456",
        "tin": "427890123",
        "name": "Genesis Medical Center",
        "facility_type": "hospital",
        "city": "Davenport",
        "county": "Scott",
        "zip_code": "52803",
    },
    {
        "npi": "8901234567",
        "tin": "428901234",
        "name": "UnityPoint Health Allen Hospital",
        "facility_type": "hospital",
        "city": "Waterloo",
        "county": "Black Hawk",
        "zip_code": "50702",
    },
    {
        "npi": "9012345678",
        "tin": "429012345",
        "name": "Mary Greeley Medical Center",
        "facility_type": "hospital",
        "city": "Ames",
        "county": "Story",
        "zip_code": "50010",
    },
    {
        "npi": "0123456789",
        "tin": "420123456",
        "name": "Iowa Specialty Hospital",
        "facility_type": "hospital",
        "city": "Clarion",
        "county": "Wright",
        "zip_code": "50525",
    },
]

# Price ranges (min, max) by CPT category
CATEGORY_PRICE_RANGES = {
    "orthopedic": (8000, 65000),
    "imaging": (200, 4000),
    "cardiology": (500, 80000),
    "gastroenterology": (1500, 25000),
    "primary_care": (75, 500),
    "emergency": (300, 3000),
    "ob_gyn": (3000, 30000),
    "urology": (800, 15000),
    "ent": (3000, 18000),
    "general_surgery": (200, 20000),
    "ophthalmology": (1500, 8000),
    "pain_management": (300, 3000),
    "neurology": (200, 2000),
    "pulmonology": (150, 3000),
    "dermatology": (100, 1000),
    "lab": (10, 300),
}

# Payer short_names to pick from (excluding delta_dental)
PAYER_SHORT_NAMES = [
    "wellmark", "uhc", "medica", "aetna", "cigna", "ia_medicaid", "cms_medicare"
]

RATE_TYPES = ["negotiated", "negotiated", "negotiated", "fee schedule", "derived"]
SERVICE_SETTINGS = ["inpatient", "outpatient", "outpatient", "outpatient", "ambulatory"]


async def seed_sample_data(db_path: str | None = None):
    """Insert sample providers and synthetic rates."""
    path = db_path or DATABASE_PATH
    rng = random.Random(42)

    db = await aiosqlite.connect(path)
    try:
        await db.execute("PRAGMA foreign_keys=ON")
        db.row_factory = aiosqlite.Row

        # Insert providers
        for provider in IOWA_PROVIDERS:
            await db.execute(
                "INSERT OR IGNORE INTO providers (npi, tin, name, facility_type, city, state, zip_code, county) "
                "VALUES (?, ?, ?, ?, ?, 'IA', ?, ?)",
                (
                    provider["npi"],
                    provider["tin"],
                    provider["name"],
                    provider["facility_type"],
                    provider["city"],
                    provider["zip_code"],
                    provider["county"],
                ),
            )
        await db.commit()

        # Get provider IDs
        cursor = await db.execute("SELECT id, name FROM providers")
        providers = await cursor.fetchall()

        # Get payer IDs
        cursor = await db.execute(
            "SELECT id, short_name FROM payers WHERE short_name IN ({})".format(
                ",".join("?" for _ in PAYER_SHORT_NAMES)
            ),
            PAYER_SHORT_NAMES,
        )
        payers = await cursor.fetchall()
        if not payers:
            print("No payers found — run etl.seed_payers first")
            return

        payer_map = {row["short_name"]: row["id"] for row in payers}

        # Get CPT codes with categories
        cursor = await db.execute("SELECT code, category FROM cpt_lookup")
        cpt_codes = await cursor.fetchall()
        if not cpt_codes:
            print("No CPT codes found — run etl.load_cpt first")
            return

        rate_count = 0
        for provider in providers:
            # Each provider gets rates for 20-30 random CPT codes
            num_codes = rng.randint(20, 30)
            selected_codes = rng.sample(list(cpt_codes), min(num_codes, len(cpt_codes)))

            # Each provider contracts with 3-5 payers
            num_payers = rng.randint(3, 5)
            available_payers = list(payer_map.keys())
            selected_payers = rng.sample(available_payers, min(num_payers, len(available_payers)))

            for code_row in selected_codes:
                code = code_row["code"]
                category = code_row["category"] or "primary_care"
                price_min, price_max = CATEGORY_PRICE_RANGES.get(
                    category, (100, 5000)
                )

                # Base price for this provider/procedure
                base_price = rng.uniform(price_min, price_max)

                for payer_short in selected_payers:
                    payer_id = payer_map[payer_short]
                    # Each payer negotiates slightly different rate (±30%)
                    rate = round(base_price * rng.uniform(0.7, 1.3), 2)
                    rate_type = rng.choice(RATE_TYPES)
                    service_setting = rng.choice(SERVICE_SETTINGS)

                    await db.execute(
                        "INSERT INTO normalized_rates "
                        "(payer_id, provider_id, billing_code, billing_code_type, "
                        "negotiated_rate, rate_type, service_setting) "
                        "VALUES (?, ?, ?, 'CPT', ?, ?, ?)",
                        (
                            payer_id,
                            provider["id"],
                            code,
                            rate,
                            rate_type,
                            service_setting,
                        ),
                    )
                    rate_count += 1

        await db.commit()

        cursor = await db.execute("SELECT COUNT(*) FROM providers")
        prov_count = (await cursor.fetchone())[0]
        cursor = await db.execute("SELECT COUNT(*) FROM normalized_rates")
        actual_rate_count = (await cursor.fetchone())[0]
        print(
            f"Seeded {prov_count} providers and {actual_rate_count} rate rows"
        )
    finally:
        await db.close()


if __name__ == "__main__":
    asyncio.run(seed_sample_data())
