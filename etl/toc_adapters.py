"""Payer-specific TOC adapters for discovering MRF file URLs.

Each major payer uses a different access pattern for their Table of Contents:
- UHC: Azure blob API with two-step fetch (list → download URL)
- Aetna: Date-templated HealthSparq URL
- Cigna: Signed CloudFront URLs scraped from compliance page

Dispatch via get_mrf_file_list(payer) which routes by short_name.
"""

from __future__ import annotations

import hashlib
import logging
import re
from datetime import datetime, timedelta

import httpx

from etl.toc_parser import MrfFileInfo, parse_toc_from_url

logger = logging.getLogger(__name__)


def _stable_hash(value: str) -> str:
    """SHA-256 truncated to 16 hex chars — same format as compute_url_hash."""
    return hashlib.sha256(value.encode()).hexdigest()[:16]


# ---------------------------------------------------------------------------
# UHC adapter — Azure blob API
# ---------------------------------------------------------------------------

async def _uhc_get_mrf_files(payer: dict) -> list[MrfFileInfo]:
    """List UHC in-network MRF files via their blob API.

    UHC exposes a blob listing at their API endpoint. We filter for
    in-network files and construct download URLs.
    """
    api_url = payer.get("toc_url", "").rstrip("/")
    if not api_url:
        return []

    results: list[tuple[int, MrfFileInfo]] = []  # (size, info) for sorting

    async with httpx.AsyncClient(timeout=60, follow_redirects=True) as client:
        # Step 1: List available blobs
        try:
            resp = await client.get(api_url)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            logger.error("UHC blob listing failed: %s", e)
            return []

        # The API returns {"blobs": [...]} with name, downloadUrl, size
        blobs = data if isinstance(data, list) else data.get("blobs", [])

        for blob in blobs:
            if not isinstance(blob, dict):
                continue

            filename = blob.get("name", "")
            download_url = blob.get("downloadUrl", "")
            if not filename or not download_url:
                continue

            # Filter: only in-network rate files (not index, not allowed-amount)
            lower = filename.lower()
            if lower.endswith("_index.json"):
                continue
            if "allowed-amount" in lower or "allowed_amount" in lower:
                continue
            if "in-network" not in lower and "in_network" not in lower:
                continue

            # Use stable filename for hash (not the expiring SAS download URL)
            url_hash = _stable_hash(filename)

            file_size = blob.get("size", 0)
            size_mb = file_size / (1024 * 1024)
            results.append((file_size, MrfFileInfo(
                url=download_url,
                url_hash=url_hash,
                description=f"UHC: {filename} ({size_mb:.0f} MB)",
            )))

    # Sort smallest first — more practical for testing with --limit
    results.sort(key=lambda x: x[0])
    sorted_files = [info for _, info in results]

    logger.info("UHC adapter: found %d in-network MRF files", len(sorted_files))
    return sorted_files


# ---------------------------------------------------------------------------
# Aetna adapter — date-templated HealthSparq URL
# ---------------------------------------------------------------------------

def _aetna_resolve_url(template: str, target_date: datetime | None = None) -> str:
    """Substitute {YYYY-MM-DD} in Aetna's URL template with a date string.

    Uses the first of the given month (or current month if not specified).
    """
    if target_date is None:
        target_date = datetime.now()
    # Aetna uses the 1st of the month
    date_str = target_date.replace(day=1).strftime("%Y-%m-%d")
    return template.replace("{YYYY-MM-DD}", date_str)


async def _aetna_get_mrf_files(payer: dict) -> list[MrfFileInfo]:
    """Resolve Aetna's date-templated TOC URL, trying current and past months."""
    template = payer.get("toc_url", "")
    if not template or "{YYYY-MM-DD}" not in template:
        return []

    now = datetime.now()
    # Try current month, then up to 3 months back
    for months_back in range(4):
        target = now - timedelta(days=months_back * 30)
        resolved_url = _aetna_resolve_url(template, target)
        logger.info("Aetna: trying TOC at %s", resolved_url)

        try:
            files = await parse_toc_from_url(resolved_url)
            if files:
                logger.info(
                    "Aetna: found %d MRF files at %s",
                    len(files), target.strftime("%Y-%m"),
                )
                return files
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                logger.info("Aetna: 404 for %s, trying older month", resolved_url)
                continue
            logger.error("Aetna TOC fetch error: %s", e)
            return []
        except Exception as e:
            logger.error("Aetna TOC fetch error: %s", e)
            return []

    logger.warning("Aetna: no valid TOC found in last 4 months")
    return []


# ---------------------------------------------------------------------------
# Cigna adapter — scrape CloudFront signed URL from compliance page
# ---------------------------------------------------------------------------

# Pattern to match CloudFront TOC URLs on Cigna's compliance page
_CIGNA_TOC_PATTERN = re.compile(
    r'https://[a-z0-9]+\.cloudfront\.net/[^"\'<>\s]+index\.json[^"\'<>\s]*',
    re.IGNORECASE,
)


async def _cigna_get_mrf_files(payer: dict) -> list[MrfFileInfo]:
    """Scrape Cigna's compliance page for the signed CloudFront TOC URL."""
    page_url = payer.get("toc_url", "")
    if not page_url:
        return []

    async with httpx.AsyncClient(
        timeout=30,
        follow_redirects=True,
        headers={"User-Agent": "Mozilla/5.0 (compatible; IowaTransparencyBot/1.0)"},
    ) as client:
        try:
            resp = await client.get(page_url)
            resp.raise_for_status()
            html = resp.text
        except Exception as e:
            logger.error("Cigna page fetch error: %s", e)
            return []

    # Extract CloudFront TOC URL(s) from HTML
    matches = _CIGNA_TOC_PATTERN.findall(html)
    if not matches:
        logger.warning("Cigna: no CloudFront TOC URL found on compliance page")
        return []

    # Use the first match as the TOC URL
    toc_url = matches[0]
    logger.info("Cigna: extracted TOC URL: %s", toc_url[:100])

    try:
        return await parse_toc_from_url(toc_url)
    except Exception as e:
        logger.error("Cigna TOC parse error: %s", e)
        return []


# ---------------------------------------------------------------------------
# Dispatch
# ---------------------------------------------------------------------------

_ADAPTERS: dict[str, callable] = {
    "uhc": _uhc_get_mrf_files,
    "aetna": _aetna_get_mrf_files,
    "cigna": _cigna_get_mrf_files,
}


async def get_mrf_file_list(payer: dict) -> list[MrfFileInfo]:
    """Get MRF file URLs for a payer, using the appropriate adapter.

    Args:
        payer: dict with at least "short_name" and "toc_url" keys.

    Returns:
        List of MrfFileInfo objects, or empty list if no files found.
    """
    short_name = payer.get("short_name", "")
    toc_url = payer.get("toc_url")

    if not toc_url:
        logger.info("No TOC URL for payer %s — skipping", short_name)
        return []

    # Check for payer-specific adapter
    adapter = _ADAPTERS.get(short_name)
    if adapter:
        logger.info("Using %s adapter for payer %s", short_name, payer.get("name", ""))
        return await adapter(payer)

    # Default: pass TOC URL directly to generic parser
    logger.info("Using default TOC parser for payer %s", short_name)
    try:
        return await parse_toc_from_url(toc_url)
    except Exception as e:
        logger.error("Default TOC parse error for %s: %s", short_name, e)
        return []
