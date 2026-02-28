"""Payer-specific TOC adapters for discovering MRF file URLs.

Each major payer uses a different access pattern for their Table of Contents:
- UHC: Azure blob API with two-step fetch (list → download URL)
- Aetna: latest_metadata.json → find Iowa TOC → parse MRF URLs
- Wellmark: latest_metadata.json → find Iowa TOC → parse MRF URLs (same HealthSparq platform as Aetna)
- Cigna: Signed CloudFront URLs scraped from compliance page
- Medica: Direct GCS bucket probing for known Iowa plan files

Dispatch via get_mrf_file_list(payer) which routes by short_name.
"""

from __future__ import annotations

import hashlib
import html
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

    # Filter out trivially small files (< 1MB compressed = likely empty plans)
    MIN_SIZE = 1 * 1024 * 1024  # 1 MB
    filtered = [(size, info) for size, info in results if size >= MIN_SIZE]
    skipped = len(results) - len(filtered)

    # Sort by size: medium files first (10-500MB range), then larger, then smaller
    # This prioritizes files likely to have Iowa data without being multi-GB behemoths
    def _sort_key(item):
        size = item[0]
        mb = size / (1024 * 1024)
        if 10 <= mb <= 500:
            return (0, size)  # preferred range, sorted ascending within
        elif mb > 500:
            return (1, size)  # large files second
        else:
            return (2, -size)  # small files last
    filtered.sort(key=_sort_key)
    sorted_files = [info for _, info in filtered]

    logger.info(
        "UHC adapter: found %d in-network MRF files (%d skipped < 1MB)",
        len(sorted_files), skipped,
    )
    return sorted_files


# ---------------------------------------------------------------------------
# Aetna adapter — latest_metadata.json → Iowa TOC → MRF URLs
# ---------------------------------------------------------------------------

# Aetna brand codes on HealthSparq; ALICFI (fully insured) is primary for Iowa
_AETNA_BASE = "https://mrf.healthsparq.com/aetnacvs-egress.nophi.kyruushsq.com/prd/mrf/AETNACVS_I"
_AETNA_BRAND = "ALICFI"


def _aetna_resolve_url(template: str, target_date: datetime | None = None) -> str:
    """Substitute {YYYY-MM-DD} in Aetna's URL template with a date string.

    Uses the first of the given month (or current month if not specified).
    """
    if target_date is None:
        target_date = datetime.now()
    date_str = target_date.replace(day=1).strftime("%Y-%m-%d")
    return template.replace("{YYYY-MM-DD}", date_str)


async def _aetna_get_mrf_files(payer: dict) -> list[MrfFileInfo]:
    """Fetch Aetna's latest_metadata.json, find Iowa TOC, parse MRF URLs.

    The metadata endpoint always returns current data — no date guessing needed.
    Falls back to date-templated URL if metadata fetch fails.
    """
    metadata_url = f"{_AETNA_BASE}/{_AETNA_BRAND}/latest_metadata.json"
    logger.info("Aetna: fetching metadata from %s", metadata_url)

    async with httpx.AsyncClient(timeout=120, follow_redirects=True) as client:
        try:
            resp = await client.get(metadata_url)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            logger.error("Aetna metadata fetch failed: %s", e)
            # Fall back to legacy date-templated approach
            return await _aetna_fallback(payer)

    files_list = data.get("files", data) if isinstance(data, dict) else data
    if not isinstance(files_list, list):
        logger.error("Aetna: unexpected metadata format")
        return await _aetna_fallback(payer)

    # Strategy 1: Find Iowa-specific TOC and parse it
    iowa_toc = None
    main_toc = None
    for entry in files_list:
        if not isinstance(entry, dict):
            continue
        schema = entry.get("fileSchema", "")
        if schema != "TABLE_OF_CONTENTS":
            continue
        entity = entry.get("reportingEntityName", "").lower()
        file_path = entry.get("filePath", "")
        if "iowa" in entity:
            iowa_toc = file_path
            break
        if "aetna life insurance" in entity and not main_toc:
            main_toc = file_path

    toc_path = iowa_toc or main_toc
    if toc_path:
        toc_url = f"{_AETNA_BASE}/{_AETNA_BRAND}/{toc_path}"
        logger.info("Aetna: parsing TOC at %s", toc_url[:120])
        try:
            files = await parse_toc_from_url(toc_url)
            if files:
                logger.info("Aetna: found %d MRF files from TOC", len(files))
                return files
        except Exception as e:
            logger.error("Aetna TOC parse failed: %s", e)

    # Strategy 2: Build MRF file list directly from metadata entries
    logger.info("Aetna: building file list from metadata entries")
    results: list[MrfFileInfo] = []
    seen: set[str] = set()
    for entry in files_list:
        if not isinstance(entry, dict):
            continue
        schema = entry.get("fileSchema", "")
        if schema != "IN_NETWORK_RATES":
            continue
        file_path = entry.get("filePath", "")
        file_name = entry.get("fileName", "")
        if not file_path or file_path in seen:
            continue
        seen.add(file_path)
        download_url = f"{_AETNA_BASE}/{_AETNA_BRAND}/{file_path}"
        url_hash = _stable_hash(file_name or file_path)
        results.append(MrfFileInfo(
            url=download_url,
            url_hash=url_hash,
            description=f"Aetna: {file_name}",
        ))

    logger.info("Aetna: found %d in-network rate files from metadata", len(results))
    return results


async def _aetna_fallback(payer: dict) -> list[MrfFileInfo]:
    """Legacy date-templated approach as fallback."""
    template = payer.get("toc_url", "")
    if not template or "{YYYY-MM-DD}" not in template:
        return []

    now = datetime.now()
    for months_back in range(4):
        target = now - timedelta(days=months_back * 30)
        resolved_url = _aetna_resolve_url(template, target)
        logger.info("Aetna fallback: trying TOC at %s", resolved_url)
        try:
            files = await parse_toc_from_url(resolved_url)
            if files:
                return files
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                continue
            logger.error("Aetna fallback error: %s", e)
            return []
        except Exception as e:
            logger.error("Aetna fallback error: %s", e)
            return []
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
            page_html = resp.text
        except Exception as e:
            logger.error("Cigna page fetch error: %s", e)
            return []

    # Extract CloudFront TOC URL(s) from HTML
    matches = _CIGNA_TOC_PATTERN.findall(page_html)
    if not matches:
        logger.warning("Cigna: no CloudFront TOC URL found on compliance page")
        return []

    # Decode HTML entities (&amp; → &) — URLs in HTML attributes are entity-encoded
    matches = [html.unescape(m) for m in matches]

    # Prefer the federal CMS TOC (no state abbreviation in path) over state-specific
    federal_matches = [m for m in matches if "/state_mrf/" not in m]
    toc_url = federal_matches[0] if federal_matches else matches[0]
    logger.info("Cigna: extracted TOC URL (%d found): %s", len(matches), toc_url[:120])

    try:
        return await parse_toc_from_url(toc_url)
    except Exception as e:
        logger.error("Cigna TOC parse error: %s", e)
        return []


# ---------------------------------------------------------------------------
# Medica adapter — direct GCS bucket probing for Iowa plan files
# ---------------------------------------------------------------------------

_MEDICA_BASE = "https://mrf.healthsparq.com/medica-egress.nophi.kyruushsq.com/prd/mrf/MEDICA_I/MEDICA"
_MEDICA_IOWA_PLANS = [
    "Elevate_by_Medica-IA_Medica_In_Network.zip",
    "Inspire_by_Medica-IA_Medica_In_Network.zip",
    "Medica_Choice_National-IA_Medica_In_Network.zip",
    "Empower_by_Medica-IA_Medica_In_Network.zip",
]


async def _medica_get_mrf_files(payer: dict) -> list[MrfFileInfo]:
    """Probe Medica's GCS bucket for known Iowa plan files.

    Medica doesn't publish a standard TOC JSON. Instead, we try known Iowa plan
    file names across recent dates until we find ones that exist.
    """
    results: list[MrfFileInfo] = []
    now = datetime.now()

    # Try dates from current month back to 12 months ago (1st of each month)
    dates_to_try = []
    for months_back in range(13):
        d = now - timedelta(days=months_back * 30)
        dates_to_try.append(d.replace(day=1).strftime("%Y-%m-%d"))

    async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
        for date_str in dates_to_try:
            found_any = False
            for plan_name in _MEDICA_IOWA_PLANS:
                url = f"{_MEDICA_BASE}/{date_str}/inNetworkRates/{plan_name}"
                try:
                    resp = await client.head(url)
                    if resp.status_code == 200:
                        content_length = int(resp.headers.get("content-length", "0"))
                        if content_length < 100:
                            continue
                        size_kb = content_length / 1024
                        results.append(MrfFileInfo(
                            url=url,
                            url_hash=_stable_hash(f"{date_str}/{plan_name}"),
                            description=f"Medica: {plan_name} ({date_str}, {size_kb:.0f} KB)",
                        ))
                        found_any = True
                        logger.info("Medica: found %s at %s (%d KB)", plan_name, date_str, size_kb)
                except Exception:
                    continue

            if found_any:
                # Found files for this date — use this month's data
                logger.info("Medica: found %d Iowa files at date %s", len(results), date_str)
                return results

    logger.warning("Medica: no Iowa plan files found in last 12 months")
    return results


# ---------------------------------------------------------------------------
# Wellmark adapter — latest_metadata.json → Iowa TOC → MRF URLs
# (Same HealthSparq platform as Aetna, different brand code)
# ---------------------------------------------------------------------------

_WELLMARK_BASE = "https://mrf.healthsparq.com/wmrk-egress.nophi.kyruushsq.com/prd/mrf/WMRK_I"
_WELLMARK_BRAND = "WELLMARK"


async def _wellmark_get_mrf_files(payer: dict) -> list[MrfFileInfo]:
    """Fetch Wellmark's latest_metadata.json, find Iowa TOC, parse MRF URLs.

    Wellmark uses the same HealthSparq platform as Aetna. Their metadata
    endpoint lists all available TOC and in-network rate files.
    """
    metadata_url = f"{_WELLMARK_BASE}/{_WELLMARK_BRAND}/latest_metadata.json"
    logger.info("Wellmark: fetching metadata from %s", metadata_url)

    async with httpx.AsyncClient(timeout=120, follow_redirects=True) as client:
        try:
            resp = await client.get(metadata_url)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            logger.error("Wellmark metadata fetch failed: %s", e)
            return []

    files_list = data.get("files", data) if isinstance(data, dict) else data
    if not isinstance(files_list, list):
        logger.error("Wellmark: unexpected metadata format")
        return []

    # Strategy 1: Find Iowa-specific TOC and parse it
    iowa_toc = None
    main_toc = None
    for entry in files_list:
        if not isinstance(entry, dict):
            continue
        schema = entry.get("fileSchema", "")
        if schema != "TABLE_OF_CONTENTS":
            continue
        entity = entry.get("reportingEntityName", "").lower()
        file_path = entry.get("filePath", "")
        if "iowa" in entity or "wellmark" in entity:
            iowa_toc = file_path
            break
        if not main_toc:
            main_toc = file_path

    toc_path = iowa_toc or main_toc
    if toc_path:
        toc_url = f"{_WELLMARK_BASE}/{_WELLMARK_BRAND}/{toc_path}"
        logger.info("Wellmark: parsing TOC at %s", toc_url[:120])
        try:
            files = await parse_toc_from_url(toc_url)
            if files:
                logger.info("Wellmark: found %d MRF files from TOC", len(files))
                return files
        except Exception as e:
            logger.error("Wellmark TOC parse failed: %s", e)

    # Strategy 2: Build MRF file list directly from metadata entries
    logger.info("Wellmark: building file list from metadata entries")
    results: list[MrfFileInfo] = []
    seen: set[str] = set()
    for entry in files_list:
        if not isinstance(entry, dict):
            continue
        schema = entry.get("fileSchema", "")
        if schema != "IN_NETWORK_RATES":
            continue
        file_path = entry.get("filePath", "")
        file_name = entry.get("fileName", "")
        if not file_path or file_path in seen:
            continue
        seen.add(file_path)
        download_url = f"{_WELLMARK_BASE}/{_WELLMARK_BRAND}/{file_path}"
        url_hash = _stable_hash(file_name or file_path)
        results.append(MrfFileInfo(
            url=download_url,
            url_hash=url_hash,
            description=f"Wellmark: {file_name}",
        ))

    logger.info("Wellmark: found %d in-network rate files from metadata", len(results))
    return results


# ---------------------------------------------------------------------------
# Dispatch
# ---------------------------------------------------------------------------

_ADAPTERS: dict[str, callable] = {
    "uhc": _uhc_get_mrf_files,
    "aetna": _aetna_get_mrf_files,
    "wellmark": _wellmark_get_mrf_files,
    "cigna": _cigna_get_mrf_files,
    "medica": _medica_get_mrf_files,
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
