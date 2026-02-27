"""Parse payer Table of Contents (TOC) JSON to extract in-network MRF file URLs.

TOC files can be 100MB+ so we use ijson streaming. Extracts URLs from
reporting_structure.*.in_network_files.*.location, filtering to include only
in-network/negotiated-rates files (excluding allowed-amounts).

Usage:
    files = await parse_toc_from_bytes(byte_source)
    for f in files:
        print(f.url, f.url_hash)
"""

from __future__ import annotations

import hashlib
import io
import logging
import zlib
from dataclasses import dataclass
from typing import AsyncIterator

import ijson

logger = logging.getLogger(__name__)

# Keywords that identify in-network rate files (vs. allowed-amounts)
_IN_NETWORK_KEYWORDS = {"in-network", "in_network", "negotiated"}
_EXCLUDE_KEYWORDS = {"allowed-amount", "allowed_amount", "allowed amount"}


@dataclass
class MrfFileInfo:
    """Metadata for a single MRF file discovered in a TOC."""
    url: str
    url_hash: str
    description: str = ""


def compute_url_hash(url: str) -> str:
    """SHA-256 of URL, truncated to 16 hex chars for mrf_files.file_hash."""
    return hashlib.sha256(url.encode()).hexdigest()[:16]


def _is_in_network_file(url: str, description: str = "") -> bool:
    """Check if a URL/description indicates an in-network rates file."""
    combined = (url + " " + description).lower()
    # Exclude if it matches an exclusion keyword
    for kw in _EXCLUDE_KEYWORDS:
        if kw in combined:
            return False
    # Include if it matches an in-network keyword
    for kw in _IN_NETWORK_KEYWORDS:
        if kw in combined:
            return True
    # Default: include (some payers don't use standard naming)
    return True


async def parse_toc_from_bytes(
    byte_source: AsyncIterator[bytes],
) -> list[MrfFileInfo]:
    """Parse a TOC JSON from an async byte source, extracting in-network MRF URLs."""
    buf = io.BytesIO()
    async for chunk in byte_source:
        buf.write(chunk)
    buf.seek(0)
    return _parse_toc_sync(buf)


async def parse_toc_from_url(toc_url: str) -> list[MrfFileInfo]:
    """Download and parse a TOC JSON from a URL (supports .json.gz)."""
    import httpx

    async with httpx.AsyncClient(follow_redirects=True, timeout=120) as client:
        async with client.stream("GET", toc_url) as response:
            response.raise_for_status()
            buf = io.BytesIO()
            url_path = toc_url.split("?")[0]
            if url_path.endswith(".gz"):
                # Handle multi-member gzip (concatenated gzip streams)
                decompressor = zlib.decompressobj(zlib.MAX_WBITS | 16)
                async for chunk in response.aiter_bytes(chunk_size=65536):
                    data = chunk
                    while data:
                        buf.write(decompressor.decompress(data))
                        if decompressor.eof:
                            data = decompressor.unused_data
                            decompressor = zlib.decompressobj(zlib.MAX_WBITS | 16)
                        else:
                            break
                buf.write(decompressor.flush())
            else:
                async for chunk in response.aiter_bytes(chunk_size=65536):
                    buf.write(chunk)
    buf.seek(0)
    return _parse_toc_sync(buf)


def _parse_toc_sync(source: io.BytesIO) -> list[MrfFileInfo]:
    """Streaming parse of TOC JSON with ijson."""
    results: list[MrfFileInfo] = []
    seen_urls: set[str] = set()

    current_description = ""
    current_location = ""

    for prefix, event, value in ijson.parse(source):
        # reporting_structure.item.in_network_files.item.description
        if prefix.endswith("in_network_files.item.description"):
            current_description = str(value)

        elif prefix.endswith("in_network_files.item.location"):
            current_location = str(value)

        elif prefix.endswith("in_network_files.item") and event == "end_map":
            if current_location and current_location not in seen_urls:
                if _is_in_network_file(current_location, current_description):
                    results.append(MrfFileInfo(
                        url=current_location,
                        url_hash=compute_url_hash(current_location),
                        description=current_description,
                    ))
                    seen_urls.add(current_location)
                else:
                    logger.debug("Skipping non-in-network file: %s", current_location)
            current_description = ""
            current_location = ""

    logger.info("TOC parsed: %d in-network MRF file URLs found", len(results))
    return results
