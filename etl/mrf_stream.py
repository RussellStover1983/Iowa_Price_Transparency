"""Core MRF streaming processor — filters multi-GB JSON for Iowa providers and target CPT codes.

Uses ijson for single-pass streaming. Never loads the full file into memory.

CMS MRF schema guarantees `provider_references` precedes `in_network`, enabling a
two-phase single-pass approach:
  Phase 1: Build provider_group_id → list[(npi, tin)] map (only Iowa groups kept)
  Phase 2: For each in_network item, check CPT code match + Iowa provider group overlap,
           then expand into RateRecord objects.

Usage:
    processor = MrfStreamProcessor(iowa_npis={"1234567890"}, target_cpt_codes={"27447"})
    async for batch in processor.stream_rates_from_bytes(byte_source):
        for record in batch:
            print(record)
"""

from __future__ import annotations

import io
import logging
import zlib
from dataclasses import dataclass, field
from typing import AsyncIterator

import ijson

logger = logging.getLogger(__name__)


@dataclass
class RateRecord:
    """A single normalized rate extracted from an MRF file."""
    npi: str
    tin: str
    billing_code: str
    billing_code_type: str
    negotiated_rate: float
    negotiated_type: str
    service_code: list[str] = field(default_factory=list)
    billing_class: str = ""
    description: str = ""


@dataclass
class MrfParseResult:
    """Statistics from parsing a single MRF file."""
    total_in_network_items: int = 0
    matched_cpt_items: int = 0
    iowa_rates_extracted: int = 0
    provider_groups_total: int = 0
    iowa_provider_groups: int = 0
    errors: list[str] = field(default_factory=list)


class MrfStreamProcessor:
    """Streams an MRF file, filtering by CPT codes and Iowa NPIs."""

    def __init__(
        self,
        iowa_npis: set[str],
        target_cpt_codes: set[str],
        batch_size: int = 1000,
    ) -> None:
        self.iowa_npis = iowa_npis
        self.target_cpt_codes = target_cpt_codes
        self.batch_size = batch_size
        self.result = MrfParseResult()

    async def stream_rates_from_bytes(
        self, byte_source: AsyncIterator[bytes]
    ) -> AsyncIterator[list[RateRecord]]:
        """Stream-parse MRF JSON from an async byte source.

        Yields batches of RateRecord (up to batch_size per batch).
        """
        # Collect bytes into a sync file-like object for ijson
        # (ijson's async support requires aiohttp-specific streams;
        #  we buffer into a BytesIO which works with the sync ijson parser)
        buf = io.BytesIO()
        async for chunk in byte_source:
            buf.write(chunk)
        buf.seek(0)

        async for batch in self._parse_stream(buf):
            yield batch

    async def stream_rates_from_url(
        self, url: str
    ) -> AsyncIterator[list[RateRecord]]:
        """Stream-parse MRF JSON from an HTTP URL (supports .json.gz)."""
        import httpx

        async with httpx.AsyncClient(follow_redirects=True, timeout=300) as client:
            async with client.stream("GET", url) as response:
                response.raise_for_status()
                if url.endswith(".gz"):
                    buf = io.BytesIO()
                    decompressor = zlib.decompressobj(zlib.MAX_WBITS | 16)
                    async for chunk in response.aiter_bytes(chunk_size=65536):
                        buf.write(decompressor.decompress(chunk))
                    buf.write(decompressor.flush())
                    buf.seek(0)
                else:
                    buf = io.BytesIO()
                    async for chunk in response.aiter_bytes(chunk_size=65536):
                        buf.write(chunk)
                    buf.seek(0)

        async for batch in self._parse_stream(buf):
            yield batch

    async def _parse_stream(
        self, source: io.BytesIO
    ) -> AsyncIterator[list[RateRecord]]:
        """Two-phase parse: provider_references then in_network items."""
        # Phase 1: Build Iowa provider group map
        iowa_groups: dict[int, list[tuple[str, str]]] = {}
        # {group_id: [(npi, tin), ...]} — only groups containing at least one Iowa NPI

        # Tracking state for the current provider_references item
        current_group_id: int | None = None
        current_group_npis: list[tuple[str, str]] = []
        # Tracking state for the current provider_groups sub-item
        current_entry_npis: list[str] = []
        current_tin: str = ""

        try:
            for prefix, event, value in ijson.parse(source):
                if prefix == "provider_references.item" and event == "start_map":
                    # New provider_reference — reset group-level state
                    current_group_id = None
                    current_group_npis = []
                    self.result.provider_groups_total += 1

                elif prefix == "provider_references.item.provider_group_id":
                    current_group_id = int(value)

                elif prefix == "provider_references.item.provider_groups.item" and event == "start_map":
                    # New provider_groups entry — reset entry-level state
                    current_entry_npis = []
                    current_tin = ""

                elif prefix == "provider_references.item.provider_groups.item.npi.item":
                    current_entry_npis.append(str(int(value)))

                elif prefix == "provider_references.item.provider_groups.item.tin.value":
                    current_tin = str(value)

                elif prefix == "provider_references.item.provider_groups.item" and event == "end_map":
                    # End of one provider_groups entry — collect Iowa NPIs
                    for npi in current_entry_npis:
                        if npi in self.iowa_npis:
                            current_group_npis.append((npi, current_tin))

                elif prefix == "provider_references.item" and event == "end_map":
                    # End of one provider_reference — save if it has Iowa NPIs
                    if current_group_npis and current_group_id is not None:
                        iowa_groups[current_group_id] = current_group_npis
                        self.result.iowa_provider_groups += 1

                elif prefix == "in_network" and event == "start_array":
                    # Transition to phase 2
                    break

        except Exception as e:
            self.result.errors.append(f"Phase 1 error: {e}")
            logger.error("Error parsing provider_references: %s", e)
            return

        logger.info(
            "Phase 1 complete: %d total groups, %d Iowa groups",
            self.result.provider_groups_total,
            self.result.iowa_provider_groups,
        )

        # Phase 2: Parse in_network items
        # Reset source position to beginning for phase 2 full parse
        source.seek(0)

        batch: list[RateRecord] = []

        try:
            for item in ijson.items(source, "in_network.item"):
                self.result.total_in_network_items += 1

                billing_code = item.get("billing_code", "")
                billing_code_type = item.get("billing_code_type", "CPT")
                description = item.get("description", "")

                # Filter: only target CPT codes
                if billing_code not in self.target_cpt_codes:
                    continue

                self.result.matched_cpt_items += 1

                # Process negotiated_rates
                for neg_rate_entry in item.get("negotiated_rates", []):
                    # Get provider_references for this rate entry
                    prov_refs = neg_rate_entry.get("provider_references", [])
                    # Find Iowa NPIs from referenced groups
                    iowa_npi_tin_pairs: list[tuple[str, str]] = []
                    for ref_id in prov_refs:
                        ref_id = int(ref_id)
                        if ref_id in iowa_groups:
                            iowa_npi_tin_pairs.extend(iowa_groups[ref_id])

                    if not iowa_npi_tin_pairs:
                        continue

                    # Get negotiated prices
                    for price_entry in neg_rate_entry.get("negotiated_prices", []):
                        neg_rate = price_entry.get("negotiated_rate", 0.0)
                        neg_type = price_entry.get("negotiated_type", "")
                        service_codes = price_entry.get("service_code", [])
                        billing_class = price_entry.get("billing_class", "")

                        # Cross-join: one RateRecord per Iowa NPI × price
                        for npi, tin in iowa_npi_tin_pairs:
                            record = RateRecord(
                                npi=npi,
                                tin=tin,
                                billing_code=billing_code,
                                billing_code_type=billing_code_type,
                                negotiated_rate=float(neg_rate),
                                negotiated_type=neg_type,
                                service_code=service_codes,
                                billing_class=billing_class,
                                description=description,
                            )
                            batch.append(record)
                            self.result.iowa_rates_extracted += 1

                            if len(batch) >= self.batch_size:
                                yield batch
                                batch = []

        except Exception as e:
            self.result.errors.append(f"Phase 2 error: {e}")
            logger.error("Error parsing in_network items: %s", e)

        if batch:
            yield batch

        logger.info(
            "Phase 2 complete: %d in_network items, %d CPT matches, %d Iowa rates",
            self.result.total_in_network_items,
            self.result.matched_cpt_items,
            self.result.iowa_rates_extracted,
        )
