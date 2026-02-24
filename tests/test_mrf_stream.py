"""Tests for etl/mrf_stream.py — MRF streaming processor."""

import json
import pathlib

import pytest

from etl.mrf_stream import MrfStreamProcessor, RateRecord

FIXTURE_DIR = pathlib.Path(__file__).parent / "fixtures"

# Iowa NPIs matching the sample_mrf.json fixture
IOWA_NPIS = {"1234567890", "2345678901"}
# Target CPT codes in our lookup
TARGET_CODES = {"27447", "99213", "45378"}


async def _bytes_from_file(path: pathlib.Path):
    """Async generator yielding file contents as byte chunks."""
    data = path.read_bytes()
    for i in range(0, len(data), 4096):
        yield data[i:i + 4096]


async def _collect_rates(processor, byte_source) -> list[RateRecord]:
    """Collect all rate records from a processor."""
    records = []
    async for batch in processor.stream_rates_from_bytes(byte_source):
        records.extend(batch)
    return records


@pytest.mark.asyncio
async def test_provider_references_extracted(sample_mrf_path):
    """3 provider groups in fixture, 2 contain Iowa NPIs."""
    processor = MrfStreamProcessor(
        iowa_npis=IOWA_NPIS, target_cpt_codes=TARGET_CODES
    )
    await _collect_rates(processor, _bytes_from_file(sample_mrf_path))
    assert processor.result.provider_groups_total == 3
    assert processor.result.iowa_provider_groups == 2


@pytest.mark.asyncio
async def test_cpt_filtering_skips_non_target_codes(sample_mrf_path):
    """Code 99999 is not in TARGET_CODES, so it should be skipped."""
    processor = MrfStreamProcessor(
        iowa_npis=IOWA_NPIS, target_cpt_codes=TARGET_CODES
    )
    records = await _collect_rates(processor, _bytes_from_file(sample_mrf_path))
    codes = {r.billing_code for r in records}
    assert "99999" not in codes
    # 3 in_network items total, 2 match target codes
    assert processor.result.total_in_network_items == 3
    assert processor.result.matched_cpt_items == 2


@pytest.mark.asyncio
async def test_iowa_filtering_excludes_non_iowa_npis(sample_mrf_path):
    """NPI 9999999999 (group 2) is non-Iowa — should not appear in results."""
    processor = MrfStreamProcessor(
        iowa_npis=IOWA_NPIS, target_cpt_codes=TARGET_CODES
    )
    records = await _collect_rates(processor, _bytes_from_file(sample_mrf_path))
    npis_in_results = {r.npi for r in records}
    assert "9999999999" not in npis_in_results


@pytest.mark.asyncio
async def test_rate_expansion_27447(sample_mrf_path):
    """27447 references groups [1,3] → 2 Iowa NPIs → 2 RateRecords."""
    processor = MrfStreamProcessor(
        iowa_npis=IOWA_NPIS, target_cpt_codes=TARGET_CODES
    )
    records = await _collect_rates(processor, _bytes_from_file(sample_mrf_path))
    knee_records = [r for r in records if r.billing_code == "27447"]
    assert len(knee_records) == 2
    npis = {r.npi for r in knee_records}
    assert npis == {"1234567890", "2345678901"}
    assert all(r.negotiated_rate == 45000.0 for r in knee_records)


@pytest.mark.asyncio
async def test_total_records(sample_mrf_path):
    """Expected: 2 records for 27447 + 1 record for 99213 = 3 total."""
    processor = MrfStreamProcessor(
        iowa_npis=IOWA_NPIS, target_cpt_codes=TARGET_CODES
    )
    records = await _collect_rates(processor, _bytes_from_file(sample_mrf_path))
    assert len(records) == 3
    assert processor.result.iowa_rates_extracted == 3


@pytest.mark.asyncio
async def test_batch_yielding():
    """Batch size is respected."""
    processor = MrfStreamProcessor(
        iowa_npis=IOWA_NPIS, target_cpt_codes=TARGET_CODES, batch_size=2
    )
    mrf_path = FIXTURE_DIR / "sample_mrf.json"
    batches = []
    async for batch in processor.stream_rates_from_bytes(_bytes_from_file(mrf_path)):
        batches.append(batch)
    # 3 total records with batch_size=2 → 2 batches (2 + 1)
    assert len(batches) == 2
    assert len(batches[0]) == 2
    assert len(batches[1]) == 1


@pytest.mark.asyncio
async def test_empty_file_produces_no_rates():
    """An empty MRF-like JSON should produce zero rates and no errors."""
    empty_mrf = json.dumps({
        "provider_references": [],
        "in_network": [],
    }).encode()

    async def _empty_bytes():
        yield empty_mrf

    processor = MrfStreamProcessor(
        iowa_npis=IOWA_NPIS, target_cpt_codes=TARGET_CODES
    )
    records = await _collect_rates(processor, _empty_bytes())
    assert len(records) == 0
    assert processor.result.total_in_network_items == 0
    assert len(processor.result.errors) == 0
