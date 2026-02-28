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
# Iowa NPIs for complex fixture (includes NPI from group 40)
COMPLEX_IOWA_NPIS = {"1234567890", "2345678901", "3456789012"}


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


# --- Complex fixture tests ---


@pytest.mark.asyncio
async def test_parse_result_stats_complex(complex_mrf_path):
    """Verify all MrfParseResult counters against complex fixture."""
    processor = MrfStreamProcessor(
        iowa_npis=COMPLEX_IOWA_NPIS, target_cpt_codes=TARGET_CODES
    )
    await _collect_rates(processor, _bytes_from_file(complex_mrf_path))
    r = processor.result
    assert r.provider_groups_total == 4
    assert r.iowa_provider_groups == 3  # groups 10, 20, 40 have Iowa NPIs
    assert r.total_in_network_items == 6
    assert r.matched_cpt_items == 5  # code 99999 filtered out
    assert r.iowa_rates_extracted == 9
    assert len(r.errors) == 0


@pytest.mark.asyncio
async def test_npi_appears_per_group_reference(complex_mrf_path):
    """Same NPI in two referenced groups produces duplicate records."""
    processor = MrfStreamProcessor(
        iowa_npis=COMPLEX_IOWA_NPIS, target_cpt_codes=TARGET_CODES
    )
    records = await _collect_rates(processor, _bytes_from_file(complex_mrf_path))

    # 99213 references groups [10, 30]. Group 10 has NPIs 1234567890 and 2345678901.
    # Group 30 is non-Iowa (9999999999), filtered out.
    office_records = [r for r in records if r.billing_code == "99213"]
    assert len(office_records) == 2
    npis = {r.npi for r in office_records}
    assert npis == {"1234567890", "2345678901"}


@pytest.mark.asyncio
async def test_multiple_negotiated_rates_entries(complex_mrf_path):
    """Code 27447 has two in_network items — both are processed."""
    processor = MrfStreamProcessor(
        iowa_npis=COMPLEX_IOWA_NPIS, target_cpt_codes=TARGET_CODES
    )
    records = await _collect_rates(processor, _bytes_from_file(complex_mrf_path))

    knee_records = [r for r in records if r.billing_code == "27447"]
    # Item 1: 2 NPIs x 2 prices = 4, Item 2: 2 NPIs x 1 price = 2 → total 6
    assert len(knee_records) == 6
    descriptions = {r.description for r in knee_records}
    assert "Total knee replacement (arthroplasty)" in descriptions
    assert "Total knee replacement alternative rate" in descriptions


# --- Inline provider_groups tests (CMS Schema alternate pattern) ---


@pytest.mark.asyncio
async def test_inline_provider_groups_extracted(inline_providers_mrf_path):
    """MRF with inline provider_groups (no top-level references) should still extract Iowa rates."""
    processor = MrfStreamProcessor(
        iowa_npis=IOWA_NPIS, target_cpt_codes=TARGET_CODES
    )
    records = await _collect_rates(processor, _bytes_from_file(inline_providers_mrf_path))
    # 27447: 2 Iowa NPIs (1234567890, 2345678901) x 1 price = 2 records
    # 99213: 1 Iowa NPI (1234567890) x 1 price = 1 record
    # Non-Iowa NPI 9999999999 is filtered out
    assert len(records) == 3
    assert processor.result.iowa_rates_extracted == 3
    assert len(processor.result.errors) == 0


@pytest.mark.asyncio
async def test_inline_provider_groups_filters_non_iowa(inline_providers_mrf_path):
    """Inline provider_groups correctly excludes non-Iowa NPIs."""
    processor = MrfStreamProcessor(
        iowa_npis=IOWA_NPIS, target_cpt_codes=TARGET_CODES
    )
    records = await _collect_rates(processor, _bytes_from_file(inline_providers_mrf_path))
    npis_in_results = {r.npi for r in records}
    assert "9999999999" not in npis_in_results
    assert "1234567890" in npis_in_results
    assert "2345678901" in npis_in_results


@pytest.mark.asyncio
async def test_inline_provider_groups_tin_preserved(inline_providers_mrf_path):
    """TIN values from inline provider_groups are correctly captured."""
    processor = MrfStreamProcessor(
        iowa_npis=IOWA_NPIS, target_cpt_codes=TARGET_CODES
    )
    records = await _collect_rates(processor, _bytes_from_file(inline_providers_mrf_path))
    tins = {(r.npi, r.tin) for r in records}
    assert ("1234567890", "421234567") in tins
    assert ("2345678901", "422345678") in tins


# --- TIN-based matching tests (UHC-style Type 1 NPIs) ---

# UHC fixture uses Type 1 NPIs (5551234567, etc.) — none in IOWA_NPIS.
# Iowa hospital TINs: 421234567, 422345678
UHC_IOWA_TINS = {"421234567", "422345678"}


@pytest.mark.asyncio
async def test_tin_based_matching_extracts_rates(uhc_style_mrf_path):
    """TIN-based matching extracts rates when NPIs don't match Iowa providers."""
    # No NPI overlap — all NPIs in fixture are Type 1 (555...)
    processor = MrfStreamProcessor(
        iowa_npis=set(),  # empty NPI set — simulates no NPI matches
        target_cpt_codes=TARGET_CODES,
        iowa_tins=UHC_IOWA_TINS,
    )
    records = await _collect_rates(processor, _bytes_from_file(uhc_style_mrf_path))

    # Group 1 (TIN 421234567): 2 NPIs → matched via TIN
    # Group 2 (TIN 422345678): 1 NPI → matched via TIN
    # Group 3 (TIN 999999999): not an Iowa TIN → excluded
    # 27447: groups [1,2] → 3 NPIs × 1 price = 3 records (group 3 excluded)
    # 99213 inline: TIN 421234567 matched → 1 NPI × 1 price = 1 record
    #               TIN 999999999 not matched → excluded
    assert len(records) == 4
    assert processor.result.iowa_provider_groups == 2  # groups 1 and 2


@pytest.mark.asyncio
async def test_tin_matching_excludes_non_iowa_tins(uhc_style_mrf_path):
    """Groups with non-Iowa TINs are excluded from TIN matching."""
    processor = MrfStreamProcessor(
        iowa_npis=set(),
        target_cpt_codes=TARGET_CODES,
        iowa_tins=UHC_IOWA_TINS,
    )
    records = await _collect_rates(processor, _bytes_from_file(uhc_style_mrf_path))
    tins_in_results = {r.tin for r in records}
    assert "999999999" not in tins_in_results


@pytest.mark.asyncio
async def test_npi_matching_takes_priority_over_tin(sample_mrf_path):
    """When both NPI and TIN could match, NPI-based matching is used."""
    # sample_mrf.json has Iowa NPIs 1234567890 and 2345678901
    # Also provide their TINs — NPI should take priority
    processor = MrfStreamProcessor(
        iowa_npis=IOWA_NPIS,
        target_cpt_codes=TARGET_CODES,
        iowa_tins={"421234567", "422345678"},
    )
    records = await _collect_rates(processor, _bytes_from_file(sample_mrf_path))
    # Should produce same results as without TIN matching
    assert len(records) == 3
    assert processor.result.iowa_provider_groups == 2


@pytest.mark.asyncio
async def test_tin_matching_without_tins_is_noop(uhc_style_mrf_path):
    """Without iowa_tins, Type 1 NPI files produce zero rates (backward compatible)."""
    processor = MrfStreamProcessor(
        iowa_npis=set(),  # no NPI matches
        target_cpt_codes=TARGET_CODES,
        # iowa_tins not provided — defaults to empty set
    )
    records = await _collect_rates(processor, _bytes_from_file(uhc_style_mrf_path))
    assert len(records) == 0
    assert processor.result.iowa_provider_groups == 0


@pytest.mark.asyncio
async def test_tin_matching_inline_provider_groups(uhc_style_mrf_path):
    """TIN matching works for inline provider_groups (Phase 2 pattern)."""
    processor = MrfStreamProcessor(
        iowa_npis=set(),
        target_cpt_codes={"99213"},  # only the inline one
        iowa_tins=UHC_IOWA_TINS,
    )
    records = await _collect_rates(processor, _bytes_from_file(uhc_style_mrf_path))
    # 99213 has inline provider_groups: TIN 421234567 matches, 999999999 doesn't
    assert len(records) == 1
    assert records[0].tin == "421234567"
    assert records[0].billing_code == "99213"
