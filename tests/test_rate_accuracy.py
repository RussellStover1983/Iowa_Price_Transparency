"""Tests for rate extraction accuracy — field-by-field validation against fixtures.

Pure parsing tests (no DB required). Validates cross-join expansion, field mapping,
data type coercion, and filtering completeness.
"""

import pathlib

import pytest

from etl.mrf_stream import MrfStreamProcessor, RateRecord

FIXTURE_DIR = pathlib.Path(__file__).parent / "fixtures"

# Iowa NPIs matching the complex_mrf.json fixture
COMPLEX_IOWA_NPIS = {"1234567890", "2345678901", "3456789012"}
TARGET_CODES = {"27447", "99213", "45378"}

# Edge case fixture uses same NPI as sample_mrf
EDGE_CASE_IOWA_NPIS = {"1234567890"}


async def _bytes_from_file(path: pathlib.Path):
    """Async generator yielding file contents as byte chunks."""
    data = path.read_bytes()
    for i in range(0, len(data), 4096):
        yield data[i : i + 4096]


async def _collect_rates(processor, byte_source) -> list[RateRecord]:
    """Collect all rate records from a processor."""
    records = []
    async for batch in processor.stream_rates_from_bytes(byte_source):
        records.extend(batch)
    return records


# --- Complex MRF: cross-join expansion tests ---


@pytest.mark.asyncio
async def test_cross_join_2_npis_x_2_prices(complex_mrf_path):
    """Item 1: code 27447, group [10] (2 NPIs), 2 prices = 4 records."""
    processor = MrfStreamProcessor(
        iowa_npis=COMPLEX_IOWA_NPIS, target_cpt_codes=TARGET_CODES
    )
    records = await _collect_rates(processor, _bytes_from_file(complex_mrf_path))

    # Item 1 has description "Total knee replacement (arthroplasty)" with 2 prices
    item1_records = [
        r
        for r in records
        if r.billing_code == "27447"
        and r.description == "Total knee replacement (arthroplasty)"
    ]
    assert len(item1_records) == 4

    # 2 NPIs x 2 prices
    npis = {r.npi for r in item1_records}
    assert npis == {"1234567890", "2345678901"}
    rates = sorted({r.negotiated_rate for r in item1_records})
    assert rates == [38000.0, 45000.0]


@pytest.mark.asyncio
async def test_cross_join_multi_group_single_price(complex_mrf_path):
    """Item 2: code 27447, groups [20, 40], 1 price = 2 records with correct NPIs."""
    processor = MrfStreamProcessor(
        iowa_npis=COMPLEX_IOWA_NPIS, target_cpt_codes=TARGET_CODES
    )
    records = await _collect_rates(processor, _bytes_from_file(complex_mrf_path))

    # Item 2 has description "Total knee replacement alternative rate"
    item2_records = [
        r
        for r in records
        if r.billing_code == "27447"
        and r.description == "Total knee replacement alternative rate"
    ]
    assert len(item2_records) == 2

    npis = {r.npi for r in item2_records}
    # Group 20 has NPI 1234567890, Group 40 has NPI 3456789012
    assert npis == {"1234567890", "3456789012"}
    assert all(r.negotiated_rate == 42000.0 for r in item2_records)


@pytest.mark.asyncio
async def test_total_record_count_complex(complex_mrf_path):
    """Exactly 9 records total from the complex fixture."""
    processor = MrfStreamProcessor(
        iowa_npis=COMPLEX_IOWA_NPIS, target_cpt_codes=TARGET_CODES
    )
    records = await _collect_rates(processor, _bytes_from_file(complex_mrf_path))
    assert len(records) == 9
    assert processor.result.iowa_rates_extracted == 9


# --- Complex MRF: field accuracy tests ---


@pytest.mark.asyncio
async def test_field_negotiated_type(complex_mrf_path):
    """2 records have 'fee schedule', 7 have 'negotiated'."""
    processor = MrfStreamProcessor(
        iowa_npis=COMPLEX_IOWA_NPIS, target_cpt_codes=TARGET_CODES
    )
    records = await _collect_rates(processor, _bytes_from_file(complex_mrf_path))

    fee_schedule = [r for r in records if r.negotiated_type == "fee schedule"]
    negotiated = [r for r in records if r.negotiated_type == "negotiated"]
    assert len(fee_schedule) == 2
    assert len(negotiated) == 7


@pytest.mark.asyncio
async def test_field_billing_class(complex_mrf_path):
    """Institutional vs professional mapped correctly per price entry."""
    processor = MrfStreamProcessor(
        iowa_npis=COMPLEX_IOWA_NPIS, target_cpt_codes=TARGET_CODES
    )
    records = await _collect_rates(processor, _bytes_from_file(complex_mrf_path))

    institutional = [r for r in records if r.billing_class == "institutional"]
    professional = [r for r in records if r.billing_class == "professional"]
    # 99213 items are professional (2 records), rest are institutional (7 records)
    assert len(professional) == 2
    assert len(institutional) == 7


@pytest.mark.asyncio
async def test_field_service_code(complex_mrf_path):
    """All service_code values are lists; match fixture values."""
    processor = MrfStreamProcessor(
        iowa_npis=COMPLEX_IOWA_NPIS, target_cpt_codes=TARGET_CODES
    )
    records = await _collect_rates(processor, _bytes_from_file(complex_mrf_path))

    for r in records:
        assert isinstance(r.service_code, list)

    # 99213 records have service_code ["11"], all others have ["21"]
    for r in records:
        if r.billing_code == "99213":
            assert r.service_code == ["11"]
        else:
            assert r.service_code == ["21"]


@pytest.mark.asyncio
async def test_field_tin(complex_mrf_path):
    """Each NPI always maps to the correct TIN from provider_references."""
    processor = MrfStreamProcessor(
        iowa_npis=COMPLEX_IOWA_NPIS, target_cpt_codes=TARGET_CODES
    )
    records = await _collect_rates(processor, _bytes_from_file(complex_mrf_path))

    expected_tins = {
        "1234567890": "421234567",
        "2345678901": "422345678",
        "3456789012": "423456789",
    }
    for r in records:
        assert r.tin == expected_tins[r.npi], (
            f"NPI {r.npi} expected TIN {expected_tins[r.npi]}, got {r.tin}"
        )


@pytest.mark.asyncio
async def test_field_description(complex_mrf_path):
    """Each record's description matches its in_network item."""
    processor = MrfStreamProcessor(
        iowa_npis=COMPLEX_IOWA_NPIS, target_cpt_codes=TARGET_CODES
    )
    records = await _collect_rates(processor, _bytes_from_file(complex_mrf_path))

    descriptions = {r.description for r in records}
    expected = {
        "Total knee replacement (arthroplasty)",
        "Total knee replacement alternative rate",
        "Office visit established patient level 3",
        "Colonoscopy diagnostic Iowa provider",
    }
    assert descriptions == expected


# --- Complex MRF: filtering completeness tests ---


@pytest.mark.asyncio
async def test_non_iowa_group_zero_records(complex_mrf_path):
    """Code 45378 via group 30 only (non-Iowa NPI) produces 0 records for that item."""
    processor = MrfStreamProcessor(
        iowa_npis=COMPLEX_IOWA_NPIS, target_cpt_codes=TARGET_CODES
    )
    records = await _collect_rates(processor, _bytes_from_file(complex_mrf_path))

    # Item 5 (group 30 only, non-Iowa) should not appear
    non_iowa_colonoscopy = [
        r
        for r in records
        if r.description == "Colonoscopy diagnostic non-Iowa only"
    ]
    assert len(non_iowa_colonoscopy) == 0

    # Item 6 (group 40, Iowa) should appear
    iowa_colonoscopy = [
        r
        for r in records
        if r.description == "Colonoscopy diagnostic Iowa provider"
    ]
    assert len(iowa_colonoscopy) == 1


@pytest.mark.asyncio
async def test_all_target_codes_present(complex_mrf_path):
    """Result codes include all three target codes (nothing silently dropped)."""
    processor = MrfStreamProcessor(
        iowa_npis=COMPLEX_IOWA_NPIS, target_cpt_codes=TARGET_CODES
    )
    records = await _collect_rates(processor, _bytes_from_file(complex_mrf_path))

    codes_found = {r.billing_code for r in records}
    assert codes_found == {"27447", "99213", "45378"}


# --- Edge case MRF: data type integrity tests ---


@pytest.mark.asyncio
async def test_zero_rate_not_filtered(edge_case_mrf_path):
    """Rate 0.00 is extracted, not treated as falsy."""
    processor = MrfStreamProcessor(
        iowa_npis=EDGE_CASE_IOWA_NPIS, target_cpt_codes=TARGET_CODES
    )
    records = await _collect_rates(processor, _bytes_from_file(edge_case_mrf_path))

    zero_rate = [r for r in records if r.negotiated_rate == 0.0]
    assert len(zero_rate) == 1
    assert zero_rate[0].billing_code == "99213"
    assert zero_rate[0].description == "Office visit with zero negotiated rate"


@pytest.mark.asyncio
async def test_integer_rate_coerced_to_float(edge_case_mrf_path):
    """JSON integer 100 is coerced to float(100.0) in RateRecord."""
    processor = MrfStreamProcessor(
        iowa_npis=EDGE_CASE_IOWA_NPIS, target_cpt_codes=TARGET_CODES
    )
    records = await _collect_rates(processor, _bytes_from_file(edge_case_mrf_path))

    int_rate = [r for r in records if r.billing_code == "45378"]
    assert len(int_rate) == 1
    assert int_rate[0].negotiated_rate == 100.0
    assert isinstance(int_rate[0].negotiated_rate, float)
