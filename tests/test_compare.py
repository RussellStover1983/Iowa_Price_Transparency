"""Tests for the compare endpoint."""

import pytest


@pytest.mark.asyncio
async def test_single_code_returns_providers(client):
    """Single code returns grouped results with providers."""
    response = await client.get("/v1/compare", params={"codes": "27447"})
    assert response.status_code == 200
    data = response.json()
    assert len(data["procedures"]) == 1
    proc = data["procedures"][0]
    assert proc["billing_code"] == "27447"
    assert proc["provider_count"] >= 1
    assert len(proc["providers"]) == proc["provider_count"]


@pytest.mark.asyncio
async def test_multiple_codes(client):
    """Multiple codes returns one entry per code."""
    response = await client.get("/v1/compare", params={"codes": "27447,45378"})
    assert response.status_code == 200
    data = response.json()
    assert len(data["procedures"]) == 2
    codes = [p["billing_code"] for p in data["procedures"]]
    assert "27447" in codes
    assert "45378" in codes


@pytest.mark.asyncio
async def test_provider_rates_have_payer_info(client):
    """Provider rates have payer_name and positive negotiated_rate."""
    response = await client.get("/v1/compare", params={"codes": "27447"})
    assert response.status_code == 200
    data = response.json()
    proc = data["procedures"][0]
    assert len(proc["providers"]) > 0
    provider = proc["providers"][0]
    assert len(provider["rates"]) > 0
    rate = provider["rates"][0]
    assert rate["payer_name"]
    assert rate["negotiated_rate"] > 0


@pytest.mark.asyncio
async def test_unknown_code_returns_empty_providers(client):
    """Unknown code returns 200 with empty providers, not 404."""
    response = await client.get("/v1/compare", params={"codes": "00000"})
    assert response.status_code == 200
    data = response.json()
    assert len(data["procedures"]) == 1
    assert data["procedures"][0]["providers"] == []
    assert data["procedures"][0]["provider_count"] == 0


@pytest.mark.asyncio
async def test_invalid_code_format(client):
    """Invalid code format returns 422."""
    response = await client.get("/v1/compare", params={"codes": "abc"})
    assert response.status_code == 422


@pytest.mark.asyncio
async def test_too_many_codes(client):
    """More than 10 codes returns 400."""
    codes = ",".join(str(10000 + i) for i in range(11))
    response = await client.get("/v1/compare", params={"codes": codes})
    assert response.status_code == 400


@pytest.mark.asyncio
async def test_preserves_code_order(client):
    """Response preserves requested code order."""
    response = await client.get("/v1/compare", params={"codes": "45378,27447"})
    assert response.status_code == 200
    data = response.json()
    assert data["codes_requested"] == ["45378", "27447"]
    assert data["procedures"][0]["billing_code"] == "45378"
    assert data["procedures"][1]["billing_code"] == "27447"


@pytest.mark.asyncio
async def test_total_providers_consistent(client):
    """total_providers count matches unique providers across all procedures."""
    response = await client.get("/v1/compare", params={"codes": "27447,45378"})
    assert response.status_code == 200
    data = response.json()
    all_provider_ids = set()
    for proc in data["procedures"]:
        for provider in proc["providers"]:
            all_provider_ids.add(provider["provider_id"])
    assert data["total_providers"] == len(all_provider_ids)
