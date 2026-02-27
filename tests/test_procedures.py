"""Tests for GET /v1/procedures/{code}/stats."""

import pytest


@pytest.mark.asyncio
async def test_procedure_stats_returns_detail(client):
    """Stats for a valid code returns all stat fields."""
    res = await client.get("/v1/procedures/27447/stats")
    assert res.status_code == 200
    data = res.json()
    assert data["billing_code"] == "27447"
    assert data["rate_count"] > 0
    assert data["provider_count"] > 0
    assert data["payer_count"] > 0
    assert data["min_rate"] <= data["median_rate"] <= data["max_rate"]
    assert data["p25_rate"] <= data["p75_rate"]
    assert data["potential_savings"] >= 0


@pytest.mark.asyncio
async def test_procedure_stats_has_percentiles(client):
    """Stats includes p25 and p75 percentiles."""
    res = await client.get("/v1/procedures/27447/stats")
    data = res.json()
    assert "p25_rate" in data
    assert "p75_rate" in data
    assert data["p25_rate"] <= data["median_rate"]
    assert data["p75_rate"] >= data["median_rate"]


@pytest.mark.asyncio
async def test_procedure_stats_not_found(client):
    """Unknown code returns 404."""
    res = await client.get("/v1/procedures/99999/stats")
    assert res.status_code == 404


@pytest.mark.asyncio
async def test_procedure_stats_invalid_code(client):
    """Non-numeric code returns 422."""
    res = await client.get("/v1/procedures/abc/stats")
    assert res.status_code == 422


@pytest.mark.asyncio
async def test_procedure_stats_description(client):
    """Stats includes CPT description and category."""
    res = await client.get("/v1/procedures/27447/stats")
    data = res.json()
    assert data["description"] is not None
    assert data["category"] is not None
