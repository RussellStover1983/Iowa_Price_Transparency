"""Tests for GET /v1/providers/{id}/procedures."""

import pytest


@pytest.mark.asyncio
async def test_provider_procedures_returns_list(client):
    """Procedures endpoint returns a list of procedures at a provider."""
    res = await client.get("/v1/providers/1/procedures")
    assert res.status_code == 200
    data = res.json()
    assert data["provider_id"] == 1
    assert data["provider_name"] is not None
    assert isinstance(data["procedures"], list)
    assert data["total"] >= len(data["procedures"])


@pytest.mark.asyncio
async def test_provider_procedures_has_rates(client):
    """Each procedure has rate breakdown with payer info."""
    res = await client.get("/v1/providers/1/procedures?limit=5")
    data = res.json()
    if data["procedures"]:
        proc = data["procedures"][0]
        assert "billing_code" in proc
        assert "rates" in proc
        assert "min_rate" in proc
        assert "max_rate" in proc
        assert "avg_rate" in proc
        assert "payer_count" in proc
        if proc["rates"]:
            rate = proc["rates"][0]
            assert "payer_id" in rate
            assert "payer_name" in rate
            assert "negotiated_rate" in rate


@pytest.mark.asyncio
async def test_provider_procedures_not_found(client):
    """Non-existent provider returns 404."""
    res = await client.get("/v1/providers/99999/procedures")
    assert res.status_code == 404


@pytest.mark.asyncio
async def test_provider_procedures_pagination(client):
    """Pagination params work correctly."""
    res = await client.get("/v1/providers/1/procedures?limit=2&offset=0")
    assert res.status_code == 200
    data = res.json()
    assert len(data["procedures"]) <= 2
    assert data["limit"] == 2
    assert data["offset"] == 0


@pytest.mark.asyncio
async def test_provider_procedures_empty(client):
    """Provider with no rates returns empty procedures list."""
    # Provider exists but may have no rates (edge case — use a provider
    # that exists; the seeded data gives all providers rates, so we test
    # pagination past the end instead)
    res = await client.get("/v1/providers/1/procedures?limit=1&offset=9999")
    assert res.status_code == 200
    data = res.json()
    assert data["procedures"] == []
