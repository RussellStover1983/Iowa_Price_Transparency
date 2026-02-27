"""Tests for GET /v1/admin/stats."""

import pytest


@pytest.mark.asyncio
async def test_admin_stats_returns_counts(client):
    """Stats endpoint returns all count fields."""
    res = await client.get("/v1/admin/stats")
    assert res.status_code == 200
    data = res.json()
    assert "total_providers" in data
    assert "total_payers" in data
    assert "total_procedures" in data
    assert "total_rates" in data
    assert "db_size_bytes" in data
    assert data["total_providers"] > 0
    assert data["total_payers"] > 0
    assert data["total_rates"] > 0


@pytest.mark.asyncio
async def test_admin_stats_empty_db(cpt_client):
    """Stats on a DB with no rates returns zeros for rates."""
    res = await cpt_client.get("/v1/admin/stats")
    assert res.status_code == 200
    data = res.json()
    assert data["total_rates"] == 0
    assert data["total_procedures"] == 0


@pytest.mark.asyncio
async def test_admin_stats_has_last_updated(client):
    """Stats includes last_updated when rates exist."""
    res = await client.get("/v1/admin/stats")
    data = res.json()
    assert data["last_updated"] is not None
