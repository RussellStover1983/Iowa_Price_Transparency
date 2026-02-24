"""Tests for CPT search and lookup endpoints."""

import pytest


@pytest.mark.asyncio
async def test_search_knee_replacement(cpt_client):
    """FTS search returns results for 'knee replacement'."""
    response = await cpt_client.get("/v1/cpt/search", params={"q": "knee replacement"})
    assert response.status_code == 200
    data = response.json()
    assert data["count"] > 0
    codes = [r["code"] for r in data["results"]]
    assert "27447" in codes


@pytest.mark.asyncio
async def test_search_colonoscopy(cpt_client):
    """Common name 'colonoscopy' finds code 45378."""
    response = await cpt_client.get("/v1/cpt/search", params={"q": "colonoscopy"})
    assert response.status_code == 200
    data = response.json()
    codes = [r["code"] for r in data["results"]]
    assert "45378" in codes


@pytest.mark.asyncio
async def test_search_no_match(cpt_client):
    """Query with no matches returns 200 with empty results."""
    response = await cpt_client.get("/v1/cpt/search", params={"q": "xyznonexistent"})
    assert response.status_code == 200
    data = response.json()
    assert data["count"] == 0
    assert data["results"] == []


@pytest.mark.asyncio
async def test_search_empty_query(cpt_client):
    """Empty query returns 422."""
    response = await cpt_client.get("/v1/cpt/search", params={"q": ""})
    assert response.status_code == 422


@pytest.mark.asyncio
async def test_search_limit(cpt_client):
    """Limit parameter restricts result count."""
    response = await cpt_client.get("/v1/cpt/search", params={"q": "surgery", "limit": 3})
    assert response.status_code == 200
    data = response.json()
    assert data["count"] <= 3


@pytest.mark.asyncio
async def test_search_common_names_is_list(cpt_client):
    """common_names should be deserialized as a list, not a JSON string."""
    response = await cpt_client.get("/v1/cpt/search", params={"q": "knee replacement"})
    assert response.status_code == 200
    data = response.json()
    assert data["count"] > 0
    first = data["results"][0]
    assert isinstance(first["common_names"], list)


@pytest.mark.asyncio
async def test_search_response_structure(cpt_client):
    """Response has expected top-level fields."""
    response = await cpt_client.get("/v1/cpt/search", params={"q": "MRI"})
    assert response.status_code == 200
    data = response.json()
    assert "query" in data
    assert "count" in data
    assert "results" in data
    assert "disambiguation_used" in data
    assert data["query"] == "MRI"


@pytest.mark.asyncio
async def test_lookup_valid_code(cpt_client):
    """Direct lookup for 27447 returns correct CPT code."""
    response = await cpt_client.get("/v1/cpt/27447")
    assert response.status_code == 200
    data = response.json()
    assert data["code"] == "27447"
    assert "knee" in data["description"].lower()
    assert isinstance(data["common_names"], list)


@pytest.mark.asyncio
async def test_lookup_not_found(cpt_client):
    """Lookup for nonexistent code returns 404."""
    response = await cpt_client.get("/v1/cpt/00000")
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_lookup_invalid_format(cpt_client):
    """Lookup with non-numeric code returns 422."""
    response = await cpt_client.get("/v1/cpt/abc")
    assert response.status_code == 422


@pytest.mark.asyncio
async def test_disambiguation_skipped_without_api_key(cpt_client, monkeypatch):
    """Disambiguation is gracefully skipped when ANTHROPIC_API_KEY is missing."""
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    response = await cpt_client.get("/v1/cpt/search", params={"q": "surgery"})
    assert response.status_code == 200
    data = response.json()
    assert data["disambiguation_used"] is False
