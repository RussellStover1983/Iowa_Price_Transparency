"""Tests for etl/toc_adapters.py — payer-specific TOC adapters."""

from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from etl.toc_adapters import (
    _aetna_resolve_url,
    _stable_hash,
    get_mrf_file_list,
)


@pytest.mark.asyncio
async def test_dispatch_returns_empty_for_no_toc_url():
    """Payer with toc_url=None returns empty list."""
    payer = {"short_name": "wellmark", "toc_url": None, "name": "Wellmark"}
    result = await get_mrf_file_list(payer)
    assert result == []


@pytest.mark.asyncio
async def test_dispatch_returns_empty_for_unknown_payer_no_url():
    """Unknown payer with no TOC URL returns empty list."""
    payer = {"short_name": "unknown_payer", "toc_url": None, "name": "Unknown"}
    result = await get_mrf_file_list(payer)
    assert result == []


@pytest.mark.asyncio
async def test_uhc_stable_hash_uses_filename():
    """UHC adapter hashes the stable filename, not the download URL."""
    filename = "2025-01-01_uhc_in-network-rates_part1.json.gz"
    url = f"https://transparency-in-coverage.uhc.com/api/v1/uhc/blobs/download?fn={filename}"

    # The hash should be based on filename, not full URL
    hash_from_filename = _stable_hash(filename)
    hash_from_url = _stable_hash(url)

    assert hash_from_filename != hash_from_url
    assert len(hash_from_filename) == 16
    # Same filename always gives same hash
    assert _stable_hash(filename) == hash_from_filename


def test_aetna_date_template_substitution():
    """Aetna URL template is resolved with correct date format."""
    template = (
        "https://mrf.healthsparq.com/aetnacvs-egress.nophi.kyruushsq.com/"
        "prd/mrf/AETNACVS_I/ALICFI/{YYYY-MM-DD}/tableOfContents/"
        "{YYYY-MM-DD}_Aetna-Life-Insurance-Company_index.json.gz"
    )
    target = datetime(2025, 7, 15)
    resolved = _aetna_resolve_url(template, target)

    # Should use 1st of month
    assert "2025-07-01" in resolved
    assert "{YYYY-MM-DD}" not in resolved
    # Should appear twice (path + filename)
    assert resolved.count("2025-07-01") == 2


def test_aetna_date_template_current_month():
    """Aetna URL template defaults to current month when no date given."""
    template = "https://example.com/{YYYY-MM-DD}/index.json.gz"
    resolved = _aetna_resolve_url(template)

    now = datetime.now()
    expected_date = now.replace(day=1).strftime("%Y-%m-%d")
    assert expected_date in resolved


@pytest.mark.asyncio
async def test_uhc_adapter_filters_in_network():
    """UHC adapter only returns in-network files, not allowed-amounts."""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = [
        {"name": "2025-01_uhc_in-network-rates_001.json.gz"},
        {"name": "2025-01_uhc_allowed-amount_001.json.gz"},
        {"name": "2025-01_uhc_in-network-rates_002.json.gz"},
    ]
    mock_response.raise_for_status = MagicMock()

    mock_client = AsyncMock()
    mock_client.get = AsyncMock(return_value=mock_response)
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)

    payer = {
        "short_name": "uhc",
        "toc_url": "https://transparency-in-coverage.uhc.com/api/v1/uhc/blobs/",
        "name": "UHC",
    }

    with patch("etl.toc_adapters.httpx.AsyncClient", return_value=mock_client):
        result = await get_mrf_file_list(payer)

    assert len(result) == 2
    for f in result:
        assert "in-network" in f.description.lower()
        assert "allowed-amount" not in f.url.lower()


@pytest.mark.asyncio
async def test_default_adapter_falls_through():
    """Unknown payer with a TOC URL uses the default parse_toc_from_url."""
    payer = {
        "short_name": "medica",
        "toc_url": "https://example.com/toc.json",
        "name": "Medica",
    }

    with patch("etl.toc_adapters.parse_toc_from_url", new_callable=AsyncMock) as mock_parse:
        from etl.toc_parser import MrfFileInfo
        mock_parse.return_value = [
            MrfFileInfo(url="https://example.com/mrf1.json", url_hash="abc123", description="test")
        ]
        result = await get_mrf_file_list(payer)

    assert len(result) == 1
    mock_parse.assert_awaited_once_with("https://example.com/toc.json")
