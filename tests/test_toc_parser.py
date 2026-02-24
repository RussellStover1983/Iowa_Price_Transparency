"""Tests for etl/toc_parser.py — TOC JSON parser."""

import pathlib

import pytest

from etl.toc_parser import (
    MrfFileInfo,
    _is_in_network_file,
    compute_url_hash,
    parse_toc_from_bytes,
)


async def _bytes_from_file(path: pathlib.Path):
    data = path.read_bytes()
    yield data


@pytest.mark.asyncio
async def test_extracts_in_network_urls(sample_toc_path):
    """Should extract the in-network URL and exclude the allowed-amounts URL."""
    files = await parse_toc_from_bytes(_bytes_from_file(sample_toc_path))
    assert len(files) == 1
    assert "in-network" in files[0].url
    assert "allowed-amount" not in files[0].url


def test_is_in_network_file_filter():
    """Keyword filter correctly classifies URLs."""
    assert _is_in_network_file("https://example.com/in-network-rates.json.gz") is True
    assert _is_in_network_file("https://example.com/negotiated-rates.json") is True
    assert _is_in_network_file("https://example.com/allowed-amounts.json.gz") is False
    assert _is_in_network_file(
        "https://example.com/rates.json", "Allowed Amounts File"
    ) is False


def test_compute_url_hash_deterministic():
    """Same URL always produces the same hash."""
    url = "https://example.com/mrf/2024-07_in-network-rates.json.gz"
    h1 = compute_url_hash(url)
    h2 = compute_url_hash(url)
    assert h1 == h2
    assert len(h1) == 16
    # Different URL gives different hash
    h3 = compute_url_hash("https://example.com/other-file.json")
    assert h3 != h1
