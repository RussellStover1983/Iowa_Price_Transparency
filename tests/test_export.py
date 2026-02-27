"""Tests for GET /v1/export (CSV export)."""

import csv
import io

import pytest


@pytest.mark.asyncio
async def test_export_csv_returns_csv(client):
    """Export returns CSV content with correct headers."""
    res = await client.get("/v1/export", params={"codes": "27447"})
    assert res.status_code == 200
    assert "text/csv" in res.headers["content-type"]
    assert "attachment" in res.headers["content-disposition"]
    assert ".csv" in res.headers["content-disposition"]


@pytest.mark.asyncio
async def test_export_csv_has_header_row(client):
    """CSV has the expected column headers."""
    res = await client.get("/v1/export", params={"codes": "27447"})
    reader = csv.reader(io.StringIO(res.text))
    header = next(reader)
    assert "CPT Code" in header
    assert "Provider" in header
    assert "Negotiated Rate" in header
    assert "Payer" in header


@pytest.mark.asyncio
async def test_export_csv_has_data_rows(client):
    """CSV contains at least one data row for a known code."""
    res = await client.get("/v1/export", params={"codes": "27447"})
    reader = csv.reader(io.StringIO(res.text))
    rows = list(reader)
    assert len(rows) > 1  # header + at least one data row
    assert rows[1][0] == "27447"


@pytest.mark.asyncio
async def test_export_csv_multiple_codes(client):
    """CSV contains data for multiple codes."""
    res = await client.get("/v1/export", params={"codes": "27447,45378"})
    reader = csv.reader(io.StringIO(res.text))
    rows = list(reader)
    codes_in_csv = set(row[0] for row in rows[1:])
    assert "27447" in codes_in_csv
    assert "45378" in codes_in_csv


@pytest.mark.asyncio
async def test_export_invalid_format(client):
    """Non-csv format returns 400."""
    res = await client.get("/v1/export", params={"codes": "27447", "format": "json"})
    assert res.status_code == 400
