"""CPT code endpoints — search and lookup."""

import json
import re

from fastapi import APIRouter, Depends, HTTPException, Query

import aiosqlite

from api.dependencies import get_db
from db.models import CptCode, CptSearchResponse, CptSearchResult
from services.cpt_disambiguation import disambiguate_cpt_results

router = APIRouter(prefix="/v1", tags=["cpt"])


@router.get("/cpt/search", response_model=CptSearchResponse)
async def search_cpt(
    q: str = Query(..., min_length=1, description="Search query for CPT codes"),
    limit: int = Query(20, ge=1, le=100, description="Max results to return"),
    db: aiosqlite.Connection = Depends(get_db),
):
    """Search CPT codes by keyword using FTS5."""
    # Sanitize: extract word tokens for FTS5 query
    tokens = re.findall(r"\w+", q)
    if not tokens:
        raise HTTPException(status_code=422, detail="Query contains no searchable terms")
    fts_query = " ".join(tokens)

    cursor = await db.execute(
        "SELECT cl.code, cl.description, cl.category, cl.common_names, f.rank "
        "FROM cpt_fts f "
        "JOIN cpt_lookup cl ON f.rowid = cl.rowid "
        "WHERE cpt_fts MATCH ? "
        "ORDER BY f.rank "
        "LIMIT ?",
        (fts_query, limit),
    )
    rows = await cursor.fetchall()

    results = []
    for row in rows:
        code, description, category, common_names_raw, rank = (
            row[0], row[1], row[2], row[3], row[4],
        )
        # Deserialize common_names from JSON string
        if common_names_raw:
            try:
                common_names = json.loads(common_names_raw)
            except (json.JSONDecodeError, TypeError):
                common_names = []
        else:
            common_names = []

        results.append({
            "code": code,
            "description": description,
            "category": category,
            "common_names": common_names,
            "rank": rank,
        })

    # Optionally disambiguate with Haiku if 5+ results
    disambiguation_used = False
    if len(results) >= 5:
        results, disambiguation_used = await disambiguate_cpt_results(
            q, results, max_results=limit
        )

    return CptSearchResponse(
        query=q,
        count=len(results),
        results=[CptSearchResult(**r) for r in results],
        disambiguation_used=disambiguation_used,
    )


@router.get("/cpt/{code}", response_model=CptCode)
async def get_cpt(
    code: str,
    db: aiosqlite.Connection = Depends(get_db),
):
    """Get details for a specific CPT code."""
    if not re.match(r"^\d{4,5}$", code):
        raise HTTPException(status_code=422, detail="CPT code must be 4-5 digits")

    cursor = await db.execute(
        "SELECT code, description, category, common_names FROM cpt_lookup WHERE code = ?",
        (code,),
    )
    row = await cursor.fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail=f"CPT code {code} not found")

    common_names_raw = row[3]
    if common_names_raw:
        try:
            common_names = json.loads(common_names_raw)
        except (json.JSONDecodeError, TypeError):
            common_names = None
    else:
        common_names = None

    return CptCode(
        code=row[0],
        description=row[1],
        category=row[2],
        common_names=common_names,
    )
