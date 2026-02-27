"""Admin endpoints — coverage stats, metadata, and ETL triggers."""

import logging
import os
import subprocess
from datetime import datetime, timezone

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query

import aiosqlite

from api.dependencies import get_db
from db.models import CoverageStats

router = APIRouter(prefix="/v1", tags=["admin"])

logger = logging.getLogger(__name__)
DATABASE_PATH = os.getenv("DATABASE_PATH", "./data/iowa_transparency.db")
ADMIN_TOKEN = os.getenv("ADMIN_TOKEN", "")

# Simple in-memory ETL job tracker
_etl_jobs: list[dict] = []


@router.get("/admin/stats", response_model=CoverageStats)
async def coverage_stats(
    db: aiosqlite.Connection = Depends(get_db),
):
    """Return high-level coverage statistics."""
    cursor = await db.execute(
        "SELECT "
        "  (SELECT COUNT(*) FROM providers WHERE active = 1) AS total_providers, "
        "  (SELECT COUNT(*) FROM payers WHERE active = 1) AS total_payers, "
        "  (SELECT COUNT(DISTINCT billing_code) FROM normalized_rates) AS total_procedures, "
        "  (SELECT COUNT(*) FROM normalized_rates) AS total_rates, "
        "  (SELECT MAX(created_at) FROM normalized_rates) AS last_updated"
    )
    row = await cursor.fetchone()

    db_path = os.getenv("DATABASE_PATH", DATABASE_PATH)
    db_size = 0
    if os.path.exists(db_path):
        db_size = os.path.getsize(db_path)

    return CoverageStats(
        total_providers=row[0],
        total_payers=row[1],
        total_procedures=row[2],
        total_rates=row[3],
        last_updated=row[4],
        db_size_bytes=db_size,
    )


def _verify_token(token: str) -> None:
    """Verify admin token for protected endpoints."""
    if not ADMIN_TOKEN:
        raise HTTPException(status_code=503, detail="ADMIN_TOKEN not configured")
    if token != ADMIN_TOKEN:
        raise HTTPException(status_code=403, detail="Invalid admin token")


def _run_subprocess(cmd: list[str], job: dict, timeout: int = 3600) -> None:
    """Run a subprocess, stream output to logger, update job status."""
    job["status"] = "running"
    job["started_at"] = datetime.now(timezone.utc).isoformat()
    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=timeout,
        )
        job["exit_code"] = result.returncode
        job["stdout"] = result.stdout[-2000:] if result.stdout else ""
        job["stderr"] = result.stderr[-2000:] if result.stderr else ""
        if result.returncode == 0:
            job["status"] = "completed"
            logger.info("ETL job %s completed. stdout: %s", job["task"], result.stdout[-500:])
        else:
            job["status"] = "failed"
            logger.error("ETL job %s failed (exit %d): %s", job["task"], result.returncode, result.stderr[-500:])
    except subprocess.TimeoutExpired:
        job["status"] = "timeout"
        logger.error("ETL job %s timed out after %ds", job["task"], timeout)
    except Exception as e:
        job["status"] = "error"
        job["stderr"] = str(e)
        logger.error("ETL job %s error: %s", job["task"], e)
    finally:
        job["finished_at"] = datetime.now(timezone.utc).isoformat()


@router.post("/admin/load-npis")
async def load_npis(
    token: str = Query(..., description="Admin token"),
    background_tasks: BackgroundTasks = None,
):
    """Trigger NPPES NPI loading in the background."""
    _verify_token(token)
    job = {"task": "load_iowa_npis", "status": "queued", "created_at": datetime.now(timezone.utc).isoformat()}
    _etl_jobs.append(job)
    background_tasks.add_task(_run_subprocess, ["python", "-m", "etl.load_iowa_npis"], job, 300)
    return {"status": "started", "task": "load_iowa_npis", "job_index": len(_etl_jobs) - 1}


@router.post("/admin/ingest")
async def ingest_mrf(
    token: str = Query(..., description="Admin token"),
    payer: str = Query(..., description="Payer short_name (uhc, aetna, cigna, medica)"),
    limit: int = Query(1, ge=1, le=20, description="Max MRF files to process"),
    search: str = Query(None, description="Filter files by keyword (e.g. 'iowa')"),
    background_tasks: BackgroundTasks = None,
):
    """Trigger MRF ingestion for a payer in the background."""
    _verify_token(token)
    cmd = ["python", "-m", "etl.ingest_mrf", "--payer", payer, "--limit", str(limit), "-v"]
    if search:
        cmd.extend(["--search", search])
    job = {"task": f"ingest_mrf:{payer}", "payer": payer, "limit": limit, "search": search, "status": "queued", "created_at": datetime.now(timezone.utc).isoformat()}
    _etl_jobs.append(job)
    background_tasks.add_task(_run_subprocess, cmd, job, 3600)
    return {"status": "started", "task": "ingest_mrf", "payer": payer, "limit": limit, "search": search, "job_index": len(_etl_jobs) - 1}


@router.get("/admin/jobs")
async def list_jobs(
    token: str = Query(..., description="Admin token"),
):
    """List all ETL jobs and their status."""
    _verify_token(token)
    return {"jobs": _etl_jobs}


@router.get("/admin/discover")
async def discover_mrf_files(
    token: str = Query(..., description="Admin token"),
    payer: str = Query(..., description="Payer short_name"),
    search: str = Query(None, description="Filter files by keyword in description (e.g. 'iowa')"),
    test_url: bool = Query(False, description="Test HEAD request on first discovered URL"),
    db: aiosqlite.Connection = Depends(get_db),
):
    """Discover MRF file URLs for a payer and optionally test connectivity.

    Useful for diagnosing ingestion failures (expired SAS tokens, etc.).
    """
    import httpx
    from etl.toc_adapters import get_mrf_file_list

    _verify_token(token)

    # Look up payer
    cursor = await db.execute(
        "SELECT id, name, short_name, toc_url FROM payers WHERE short_name = ?",
        (payer,),
    )
    row = await cursor.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail=f"Unknown payer: {payer}")

    payer_dict = {"id": row[0], "name": row[1], "short_name": row[2], "toc_url": row[3]}

    # Discover files
    try:
        files = await get_mrf_file_list(payer_dict)
    except Exception as e:
        return {"error": f"Discovery failed: {e}", "payer": payer_dict}

    # Optional keyword filter
    if search:
        search_lower = search.lower()
        files = [f for f in files if search_lower in f.description.lower()]

    result = {
        "payer": payer_dict["name"],
        "toc_url": payer_dict["toc_url"][:100] if payer_dict["toc_url"] else None,
        "files_found": len(files),
        "search": search,
        "files": [
            {
                "description": f.description,
                "url_hash": f.url_hash,
                "url_preview": f.url.split("?")[0][-80:],  # path only, no SAS token
            }
            for f in files[:20]  # cap at 20 for response size
        ],
    }

    # Optionally test the first URL with a HEAD request
    if test_url and files:
        try:
            timeout = httpx.Timeout(connect=15.0, read=15.0, write=10.0, pool=None)
            async with httpx.AsyncClient(follow_redirects=True, timeout=timeout) as client:
                resp = await client.head(files[0].url)
                result["url_test"] = {
                    "status_code": resp.status_code,
                    "content_type": resp.headers.get("content-type", ""),
                    "content_length": resp.headers.get("content-length", ""),
                    "url_tested": files[0].url.split("?")[0][-80:],
                }
        except Exception as e:
            result["url_test"] = {"error": str(e)}

    # Check what's already processed for this payer
    cursor = await db.execute(
        "SELECT status, COUNT(*) FROM mrf_files WHERE payer_id = ? GROUP BY status",
        (payer_dict["id"],),
    )
    status_rows = await cursor.fetchall()
    result["db_mrf_files"] = {row[0]: row[1] for row in status_rows}

    return result


@router.get("/admin/peek-mrf")
async def peek_mrf_file(
    token: str = Query(..., description="Admin token"),
    payer: str = Query(..., description="Payer short_name"),
    search: str = Query(..., description="Filter keyword to find specific file"),
):
    """Download first 4KB of a discovered MRF file to inspect its JSON structure.

    Useful for diagnosing parse errors (Schema 2.0, different structure, etc.).
    """
    import zlib
    import httpx
    from etl.toc_adapters import get_mrf_file_list

    _verify_token(token)

    # Look up payer
    db_conn = None
    try:
        db_conn = await aiosqlite.connect(os.getenv("DATABASE_PATH", DATABASE_PATH))
        db_conn.row_factory = aiosqlite.Row
        cursor = await db_conn.execute(
            "SELECT id, name, short_name, toc_url FROM payers WHERE short_name = ?",
            (payer,),
        )
        row = await cursor.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail=f"Unknown payer: {payer}")
        payer_dict = {"id": row[0], "name": row[1], "short_name": row[2], "toc_url": row[3]}
    finally:
        if db_conn:
            await db_conn.close()

    # Discover and filter files
    files = await get_mrf_file_list(payer_dict)
    search_lower = search.lower()
    files = [f for f in files if search_lower in f.description.lower()]
    if not files:
        return {"error": f"No files matching '{search}'", "total_before_filter": len(files)}

    target = files[0]

    # Download first ~64KB (compressed) and decompress
    timeout = httpx.Timeout(connect=15.0, read=15.0, write=10.0, pool=None)
    try:
        async with httpx.AsyncClient(follow_redirects=True, timeout=timeout) as client:
            async with client.stream("GET", target.url) as response:
                response.raise_for_status()
                content_type = response.headers.get("content-type", "")
                content_length = response.headers.get("content-length", "unknown")

                # Read first 512KB compressed (enough to see in_network items)
                compressed_bytes = b""
                async for chunk in response.aiter_bytes(chunk_size=65536):
                    compressed_bytes += chunk
                    if len(compressed_bytes) >= 512 * 1024:
                        break

                # Try decompressing (handle multi-member gzip)
                url_path = target.url.split("?")[0]
                if url_path.endswith(".gz"):
                    try:
                        decompressor = zlib.decompressobj(zlib.MAX_WBITS | 16)
                        decompressed = b""
                        data = compressed_bytes
                        while data:
                            decompressed += decompressor.decompress(data)
                            if decompressor.eof:
                                data = decompressor.unused_data
                                decompressor = zlib.decompressobj(zlib.MAX_WBITS | 16)
                            else:
                                break
                        decompressed += decompressor.flush()
                        preview = decompressed[:32768].decode("utf-8", errors="replace")
                    except Exception as e:
                        preview = f"Decompression error: {e}. Raw first 200 bytes: {compressed_bytes[:200]!r}"
                else:
                    preview = compressed_bytes[:32768].decode("utf-8", errors="replace")

    except Exception as e:
        return {"error": f"Download failed: {e}", "file": target.description}

    # Extract top-level keys from the JSON preview
    top_keys = []
    try:
        import json
        # Find matching bracket depth
        partial_json = preview
        if partial_json.strip().startswith("{"):
            # Count top-level keys
            depth = 0
            current_key = ""
            in_string = False
            escape_next = False
            for ch in partial_json:
                if escape_next:
                    escape_next = False
                    continue
                if ch == '\\':
                    escape_next = True
                    continue
                if ch == '"' and not escape_next:
                    in_string = not in_string
                    continue
                if not in_string:
                    if ch == '{':
                        depth += 1
                    elif ch == '}':
                        depth -= 1
                    elif ch == ':' and depth == 1:
                        # Found a top-level key
                        key = current_key.strip().strip('"')
                        if key:
                            top_keys.append(key)
                        current_key = ""
                        continue
                    elif ch == ',' and depth == 1:
                        current_key = ""
                        continue
                if depth == 1 and not in_string:
                    current_key += ch
    except Exception:
        pass

    # Find a sample negotiated_prices entry in the preview
    neg_prices_sample = ""
    np_idx = preview.find("negotiated_prices")
    if np_idx >= 0:
        # Grab 500 chars around it
        neg_prices_sample = preview[max(0, np_idx - 50):np_idx + 500]

    return {
        "file": target.description,
        "url_hash": target.url_hash,
        "content_type": content_type,
        "content_length": content_length,
        "compressed_bytes_read": len(compressed_bytes),
        "decompressed_preview_length": len(preview),
        "top_level_keys": top_keys[:20],
        "preview_first_500": preview[:500],
        "negotiated_prices_sample": neg_prices_sample,
    }


@router.post("/admin/reset-mrf-files")
async def reset_mrf_files(
    token: str = Query(..., description="Admin token"),
    payer: str = Query(None, description="Payer short_name (optional, resets all if omitted)"),
    db: aiosqlite.Connection = Depends(get_db),
):
    """Reset failed/error MRF files so they can be re-attempted.

    Deletes mrf_files rows with status 'error' or 'processing' (stuck jobs).
    Also deletes any rates linked to those files.
    """
    _verify_token(token)

    if payer:
        cursor = await db.execute(
            "SELECT id FROM payers WHERE short_name = ?", (payer,)
        )
        row = await cursor.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail=f"Unknown payer: {payer}")
        payer_id = row[0]
        # Delete rates linked to failed files
        await db.execute(
            "DELETE FROM normalized_rates WHERE mrf_file_id IN "
            "(SELECT id FROM mrf_files WHERE payer_id = ? AND status IN ('error', 'processing'))",
            (payer_id,),
        )
        cursor = await db.execute(
            "DELETE FROM mrf_files WHERE payer_id = ? AND status IN ('error', 'processing')",
            (payer_id,),
        )
    else:
        await db.execute(
            "DELETE FROM normalized_rates WHERE mrf_file_id IN "
            "(SELECT id FROM mrf_files WHERE status IN ('error', 'processing'))"
        )
        cursor = await db.execute(
            "DELETE FROM mrf_files WHERE status IN ('error', 'processing')"
        )

    deleted = cursor.rowcount
    await db.commit()
    return {"deleted_mrf_files": deleted, "payer": payer or "all"}
