"""CPT code endpoints — search and lookup."""

from fastapi import APIRouter, Response

router = APIRouter(prefix="/v1", tags=["cpt"])


@router.get("/cpt/search")
async def search_cpt():
    """Search CPT codes by keyword.

    Not yet implemented — coming in Phase 1.
    """
    return Response(status_code=501, content="Not implemented")


@router.get("/cpt/{code}")
async def get_cpt(code: str):
    """Get details for a specific CPT code.

    Not yet implemented — coming in Phase 1.
    """
    return Response(status_code=501, content="Not implemented")
