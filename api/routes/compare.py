"""Compare endpoint — prices for procedures across Iowa facilities."""

from fastapi import APIRouter, Response

router = APIRouter(prefix="/v1", tags=["compare"])


@router.get("/compare")
async def compare_prices():
    """Compare prices across providers for given procedures.

    Not yet implemented — coming in Phase 1.
    """
    return Response(status_code=501, content="Not implemented")
