from __future__ import annotations

import logging
from typing import Annotated

from fastapi import APIRouter, Depends, Request, Response
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth import require_admin_read
from app.database import get_db
from app.models.auth import AppUser
from app.schemas.health import HealthResponse, SourceStatus
from app.services.health_status import get_health_status_service

router = APIRouter()
logger = logging.getLogger(__name__)


async def _check_db(db: AsyncSession) -> bool:
    try:
        await db.execute(text("SELECT 1"))
        return True
    except Exception:
        logger.warning("database readiness check failed", exc_info=True)
        return False


def _readiness_response(db_ready: bool) -> HealthResponse:
    return HealthResponse(
        status="ok" if db_ready else "down",
        sources={},
        db_writable=db_ready,
    )


@router.get("/health/live", response_model=dict[str, str])
async def liveness() -> dict[str, str]:
    """Process liveness only; never performs network or database probes."""
    return {"status": "ok"}


@router.get("/health/ready", response_model=HealthResponse)
async def readiness(
    response: Response,
    db: Annotated[AsyncSession, Depends(get_db)],
) -> HealthResponse:
    """Cheap readiness check. A failed database check returns HTTP 503."""
    db_ready = await _check_db(db)
    if not db_ready:
        response.status_code = 503
    return _readiness_response(db_ready)


@router.get("/health", response_model=HealthResponse)
async def health(
    response: Response,
    db: Annotated[AsyncSession, Depends(get_db)],
) -> HealthResponse:
    """Backward-compatible readiness endpoint without live provider probes."""
    return await readiness(response, db)


@router.get("/health/sources", response_model=dict[str, SourceStatus])
async def health_sources(
    request: Request,
    _admin: Annotated[AppUser, Depends(require_admin_read)],
) -> dict[str, SourceStatus]:
    """Authenticated cached provider diagnostics; refresh runs in the background."""
    service = getattr(request.app.state, "health_status_service", get_health_status_service())
    return {name: cached.status for name, cached in service.snapshot().items()}
