from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.source_candidate_block import SourceCandidateBlock
from app.services.source_candidate_identity import (
    SourceCandidateIdentity,
    normalize_source_candidate_identity,
)


async def active_slskd_candidate_identities(
    db: AsyncSession, *, now: datetime | None = None
) -> set[SourceCandidateIdentity]:
    """Return canonical exact identities from the authoritative active-block table."""
    rows = (
        await db.execute(
            select(SourceCandidateBlock.peer, SourceCandidateBlock.filename).where(
                SourceCandidateBlock.provider == "slskd",
                or_(
                    SourceCandidateBlock.blocked_until.is_(None),
                    SourceCandidateBlock.blocked_until > (now or datetime.now(UTC)),
                ),
            )
        )
    ).all()
    return {
        identity
        for peer, filename in rows
        if (identity := normalize_source_candidate_identity("slskd", peer, filename)) is not None
    }
