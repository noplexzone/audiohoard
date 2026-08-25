from __future__ import annotations

from collections.abc import Iterable

from sqlalchemy.ext.asyncio import AsyncSession


def reject_pending_orm_changes(
    db: AsyncSession, *, allowed_entities: Iterable[object] = ()
) -> None:
    """Reject caller-owned state that a committing service would otherwise flush."""
    allowed = set(allowed_entities)
    pending = set(db.new) | set(db.dirty) | set(db.deleted)
    unrelated = pending - allowed
    unclean_allowed = pending & allowed
    if unrelated or unclean_allowed:
        names = sorted({type(entity).__name__ for entity in pending})
        detail = ", ".join(names) if names else "unknown entities"
        raise ValueError(
            "session has pending ORM changes; commit or rollback before calling "
            f"this service ({detail})"
        )
