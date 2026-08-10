from __future__ import annotations

from uuid import UUID


def canonical_provider_uuid(value: object) -> str | None:
    """Return a normalized canonical UUID, rejecting peer/path fallback identities."""
    if not isinstance(value, str):
        return None
    candidate = value.strip()
    try:
        parsed = UUID(candidate)
    except (ValueError, AttributeError):
        return None
    canonical = str(parsed)
    return canonical if candidate.casefold() == canonical else None
