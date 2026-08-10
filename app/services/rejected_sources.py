from __future__ import annotations

from datetime import datetime, timedelta
from enum import StrEnum


class RejectionClass(StrEnum):
    permanent = "permanent"
    temporary = "temporary"


_TEMPORARY_BASE_DELAYS: dict[str, timedelta] = {
    "timeout": timedelta(minutes=15),
    "transfer_timeout": timedelta(minutes=15),
    "provider_timeout": timedelta(minutes=15),
    "network_failure": timedelta(minutes=5),
    "download_client_outage": timedelta(minutes=15),
    "stalled_download": timedelta(hours=1),
    "rate_limit": timedelta(minutes=30),
    "rate_limited": timedelta(minutes=30),
}
_PERMANENT_REASONS = {
    "denied",
    "user_denied",
    "identity_mismatch",
    "wrong_recording",
    "corrupt_media",
    "invalid_media",
}
_MAX_COOLDOWN = timedelta(hours=24)


def normalize_rejection_reason(reason: str) -> str:
    return "_".join(reason.strip().casefold().replace("-", " ").split())


def classify_rejection_reason(reason: str) -> RejectionClass:
    normalized = normalize_rejection_reason(reason)
    if normalized in _TEMPORARY_BASE_DELAYS:
        return RejectionClass.temporary
    if normalized in _PERMANENT_REASONS:
        return RejectionClass.permanent
    # Unknown historical reasons fail closed until an operator explicitly allows them.
    return RejectionClass.permanent


def calculate_blocked_until(
    reason: str, retry_count: int, last_failure_at: datetime
) -> datetime | None:
    normalized = normalize_rejection_reason(reason)
    if classify_rejection_reason(normalized) is RejectionClass.permanent:
        return None
    base = _TEMPORARY_BASE_DELAYS[normalized]
    multiplier = 2 ** max(0, min(retry_count, 16) - 1)
    delay_seconds = min(base.total_seconds() * multiplier, _MAX_COOLDOWN.total_seconds())
    return last_failure_at + timedelta(seconds=delay_seconds)
