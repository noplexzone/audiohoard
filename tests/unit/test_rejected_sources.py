from __future__ import annotations

from datetime import UTC, datetime, timedelta

from app.services.rejected_sources import (
    RejectionClass,
    calculate_blocked_until,
    classify_rejection_reason,
)


def test_user_denial_and_identity_mismatch_are_permanent() -> None:
    assert classify_rejection_reason("denied") is RejectionClass.permanent
    assert classify_rejection_reason("identity_mismatch") is RejectionClass.permanent
    assert calculate_blocked_until("denied", 1, datetime.now(UTC)) is None


def test_transient_failures_receive_bounded_exponential_cooldowns() -> None:
    failed_at = datetime(2026, 8, 10, 12, tzinfo=UTC)

    assert classify_rejection_reason("transfer_timeout") is RejectionClass.temporary
    assert calculate_blocked_until("transfer_timeout", 1, failed_at) == failed_at + timedelta(
        minutes=15
    )
    assert calculate_blocked_until("rate_limited", 3, failed_at) == failed_at + timedelta(hours=2)
    assert calculate_blocked_until("network_failure", 99, failed_at) <= failed_at + timedelta(
        hours=24
    )


def test_unknown_reasons_fail_safe_as_permanent() -> None:
    assert classify_rejection_reason("unrecognized") is RejectionClass.permanent
    assert calculate_blocked_until("unrecognized", 1, datetime.now(UTC)) is None
