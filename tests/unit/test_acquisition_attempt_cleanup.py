from __future__ import annotations

import asyncio
import hashlib
import os
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.models.acquisition_attempt import (
    AcquisitionAttempt,
    ArtifactState,
    AttemptOutcome,
    CleanupState,
    ProviderTransferState,
    RetentionDisposition,
)
from app.models.job import Job
from app.services.acquisition_cleanup import (
    AttemptCleanupResult,
    cleanup_attempt_file,
    cleanup_attempt_partial,
    cleanup_attempt_provider,
    cleanup_durable_slskd_transfers,
    reconcile_terminal_slskd_intents,
)
from app.sources.base import CapabilityState

UUID = "2d93899b-cf9a-4567-8f10-993610f274cf"
OTHER_UUID = "06fdfa12-6d4a-4f9e-aa13-bc35685fef65"


class FakeAdapter:
    def __init__(self, snapshots: list[list[dict[str, object]]], *, fail_delete: bool = False):
        self.snapshots = list(snapshots)
        self.fail_delete = fail_delete
        self.calls: list[tuple[object, ...]] = []

    async def downloads(self, *, force_refresh: bool = False) -> list[dict[str, object]]:
        self.calls.append(("get", force_refresh))
        return self.snapshots.pop(0) if self.snapshots else []

    async def remove_exact(self, username: str, provider_uuid: str) -> None:
        self.calls.append(("delete", username, provider_uuid))
        if self.fail_delete:
            raise RuntimeError("provider secret response")


async def _attempt(
    db: AsyncSession, *, provider_uuid: str | None = UUID, staged_path: Path | None = None
) -> AcquisitionAttempt:
    job = Job(source="slskd", query="Artist Song")
    attempt = AcquisitionAttempt(
        job=job,
        provider="slskd",
        peer="peer",
        remote_path="Album/01 Song.flac",
        provider_uuid=provider_uuid,
        provider_state=(
            ProviderTransferState.completed
            if staged_path is not None
            else ProviderTransferState.failed
        ),
        outcome=AttemptOutcome.downloaded if staged_path is not None else AttemptOutcome.failed,
        provider_terminal_at=datetime.now(UTC),
        terminal_at=datetime.now(UTC),
    )
    if staged_path is not None:
        st = await asyncio.to_thread(staged_path.stat, follow_symlinks=False)
        attempt.staged_path = str(staged_path)
        attempt.artifact_state = ArtifactState.staged
        attempt.artifact_device = st.st_dev
        attempt.artifact_inode = st.st_ino
        attempt.artifact_mtime_ns = st.st_mtime_ns
        attempt.artifact_size = st.st_size
        attempt.artifact_sha256 = hashlib.sha256(
            await asyncio.to_thread(staged_path.read_bytes)
        ).hexdigest()
        attempt.file_cleanup_eligible = True
        attempt.retention_disposition = RetentionDisposition.cleanup_eligible
        attempt.provider_cleanup_state = CleanupState.completed
    db.add_all([job, attempt])
    await db.commit()
    return attempt


def _factory(db: AsyncSession) -> async_sessionmaker[AsyncSession]:
    assert db.bind is not None
    return async_sessionmaker(db.bind, expire_on_commit=False)


async def test_exact_uuid_removed_after_fresh_match_and_fresh_absence(
    db_session: AsyncSession,
) -> None:
    attempt = await _attempt(db_session)
    adapter = FakeAdapter(
        [[{"id": UUID, "username": "peer", "filename": r"Album\01 Song.flac"}], []]
    )
    result = await cleanup_attempt_provider(_factory(db_session), adapter, attempt.id)
    assert result is AttemptCleanupResult.removed
    assert adapter.calls == [("get", True), ("delete", "peer", UUID), ("get", True)]
    async with _factory(db_session)() as db:
        current = await db.get(AcquisitionAttempt, attempt.id)
        assert current is not None
        assert current.provider_cleanup_state is CleanupState.completed
        assert current.cleanup_claim_token is None


async def test_same_peer_path_replacement_uuid_is_preserved(db_session: AsyncSession) -> None:
    attempt = await _attempt(db_session)
    adapter = FakeAdapter(
        [[{"id": OTHER_UUID, "username": "peer", "filename": "Album/01 Song.flac"}]]
    )
    result = await cleanup_attempt_provider(_factory(db_session), adapter, attempt.id)
    assert result is AttemptCleanupResult.already_absent
    assert all(call[0] != "delete" for call in adapter.calls)


async def test_missing_uuid_is_blocked_without_provider_delete(db_session: AsyncSession) -> None:
    attempt = await _attempt(db_session, provider_uuid=None)
    adapter = FakeAdapter([])
    result = await cleanup_attempt_provider(_factory(db_session), adapter, attempt.id)
    assert result is AttemptCleanupResult.blocked
    assert adapter.calls == []
    await db_session.refresh(attempt)
    assert attempt.provider_cleanup_state is CleanupState.blocked
    assert attempt.error_code == "cleanup_missing_provider_uuid"


async def test_completed_transfer_without_durable_artifact_binding_is_not_cleanup_eligible(
    db_session: AsyncSession,
) -> None:
    attempt = await _attempt(db_session)
    attempt.provider_state = ProviderTransferState.completed
    attempt.outcome = AttemptOutcome.downloaded
    attempt.artifact_state = ArtifactState.none
    attempt.staged_path = None
    attempt.artifact_sha256 = None
    await db_session.commit()
    adapter = FakeAdapter([[{"id": UUID, "username": "peer"}]])

    assert (
        await cleanup_attempt_provider(_factory(db_session), adapter, attempt.id)
        is AttemptCleanupResult.not_eligible
    )
    assert await cleanup_durable_slskd_transfers(_factory(db_session), adapter) == 0
    assert adapter.calls == []


@pytest.mark.parametrize("response_was_checkpointed", [True, False])
async def test_terminal_job_reconciles_post_enqueue_cancellation_to_exact_uuid_cleanup(
    db_session: AsyncSession, response_was_checkpointed: bool
) -> None:
    job = Job(source="slskd", query="Artist Song", status="cancelled")
    attempt = AcquisitionAttempt(
        job=job,
        provider="slskd",
        peer="peer",
        remote_path="Album/01 Song.flac",
        provisional_transfer_id="peer:Album/01 Song.flac",
        provider_uuid=UUID if response_was_checkpointed else None,
        provider_state=ProviderTransferState.enqueued
        if response_was_checkpointed
        else ProviderTransferState.pending,
        provider_enqueued_at=datetime.now(UTC) if response_was_checkpointed else None,
    )
    db_session.add_all([job, attempt])
    await db_session.commit()

    class ReconcileAdapter(FakeAdapter):
        async def status(
            self, transfer_id: str, *, force_refresh: bool = False
        ) -> CapabilityState:
            self.calls.append(("status", transfer_id, force_refresh))
            return CapabilityState(
                True,
                "InProgress",
                {"id": UUID, "username": "peer", "filename": "Album/01 Song.flac"},
            )

    adapter = ReconcileAdapter(
        [[{"id": UUID, "username": "peer", "filename": "Album/01 Song.flac"}], []]
    )
    assert await reconcile_terminal_slskd_intents(_factory(db_session), adapter) == 1
    await db_session.refresh(attempt)
    assert attempt.provider_uuid == UUID
    assert attempt.provider_state is ProviderTransferState.cancelled
    assert attempt.terminal_at is not None
    await db_session.rollback()

    assert await cleanup_durable_slskd_transfers(_factory(db_session), adapter) == 1
    assert ("delete", "peer", UUID) in adapter.calls
    assert all(call[0] != "delete" or call[2] == UUID for call in adapter.calls)


async def test_contradictory_uuid_identity_is_blocked(db_session: AsyncSession) -> None:
    attempt = await _attempt(db_session)
    adapter = FakeAdapter(
        [[{"id": UUID, "username": "other-peer", "filename": "Other/Song.flac"}]]
    )
    result = await cleanup_attempt_provider(_factory(db_session), adapter, attempt.id)
    assert result is AttemptCleanupResult.blocked
    assert all(call[0] != "delete" for call in adapter.calls)


async def test_provider_http_failure_is_retryable_with_backoff(db_session: AsyncSession) -> None:
    attempt = await _attempt(db_session)
    adapter = FakeAdapter(
        [[{"id": UUID, "username": "peer", "filename": "Album/01 Song.flac"}]], fail_delete=True
    )
    result = await cleanup_attempt_provider(_factory(db_session), adapter, attempt.id)
    assert result is AttemptCleanupResult.retryable_failure
    async with _factory(db_session)() as db:
        current = await db.get(AcquisitionAttempt, attempt.id)
        assert current is not None
        assert current.provider_cleanup_state is CleanupState.failed
        assert current.provider_cleanup_attempt_count == 1
        assert current.provider_cleanup_retry_at is not None
        assert current.error_code == "provider_cleanup_failed"
        assert "secret" not in (current.error_detail or "")


async def test_double_worker_claim_issues_one_delete(db_session: AsyncSession) -> None:
    attempt = await _attempt(db_session)
    entered = asyncio.Event()
    release = asyncio.Event()

    class BlockingAdapter(FakeAdapter):
        async def downloads(self, *, force_refresh: bool = False) -> list[dict[str, object]]:
            self.calls.append(("get", force_refresh))
            if len(self.calls) == 1:
                entered.set()
                await release.wait()
                return [{"id": UUID, "username": "peer", "filename": "Album/01 Song.flac"}]
            return []

    adapter = BlockingAdapter([])
    first = asyncio.create_task(
        cleanup_attempt_provider(_factory(db_session), adapter, attempt.id)
    )
    await entered.wait()
    second = await cleanup_attempt_provider(_factory(db_session), adapter, attempt.id)
    release.set()
    assert await first is AttemptCleanupResult.removed
    assert second is AttemptCleanupResult.claimed_elsewhere
    assert [call for call in adapter.calls if call[0] == "delete"] == [("delete", "peer", UUID)]


async def test_expired_lease_recovery(db_session: AsyncSession) -> None:
    attempt = await _attempt(db_session)
    attempt.provider_cleanup_state = CleanupState.claimed
    attempt.cleanup_claim_token = str(uuid4())
    attempt.cleanup_claim_version = 4
    attempt.cleanup_lease_expires_at = datetime.now(UTC) - timedelta(seconds=1)
    await db_session.commit()
    result = await cleanup_attempt_provider(_factory(db_session), FakeAdapter([[]]), attempt.id)
    assert result is AttemptCleanupResult.already_absent
    await db_session.refresh(attempt)
    assert attempt.cleanup_claim_version == 5
    assert attempt.provider_cleanup_state is CleanupState.completed


async def test_content_bound_file_cleanup_removes_exact_file_and_repeats_idempotently(
    db_session: AsyncSession, tmp_path: Path
) -> None:
    root = tmp_path / "staging"
    root.mkdir()
    staged = root / "song.flac"
    staged.write_bytes(b"owned audio")
    attempt = await _attempt(db_session, staged_path=staged)
    assert (
        await cleanup_attempt_file(_factory(db_session), attempt.id, root)
        is AttemptCleanupResult.removed
    )
    assert not staged.exists()
    assert (
        await cleanup_attempt_file(_factory(db_session), attempt.id, root)
        is AttemptCleanupResult.already_absent
    )


@pytest.mark.parametrize("mutation", ["metadata", "hash"])
async def test_file_mismatch_is_blocked_and_retained(
    db_session: AsyncSession, tmp_path: Path, mutation: str
) -> None:
    root = tmp_path / "staging"
    root.mkdir()
    staged = root / "song.flac"
    staged.write_bytes(b"owned audio")
    attempt = await _attempt(db_session, staged_path=staged)
    if mutation == "metadata":
        staged.write_bytes(b"replacement with different metadata")
    else:
        replacement = b"other audio"
        assert len(replacement) == len(b"owned audio")
        staged.write_bytes(replacement)
        st = staged.stat()
        os.utime(staged, ns=(st.st_atime_ns, attempt.artifact_mtime_ns or st.st_mtime_ns))
    result = await cleanup_attempt_file(_factory(db_session), attempt.id, root)
    assert result is AttemptCleanupResult.blocked
    assert staged.exists()


async def test_symlink_and_root_escape_are_blocked_and_retained(
    db_session: AsyncSession, tmp_path: Path
) -> None:
    root = tmp_path / "staging"
    root.mkdir()
    outside = tmp_path / "outside.flac"
    outside.write_bytes(b"owned audio")
    escaped = await _attempt(db_session, staged_path=outside)
    link = root / "link.flac"
    link.symlink_to(outside)
    symlinked = await _attempt(db_session, staged_path=outside)
    symlinked.staged_path = str(link)
    await db_session.commit()
    assert (
        await cleanup_attempt_file(_factory(db_session), escaped.id, root)
        is AttemptCleanupResult.blocked
    )
    assert (
        await cleanup_attempt_file(_factory(db_session), symlinked.id, root)
        is AttemptCleanupResult.blocked
    )
    assert outside.exists() and link.is_symlink()


async def test_retention_disposition_prevents_file_cleanup(
    db_session: AsyncSession, tmp_path: Path
) -> None:
    root = tmp_path / "staging"
    root.mkdir()
    staged = root / "review.flac"
    staged.write_bytes(b"review")
    attempt = await _attempt(db_session, staged_path=staged)
    attempt.retention_disposition = RetentionDisposition.retain_review
    await db_session.commit()
    assert (
        await cleanup_attempt_file(_factory(db_session), attempt.id, root)
        is AttemptCleanupResult.blocked
    )
    assert staged.exists()


async def _partial_attempt(
    db: AsyncSession, partial: Path, *, provider_cleanup: CleanupState = CleanupState.completed
) -> AcquisitionAttempt:
    job = Job(source="slskd", query="partial")
    current = await asyncio.to_thread(partial.stat, follow_symlinks=False)
    attempt = AcquisitionAttempt(
        job=job,
        provider="slskd",
        peer="peer",
        remote_path="Album/01 Song.flac",
        provider_uuid=UUID,
        provider_state=ProviderTransferState.failed,
        provider_cleanup_state=provider_cleanup,
        partial_path=str(partial),
        artifact_state=ArtifactState.partial,
        outcome=AttemptOutcome.failed,
        terminal_at=datetime.now(UTC) - timedelta(days=2),
        artifact_device=current.st_dev,
        artifact_inode=current.st_ino,
        artifact_mtime_ns=current.st_mtime_ns,
        artifact_size=current.st_size,
        file_cleanup_eligible=True,
        retention_disposition=RetentionDisposition.cleanup_eligible,
    )
    db.add_all([job, attempt])
    await db.commit()
    return attempt


async def test_exact_owned_partial_removed_only_after_fresh_absence_and_grace(
    db_session: AsyncSession, tmp_path: Path
) -> None:
    root = tmp_path / "incomplete"
    root.mkdir()
    partial = root / "peer" / "song.flac.part"
    partial.parent.mkdir()
    partial.write_bytes(b"partial")
    old = (datetime.now(UTC) - timedelta(days=2)).timestamp()
    os.utime(partial, (old, old))
    attempt = await _partial_attempt(db_session, partial)

    result = await cleanup_attempt_partial(
        _factory(db_session), FakeAdapter([[]]), attempt.id, root, minimum_age=timedelta(days=1)
    )

    assert result is AttemptCleanupResult.removed
    assert not partial.exists()
    assert not partial.parent.exists()
    await db_session.refresh(attempt)
    assert attempt.file_cleanup_state is CleanupState.completed


async def test_partial_cleanup_preserves_live_new_unremoved_and_escaped_artifacts(
    db_session: AsyncSession, tmp_path: Path
) -> None:
    root = tmp_path / "incomplete"
    root.mkdir()
    old_partial = root / "live.part"
    new_partial = root / "new.part"
    outside = tmp_path / "outside.part"
    for path in (old_partial, new_partial, outside):
        path.write_bytes(b"partial")
    old = (datetime.now(UTC) - timedelta(days=2)).timestamp()
    os.utime(old_partial, (old, old))
    os.utime(outside, (old, old))
    live_attempt = await _partial_attempt(db_session, old_partial)
    new_attempt = await _partial_attempt(db_session, new_partial)
    pending_attempt = await _partial_attempt(
        db_session, outside, provider_cleanup=CleanupState.pending
    )
    snapshot = [
        {
            "id": UUID,
            "username": "peer",
            "filename": "Album/01 Song.flac",
            "state": "InProgress",
            "localPath": str(old_partial),
        }
    ]

    assert (
        await cleanup_attempt_partial(
            _factory(db_session),
            FakeAdapter([snapshot]),
            live_attempt.id,
            root,
            minimum_age=timedelta(days=1),
        )
        is AttemptCleanupResult.not_eligible
    )
    assert (
        await cleanup_attempt_partial(
            _factory(db_session),
            FakeAdapter([[]]),
            new_attempt.id,
            root,
            minimum_age=timedelta(days=1),
        )
        is AttemptCleanupResult.not_eligible
    )
    assert (
        await cleanup_attempt_partial(
            _factory(db_session),
            FakeAdapter([[]]),
            pending_attempt.id,
            root,
            minimum_age=timedelta(days=1),
        )
        is AttemptCleanupResult.not_eligible
    )
    assert old_partial.exists() and new_partial.exists() and outside.exists()


async def test_partial_cleanup_blocks_symlink_and_replaced_inode(
    db_session: AsyncSession, tmp_path: Path
) -> None:
    root = tmp_path / "incomplete"
    root.mkdir()
    target = root / "target.part"
    target.write_bytes(b"partial")
    old = (datetime.now(UTC) - timedelta(days=2)).timestamp()
    os.utime(target, (old, old))
    replaced = await _partial_attempt(db_session, target)
    target.unlink()
    target.write_bytes(b"replacement")
    os.utime(target, (old, old))
    outside = tmp_path / "outside.part"
    outside.write_bytes(b"outside")
    link = root / "link.part"
    link.symlink_to(outside)
    linked = await _partial_attempt(db_session, link)

    assert (
        await cleanup_attempt_partial(
            _factory(db_session), FakeAdapter([[]]), replaced.id, root, minimum_age=timedelta(0)
        )
        is AttemptCleanupResult.blocked
    )
    assert (
        await cleanup_attempt_partial(
            _factory(db_session), FakeAdapter([[]]), linked.id, root, minimum_age=timedelta(0)
        )
        is AttemptCleanupResult.blocked
    )
    assert target.exists() and link.is_symlink() and outside.exists()
