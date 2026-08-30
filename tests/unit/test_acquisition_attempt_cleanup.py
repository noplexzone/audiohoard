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
from app.services import acquisition_cleanup
from app.services.acquisition_cleanup import (
    AttemptCleanupResult,
    cleanup_attempt_file,
    cleanup_attempt_partial,
    cleanup_attempt_provider,
    cleanup_durable_slskd_transfers,
    reconcile_terminal_slskd_intents,
)
from app.sources.slskd import ProvisionalTransferMatch

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


async def test_completed_missing_artifact_is_retained_by_cleanup_reconciler(
    db_session: AsyncSession,
) -> None:
    job = Job(source="slskd", query="Artist Song")
    attempt = AcquisitionAttempt(
        job=job,
        provider="slskd",
        peer="peer",
        remote_path="Album/01 Song.flac",
        provisional_transfer_id="peer:Album/01 Song.flac",
        provider_uuid=UUID,
        provider_state=ProviderTransferState.completed,
        outcome=AttemptOutcome.failed,
        provider_terminal_at=datetime.now(UTC),
        terminal_at=datetime.now(UTC),
        artifact_state=ArtifactState.missing,
        error_code="artifact_missing",
        provider_cleanup_state=CleanupState.not_required,
        file_cleanup_state=CleanupState.not_required,
        file_cleanup_eligible=False,
        retention_disposition=RetentionDisposition.retain_recovery,
    )
    db_session.add_all([job, attempt])
    await db_session.commit()
    adapter = FakeAdapter([[{"id": UUID, "username": "peer", "filename": "Album/01 Song.flac"}]])

    assert await cleanup_durable_slskd_transfers(_factory(db_session), adapter) == 0
    assert adapter.calls == []
    await db_session.refresh(attempt)
    assert attempt.provider_cleanup_state is CleanupState.not_required
    assert attempt.retention_disposition is RetentionDisposition.retain_recovery


async def _terminal_intent(
    db_session: AsyncSession,
    *,
    provider_uuid: str | None = None,
    provider_state: ProviderTransferState | None = None,
    remote_path: str = "Album/01 Song.flac",
) -> AcquisitionAttempt:
    job = Job(source="slskd", query="Artist Song", status="cancelled")
    attempt = AcquisitionAttempt(
        job=job,
        provider="slskd",
        peer="peer",
        remote_path=remote_path,
        provisional_transfer_id=f"peer:{remote_path}",
        provider_uuid=provider_uuid,
        provider_state=provider_state
        or (ProviderTransferState.enqueued if provider_uuid else ProviderTransferState.pending),
        provider_enqueued_at=datetime.now(UTC) if provider_uuid else None,
    )
    db_session.add_all([job, attempt])
    await db_session.commit()
    return attempt


class ReconcileAdapter(FakeAdapter):
    def __init__(
        self,
        matches: list[dict[str, object]],
        cleanup_snapshots: list[list[dict[str, object]]] | None = None,
    ) -> None:
        super().__init__(cleanup_snapshots or [])
        self.matches = matches

    async def match_provisional_transfer(
        self, username: str, filename: str, *, force_refresh: bool = False
    ) -> ProvisionalTransferMatch:
        self.calls.append(("match", username, filename, force_refresh))
        return ProvisionalTransferMatch(
            match_count=len(self.matches),
            transfer=self.matches[0] if len(self.matches) == 1 else None,
        )


@pytest.mark.parametrize("response_was_checkpointed", [True, False])
async def test_terminal_job_reconciles_unique_enqueue_intent_to_exact_uuid_cleanup(
    db_session: AsyncSession, response_was_checkpointed: bool
) -> None:
    attempt = await _terminal_intent(
        db_session, provider_uuid=UUID if response_was_checkpointed else None
    )

    adapter = ReconcileAdapter(
        [{"id": UUID, "username": "peer", "filename": "Album/01 Song.flac"}],
        [[{"id": UUID, "username": "peer", "filename": "Album/01 Song.flac"}], []],
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


@pytest.mark.parametrize(
    "provider_state",
    [ProviderTransferState.queued, ProviderTransferState.downloading],
)
async def test_terminal_job_reconciles_exact_active_transfer_before_canonical_cleanup(
    db_session: AsyncSession, provider_state: ProviderTransferState
) -> None:
    attempt = await _terminal_intent(
        db_session,
        provider_uuid=UUID,
        provider_state=provider_state,
    )
    matching = {"id": UUID, "username": "peer", "filename": r"Album\01 Song.flac"}
    adapter = ReconcileAdapter([matching], [[matching], []])

    assert await reconcile_terminal_slskd_intents(_factory(db_session), adapter) == 1
    await db_session.refresh(attempt)
    assert attempt.provider_state is ProviderTransferState.cancelled
    assert attempt.outcome is AttemptOutcome.failed
    assert attempt.provider_terminal_at is not None
    assert attempt.terminal_at is not None
    attempt_id = attempt.id
    await db_session.rollback()

    assert await cleanup_durable_slskd_transfers(_factory(db_session), adapter) == 1
    assert [call for call in adapter.calls if call[0] == "delete"] == [("delete", "peer", UUID)]
    async with _factory(db_session)() as db:
        current = await db.get(AcquisitionAttempt, attempt_id)
        assert current is not None
        assert current.provider_cleanup_state is CleanupState.completed


@pytest.mark.parametrize(
    "matches",
    [
        [],
        [
            {"id": UUID, "username": "peer", "filename": "Album/01 Song.flac"},
            {"id": UUID, "username": "peer", "filename": "Album/01 Song.flac"},
        ],
        [{"id": UUID, "username": "other-peer", "filename": "Album/01 Song.flac"}],
        [{"id": UUID, "username": "peer", "filename": "Other/01 Song.flac"}],
        [{"id": OTHER_UUID, "username": "peer", "filename": "Album/01 Song.flac"}],
    ],
    ids=["zero", "multiple", "peer-mismatch", "path-mismatch", "replacement-uuid"],
)
async def test_terminal_exact_active_attempt_retains_unproven_cleanup_obligation(
    db_session: AsyncSession, matches: list[dict[str, object]]
) -> None:
    attempt = await _terminal_intent(
        db_session,
        provider_uuid=UUID,
        provider_state=ProviderTransferState.downloading,
    )
    adapter = ReconcileAdapter(matches)

    assert await reconcile_terminal_slskd_intents(_factory(db_session), adapter) == 0
    assert await cleanup_durable_slskd_transfers(_factory(db_session), adapter) == 0
    assert all(call[0] != "delete" for call in adapter.calls)
    await db_session.refresh(attempt)
    assert attempt.provider_state is ProviderTransferState.downloading
    assert attempt.outcome is AttemptOutcome.pending
    assert attempt.terminal_at is None
    assert attempt.provider_cleanup_state is CleanupState.pending


async def test_terminal_intent_never_adopts_or_deletes_racing_replacement(
    db_session: AsyncSession,
) -> None:
    attempt = await _terminal_intent(db_session)

    class RacingReplacementAdapter(ReconcileAdapter):
        async def match_provisional_transfer(
            self, username: str, filename: str, *, force_refresh: bool = False
        ) -> ProvisionalTransferMatch:
            evidence = await super().match_provisional_transfer(
                username, filename, force_refresh=force_refresh
            )
            async with _factory(db_session)() as db:
                current = await db.get(AcquisitionAttempt, attempt.id)
                assert current is not None
                current.provider_uuid = UUID
                await db.commit()
            return evidence

    adapter = RacingReplacementAdapter(
        [{"id": OTHER_UUID, "username": "peer", "filename": "Album/01 Song.flac"}]
    )

    assert await reconcile_terminal_slskd_intents(_factory(db_session), adapter) == 0
    await db_session.refresh(attempt)
    assert attempt.provider_uuid == UUID
    assert attempt.provider_state is ProviderTransferState.pending
    assert await cleanup_durable_slskd_transfers(_factory(db_session), adapter) == 0
    assert all(call[0] != "delete" for call in adapter.calls)


@pytest.mark.parametrize(
    "matches",
    [
        [],
        [
            {"id": UUID, "username": "peer", "filename": "Album/01 Song.flac"},
            {"id": OTHER_UUID, "username": "peer", "filename": "Album/01 Song.flac"},
        ],
        [
            {
                "id": "peer:Album/01 Song.flac",
                "username": "peer",
                "filename": "Album/01 Song.flac",
            }
        ],
    ],
    ids=["zero", "multiple", "noncanonical-peer-path-id"],
)
async def test_terminal_intent_never_adopts_or_deletes_unproven_matches(
    db_session: AsyncSession, matches: list[dict[str, object]]
) -> None:
    attempt = await _terminal_intent(db_session)
    adapter = ReconcileAdapter(matches)

    assert await reconcile_terminal_slskd_intents(_factory(db_session), adapter) == 0
    await db_session.refresh(attempt)
    assert attempt.provider_uuid is None
    assert attempt.provider_state is ProviderTransferState.pending
    assert await cleanup_durable_slskd_transfers(_factory(db_session), adapter) == 0
    assert all(call[0] != "delete" for call in adapter.calls)


async def test_terminal_intent_never_adopts_uuid_claimed_during_provider_probe(
    db_session: AsyncSession,
) -> None:
    attempt = await _terminal_intent(db_session)

    class RacingOwnerAdapter(ReconcileAdapter):
        async def match_provisional_transfer(
            self, username: str, filename: str, *, force_refresh: bool = False
        ) -> ProvisionalTransferMatch:
            evidence = await super().match_provisional_transfer(
                username, filename, force_refresh=force_refresh
            )
            async with _factory(db_session)() as db:
                db.add(
                    AcquisitionAttempt(
                        job=Job(source="slskd", query="owner"),
                        provider="slskd",
                        peer="other-peer",
                        remote_path="Other/Song.flac",
                        provider_uuid=UUID,
                        provider_state=ProviderTransferState.downloading,
                    )
                )
                await db.commit()
            return evidence

    adapter = RacingOwnerAdapter(
        [{"id": UUID, "username": "peer", "filename": "Album/01 Song.flac"}]
    )

    assert await reconcile_terminal_slskd_intents(_factory(db_session), adapter) == 0
    await db_session.refresh(attempt)
    assert attempt.provider_uuid is None
    assert attempt.provider_state is ProviderTransferState.pending
    assert await cleanup_durable_slskd_transfers(_factory(db_session), adapter) == 0
    assert all(call[0] != "delete" for call in adapter.calls)


async def test_terminal_intent_never_adopts_new_attempt_before_uuid_checkpoint(
    db_session: AsyncSession,
) -> None:
    old_attempt = await _terminal_intent(db_session)

    class AcceptedReplacementAdapter(ReconcileAdapter):
        async def match_provisional_transfer(
            self, username: str, filename: str, *, force_refresh: bool = False
        ) -> ProvisionalTransferMatch:
            evidence = await super().match_provisional_transfer(
                username, filename, force_refresh=force_refresh
            )
            async with _factory(db_session)() as db:
                db.add(
                    AcquisitionAttempt(
                        job=Job(source="slskd", query="replacement"),
                        provider="slskd",
                        peer="peer",
                        remote_path="Album/01 Song.flac",
                        provisional_transfer_id="peer:Album/01 Song.flac",
                        provider_uuid=None,
                        provider_state=ProviderTransferState.pending,
                    )
                )
                await db.commit()
            return evidence

    adapter = AcceptedReplacementAdapter(
        [{"id": UUID, "username": "peer", "filename": "Album/01 Song.flac"}],
        [[{"id": UUID, "username": "peer", "filename": "Album/01 Song.flac"}], []],
    )

    reconciled = await reconcile_terminal_slskd_intents(_factory(db_session), adapter)
    cleaned = await cleanup_durable_slskd_transfers(_factory(db_session), adapter)

    await db_session.refresh(old_attempt)
    assert reconciled == 0
    assert cleaned == 0
    assert old_attempt.provider_uuid is None
    assert old_attempt.provider_state is ProviderTransferState.pending
    assert all(call[0] != "delete" for call in adapter.calls)


@pytest.mark.parametrize("old_uses_windows_path", [True, False])
async def test_terminal_intent_retains_normalized_equivalent_unresolved_generation(
    db_session: AsyncSession,
    old_uses_windows_path: bool,
) -> None:
    windows_path = "Album" + chr(92) + "01 Song.flac"
    unix_path = "Album/01 Song.flac"
    old_path, new_path = (
        (windows_path, unix_path) if old_uses_windows_path else (unix_path, windows_path)
    )
    old_attempt = await _terminal_intent(db_session, remote_path=old_path)
    db_session.add(
        AcquisitionAttempt(
            job=Job(source="slskd", query="replacement"),
            provider="slskd",
            peer="peer",
            remote_path=new_path,
            provisional_transfer_id=f"peer:{new_path}",
            provider_uuid=None,
            provider_state=ProviderTransferState.pending,
        )
    )
    await db_session.commit()
    adapter = ReconcileAdapter(
        [{"id": UUID, "username": "peer", "filename": old_path}],
        [[{"id": UUID, "username": "peer", "filename": old_path}], []],
    )

    assert await reconcile_terminal_slskd_intents(_factory(db_session), adapter) == 0
    assert await cleanup_durable_slskd_transfers(_factory(db_session), adapter) == 0
    await db_session.refresh(old_attempt)
    assert old_attempt.provider_uuid is None
    assert old_attempt.provider_state is ProviderTransferState.pending
    assert old_attempt.terminal_at is None
    assert old_attempt.provider_cleanup_state is CleanupState.pending
    assert all(call[0] != "delete" for call in adapter.calls)


@pytest.mark.parametrize("old_uses_windows_path", [True, False])
async def test_terminal_intent_retains_normalized_replacement_during_uuid_checkpoint_race(
    db_session: AsyncSession,
    old_uses_windows_path: bool,
) -> None:
    windows_path = "Album" + chr(92) + "01 Song.flac"
    unix_path = "Album/01 Song.flac"
    old_path, new_path = (
        (windows_path, unix_path) if old_uses_windows_path else (unix_path, windows_path)
    )
    old_attempt = await _terminal_intent(db_session, remote_path=old_path)

    class SeparatorReplacementAdapter(ReconcileAdapter):
        async def match_provisional_transfer(
            self, username: str, filename: str, *, force_refresh: bool = False
        ) -> ProvisionalTransferMatch:
            evidence = await super().match_provisional_transfer(
                username, filename, force_refresh=force_refresh
            )
            async with _factory(db_session)() as db:
                db.add(
                    AcquisitionAttempt(
                        job=Job(source="slskd", query="replacement"),
                        provider="slskd",
                        peer="peer",
                        remote_path=new_path,
                        provisional_transfer_id=f"peer:{new_path}",
                        provider_uuid=None,
                        provider_state=ProviderTransferState.pending,
                    )
                )
                await db.commit()
            return evidence

    adapter = SeparatorReplacementAdapter(
        [{"id": UUID, "username": "peer", "filename": old_path}],
        [[{"id": UUID, "username": "peer", "filename": old_path}], []],
    )

    assert await reconcile_terminal_slskd_intents(_factory(db_session), adapter) == 0
    assert await cleanup_durable_slskd_transfers(_factory(db_session), adapter) == 0
    await db_session.refresh(old_attempt)
    assert old_attempt.provider_uuid is None
    assert old_attempt.provider_state is ProviderTransferState.pending
    assert old_attempt.terminal_at is None
    assert old_attempt.provider_cleanup_state is CleanupState.pending
    assert all(call[0] != "delete" for call in adapter.calls)


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


async def test_content_bound_file_cleanup_erases_content_but_retains_tombstone(
    db_session: AsyncSession, tmp_path: Path
) -> None:
    root = tmp_path / "staging"
    root.mkdir()
    staged = root / "song.flac"
    staged.write_bytes(b"owned audio")
    attempt = await _attempt(db_session, staged_path=staged)
    quarantine = acquisition_cleanup._file_quarantine_path(staged, attempt.id)

    assert (
        await cleanup_attempt_file(_factory(db_session), attempt.id, root)
        is AttemptCleanupResult.quarantined
    )
    assert not staged.exists()
    assert quarantine.is_file()
    assert quarantine.read_bytes() == b""
    await db_session.refresh(attempt)
    assert attempt.file_cleanup_state is CleanupState.completed
    assert attempt.retention_disposition is RetentionDisposition.retained
    assert attempt.artifact_state is ArtifactState.missing
    assert (
        await cleanup_attempt_file(_factory(db_session), attempt.id, root)
        is AttemptCleanupResult.already_absent
    )


async def test_content_erased_tombstone_recovers_after_claim_finalization_loss(
    db_session: AsyncSession, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "staging"
    root.mkdir()
    staged = root / "song.flac"
    staged.write_bytes(b"owned audio")
    attempt = await _attempt(db_session, staged_path=staged)
    quarantine = acquisition_cleanup._file_quarantine_path(staged, attempt.id)
    finish_file_claim = acquisition_cleanup._finish_file_claim

    async def lose_finalization(*_args: object, **_kwargs: object) -> bool:
        return False

    monkeypatch.setattr(acquisition_cleanup, "_finish_file_claim", lose_finalization)
    assert (
        await cleanup_attempt_file(_factory(db_session), attempt.id, root)
        is AttemptCleanupResult.claimed_elsewhere
    )
    assert quarantine.read_bytes() == b""

    monkeypatch.setattr(acquisition_cleanup, "_finish_file_claim", finish_file_claim)
    await db_session.refresh(attempt)
    attempt.cleanup_lease_expires_at = datetime.now(UTC) - timedelta(seconds=1)
    await db_session.commit()

    assert (
        await cleanup_attempt_file(_factory(db_session), attempt.id, root)
        is AttemptCleanupResult.quarantined
    )
    await db_session.refresh(attempt)
    assert attempt.file_cleanup_state is CleanupState.completed
    assert attempt.retention_disposition is RetentionDisposition.retained
    assert quarantine.read_bytes() == b""


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


async def test_replacement_inserted_after_final_hash_is_retained_and_blocked(
    db_session: AsyncSession, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "staging"
    root.mkdir()
    staged = root / "song.flac"
    staged.write_bytes(b"owned audio")
    attempt = await _attempt(db_session, staged_path=staged)
    quarantine = acquisition_cleanup._file_quarantine_path(staged, attempt.id)
    hook_called = False

    def replace_in_final_erase_window(parent_fd: int, name: str) -> None:
        nonlocal hook_called
        hook_called = True
        os.unlink(name, dir_fd=parent_fd)
        replacement_fd = os.open(
            name,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL,
            0o600,
            dir_fd=parent_fd,
        )
        try:
            os.write(replacement_fd, b"replacement audio")
        finally:
            os.close(replacement_fd)

    monkeypatch.setattr(
        acquisition_cleanup,
        "_before_final_file_erase",
        replace_in_final_erase_window,
    )

    result = await cleanup_attempt_file(_factory(db_session), attempt.id, root)

    assert hook_called
    assert result is AttemptCleanupResult.blocked
    assert not staged.exists()
    assert quarantine.read_bytes() == b"replacement audio"
    await db_session.refresh(attempt)
    assert attempt.file_cleanup_state is CleanupState.blocked
    assert attempt.quarantine_path == str(quarantine)


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


async def test_periodic_cleanup_retries_staged_file_after_provider_cleanup_completed(
    db_session: AsyncSession, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "complete"
    root.mkdir()
    staged = root / "song.flac"
    staged.write_bytes(b"owned audio")
    attempt = await _attempt(db_session, staged_path=staged)
    remove_claimed_file = acquisition_cleanup._remove_claimed_file
    calls = 0

    def fail_once(claim, configured_root):  # noqa: ANN001
        nonlocal calls
        calls += 1
        if calls == 1:
            return AttemptCleanupResult.retryable_failure
        return remove_claimed_file(claim, configured_root)

    monkeypatch.setattr(acquisition_cleanup, "_remove_claimed_file", fail_once)

    assert (
        await cleanup_durable_slskd_transfers(
            _factory(db_session), FakeAdapter([]), complete_root=root
        )
        == 0
    )
    assert staged.exists()
    await db_session.refresh(attempt)
    assert attempt.file_cleanup_state is CleanupState.failed
    attempt.file_cleanup_retry_at = datetime.now(UTC) - timedelta(seconds=1)
    await db_session.commit()

    assert (
        await cleanup_durable_slskd_transfers(
            _factory(db_session), FakeAdapter([]), complete_root=root
        )
        == 0
    )
    assert not staged.exists()
    await db_session.refresh(attempt)
    assert attempt.file_cleanup_state is CleanupState.completed


@pytest.mark.parametrize("deferred_by", ["age", "live", "retry"])
async def test_periodic_cleanup_retries_deferred_partial_after_provider_cleanup_completed(
    db_session: AsyncSession, tmp_path: Path, deferred_by: str
) -> None:
    root = tmp_path / "incomplete"
    root.mkdir()
    partial = root / "peer" / "song.flac.part"
    partial.parent.mkdir()
    partial.write_bytes(b"partial")
    old = (datetime.now(UTC) - timedelta(days=2)).timestamp()
    os.utime(partial, (old, old))
    attempt = await _partial_attempt(db_session, partial)
    live_snapshot = [
        {
            "id": UUID,
            "username": "peer",
            "filename": "Album/01 Song.flac",
            "state": "InProgress",
            "localPath": str(partial),
        }
    ]

    class RetryAdapter(FakeAdapter):
        async def downloads(self, *, force_refresh: bool = False) -> list[dict[str, object]]:
            self.calls.append(("get", force_refresh))
            if len(self.calls) == 1:
                raise RuntimeError("temporary snapshot failure")
            return []

    adapter = (
        FakeAdapter([live_snapshot, []])
        if deferred_by == "live"
        else RetryAdapter([])
        if deferred_by == "retry"
        else FakeAdapter([[], []])
    )

    first_minimum_age = timedelta(days=3) if deferred_by == "age" else timedelta(0)
    assert (
        await cleanup_durable_slskd_transfers(
            _factory(db_session),
            adapter,
            incomplete_root=root,
            partial_minimum_age=first_minimum_age,
        )
        == 0
    )
    assert partial.exists()
    await db_session.refresh(attempt)
    if deferred_by == "retry":
        assert attempt.file_cleanup_state is CleanupState.failed
        attempt.file_cleanup_retry_at = datetime.now(UTC) - timedelta(seconds=1)
        await db_session.commit()
    else:
        assert attempt.file_cleanup_state is CleanupState.pending

    assert (
        await cleanup_durable_slskd_transfers(
            _factory(db_session),
            adapter,
            incomplete_root=root,
            partial_minimum_age=timedelta(0),
        )
        == 0
    )
    assert not partial.exists()
    await db_session.refresh(attempt)
    assert attempt.file_cleanup_state is CleanupState.completed
