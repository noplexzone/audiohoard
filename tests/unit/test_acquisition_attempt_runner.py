from __future__ import annotations

import asyncio
from pathlib import Path

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import Settings
from app.jobs import runner
from app.models.acquisition_attempt import (
    AcquisitionAttempt,
    ArtifactState,
    AttemptOutcome,
    ProviderTransferState,
)
from app.models.job import Job
from app.models.workflow import AcquisitionState
from app.schemas.search import SearchResult
from app.sources.base import CapabilityState
from app.sources.youtube import ProviderError


async def test_runner_keeps_sequential_candidate_attempts_distinct(
    db_session: AsyncSession,
    test_settings: Settings,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    job = Job(source="slskd", query="Artist Song")
    db_session.add(job)
    await db_session.flush()
    results = [
        SearchResult(
            source="slskd",
            title="Song",
            artist="Artist",
            metadata={"username": "peer-a", "filename": r"Album\01 Song.flac"},
        ),
        SearchResult(
            source="slskd",
            title="Song",
            artist="Artist",
            metadata={"username": "peer-b", "filename": r"Other\01 Song.flac"},
        ),
    ]

    async def fake_fetch(*args: object, **kwargs: object) -> list[SearchResult]:
        return results

    calls = 0

    async def fake_prepare(
        result: SearchResult,
        source: str,
        cfg: Settings,
        track: runner.Track | None = None,
        *,
        checkpoint: object | None = None,
        attempt: AcquisitionAttempt | None = None,
    ) -> tuple[str | None, str | None]:
        nonlocal calls
        calls += 1
        assert attempt is not None
        if calls == 1:
            raise ProviderError("transfer_failed", "first candidate failed", "acquire", True)
        assert track is not None
        track.acquisition_state = AcquisitionState.downloaded
        attempt.outcome = AttemptOutcome.downloaded
        attempt.provider_state = ProviderTransferState.completed
        return "second", "downloaded"

    async def noop(*args: object, **kwargs: object) -> None:
        return None

    monkeypatch.setattr(runner, "_call_fetch_results", fake_fetch)
    monkeypatch.setattr(runner, "_prepare_acquisition", fake_prepare)
    monkeypatch.setattr(runner, "_enrich_musicbrainz", noop)
    monkeypatch.setattr(runner, "_enrich_deezer", noop)
    monkeypatch.setattr(runner, "_run_fingerprint_and_verify", noop)
    monkeypatch.setattr(runner, "_compute_path_preview", noop)
    monkeypatch.setattr(runner, "_try_auto_import", noop)

    await runner._run_job_in_session(job.id, db_session, test_settings)

    attempts = list(
        (
            await db_session.scalars(select(AcquisitionAttempt).order_by(AcquisitionAttempt.id))
        ).all()
    )
    assert [(a.peer, a.remote_path) for a in attempts] == [
        ("peer-a", "Album/01 Song.flac"),
        ("peer-b", "Other/01 Song.flac"),
    ]
    assert [a.outcome for a in attempts] == [AttemptOutcome.failed, AttemptOutcome.downloaded]
    assert attempts[0].id != attempts[1].id


async def test_enqueue_without_uuid_keeps_fallback_provisional_and_checkpoints_uuid(
    test_settings: Settings,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    staged = test_settings.staging_root / "song.flac"
    staged.parent.mkdir(parents=True)
    staged.write_bytes(b"audio-content")
    canonical = "2d93899b-cf9a-4567-8f10-993610f274cf"
    fallback = "peer:music/song.flac"
    attempt = AcquisitionAttempt(
        job_id=1, provider="slskd", peer="peer", remote_path="music/song.flac"
    )
    snapshots: list[tuple[str | None, str | None]] = []

    class FakeSlskd:
        def __init__(self, url: str, key: str) -> None:
            pass

        async def status(
            self, transfer_id: str, *, force_refresh: bool = False
        ) -> CapabilityState:
            assert transfer_id == fallback
            assert force_refresh
            return CapabilityState(False, "transfer not found")

        async def enqueue(self, username: str, filename: str, size: int | None = None) -> str:
            return fallback

    async def fake_poll(
        transfer_id: str,
        username: str,
        filename: str,
        adapter: object,
        cfg: Settings,
        on_provider_id: object,
        on_provider_state: object,
    ) -> tuple[Path, str]:
        assert transfer_id == fallback
        await on_provider_id(canonical)  # type: ignore[operator]
        await on_provider_state(AcquisitionState.downloaded)  # type: ignore[operator]
        return staged, canonical

    async def checkpoint() -> None:
        snapshots.append((attempt.provisional_transfer_id, attempt.provider_uuid))

    monkeypatch.setattr(runner, "SlskdAdapter", FakeSlskd)
    monkeypatch.setattr(runner, "_call_poll_slskd_transfer", fake_poll)
    result = SearchResult(
        source="slskd",
        title="Song",
        metadata={"username": "peer", "filename": "music/song.flac"},
    )

    transfer_id, status = await runner._prepare_acquisition(
        result, "slskd", test_settings, attempt=attempt, checkpoint=checkpoint
    )

    assert snapshots[0] == (fallback, None)
    assert transfer_id == canonical
    assert status == "downloaded"
    assert attempt.provisional_transfer_id == fallback
    assert attempt.provider_uuid == canonical
    assert attempt.provider_state == ProviderTransferState.completed
    assert attempt.artifact_state == ArtifactState.staged
    assert attempt.outcome == AttemptOutcome.downloaded
    assert attempt.staged_path == str(staged)
    assert attempt.artifact_sha256 is not None
    assert attempt.artifact_mtime_ns is not None


async def test_candidate_attempt_adopts_only_active_transfer(db_session: AsyncSession) -> None:
    job = Job(source="slskd", query="Artist Song")
    active = AcquisitionAttempt(
        job=job,
        provider="slskd",
        peer="peer",
        remote_path="Album/01 Song.flac",
        outcome=AttemptOutcome.selected,
        provider_state=ProviderTransferState.downloading,
        provisional_transfer_id="peer:Album/01 Song.flac",
    )
    db_session.add_all([job, active])
    await db_session.flush()
    result = SearchResult(
        source="slskd",
        title="Song",
        metadata={"username": "peer", "filename": r"Album\01 Song.flac"},
    )

    adopted = await runner._candidate_attempt(db_session, job, result)

    assert adopted.id == active.id
    assert adopted.provisional_transfer_id == "peer:Album/01 Song.flac"


@pytest.mark.parametrize(
    "provider_state",
    [
        ProviderTransferState.failed,
        ProviderTransferState.cancelled,
        ProviderTransferState.completed,
    ],
)
async def test_candidate_attempt_retry_after_terminal_transfer_creates_new_row(
    db_session: AsyncSession, provider_state: ProviderTransferState
) -> None:
    job = Job(source="slskd", query="Artist Song")
    terminal = AcquisitionAttempt(
        job=job,
        provider="slskd",
        peer="peer",
        remote_path="Album/01 Song.flac",
        outcome=AttemptOutcome.failed,
        provider_state=provider_state,
        provisional_transfer_id="old-transfer",
        terminal_at=runner._now(),
    )
    db_session.add_all([job, terminal])
    await db_session.flush()
    result = SearchResult(
        source="slskd",
        title="Song",
        metadata={"username": "peer", "filename": r"Album\01 Song.flac"},
    )

    retried = await runner._candidate_attempt(db_session, job, result)

    assert retried.id != terminal.id
    assert retried.provider_state == ProviderTransferState.pending
    assert retried.provisional_transfer_id is None


async def test_restart_adopts_canonical_uuid_without_enqueue(
    test_settings: Settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    canonical = "2d93899b-cf9a-4567-8f10-993610f274cf"
    staged = test_settings.staging_root / "restart.flac"
    staged.parent.mkdir(parents=True, exist_ok=True)
    staged.write_bytes(b"audio")
    attempt = AcquisitionAttempt(
        job_id=1,
        provider="slskd",
        peer="peer",
        remote_path="Album/01 Song.flac",
        provider_uuid=canonical,
        provider_state=ProviderTransferState.downloading,
    )

    class FakeSlskd:
        def __init__(self, url: str, key: str) -> None:
            pass

        async def enqueue(self, *args: object, **kwargs: object) -> str:
            raise AssertionError("active canonical transfer must be adopted")

    async def fake_poll(transfer_id: str, *args: object, **kwargs: object) -> tuple[Path, str]:
        assert transfer_id == canonical
        return staged, canonical

    monkeypatch.setattr(runner, "SlskdAdapter", FakeSlskd)
    monkeypatch.setattr(runner, "_call_poll_slskd_transfer", fake_poll)
    result = SearchResult(
        source="slskd",
        title="Song",
        metadata={"username": "peer", "filename": "Album/01 Song.flac"},
    )

    transfer_id, _ = await runner._prepare_acquisition(
        result, "slskd", test_settings, attempt=attempt
    )

    assert transfer_id == canonical
    assert attempt.provider_state == ProviderTransferState.completed


async def test_restart_probes_fallback_and_does_not_duplicate_accepted_enqueue(
    test_settings: Settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    fallback = "peer:Album/01 Song.flac"
    canonical = "2d93899b-cf9a-4567-8f10-993610f274cf"
    staged = test_settings.staging_root / "recovered.flac"
    staged.parent.mkdir(parents=True, exist_ok=True)
    staged.write_bytes(b"audio")
    queue: dict[str, object] = {}
    enqueue_calls = 0
    attempt = AcquisitionAttempt(
        job_id=1, provider="slskd", peer="peer", remote_path="Album/01 Song.flac"
    )

    class FakeSlskd:
        def __init__(self, url: str, key: str) -> None:
            pass

        async def status(self, transfer_id: str, *, force_refresh: bool = False):
            assert transfer_id == fallback
            assert force_refresh
            if queue:
                return CapabilityState(True, "InProgress", dict(queue))
            return CapabilityState(False, "transfer not found")

        async def enqueue(self, username: str, filename: str, size: int | None = None) -> str:
            nonlocal enqueue_calls
            enqueue_calls += 1
            queue.update({"id": canonical, "username": username, "filename": filename})
            raise asyncio.CancelledError

    async def fake_poll(transfer_id: str, *args: object, **kwargs: object) -> tuple[Path, str]:
        assert transfer_id in {fallback, canonical}
        return staged, canonical

    async def checkpoint() -> None:
        return None

    monkeypatch.setattr(runner, "SlskdAdapter", FakeSlskd)
    monkeypatch.setattr(runner, "_call_poll_slskd_transfer", fake_poll)
    result = SearchResult(
        source="slskd",
        title="Song",
        metadata={"username": "peer", "filename": "Album/01 Song.flac"},
    )

    with pytest.raises(asyncio.CancelledError):
        await runner._prepare_acquisition(
            result, "slskd", test_settings, attempt=attempt, checkpoint=checkpoint
        )
    assert attempt.provisional_transfer_id == fallback

    transfer_id, _ = await runner._prepare_acquisition(
        result, "slskd", test_settings, attempt=attempt, checkpoint=checkpoint
    )

    assert enqueue_calls == 1
    assert transfer_id == canonical
    assert attempt.provider_uuid == canonical


async def test_cancellation_checkpoints_attempt_before_provider_cancel(
    test_settings: Settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    fallback = "peer:Album/01 Song.flac"
    attempt = AcquisitionAttempt(
        job_id=1,
        provider="slskd",
        peer="peer",
        remote_path="Album/01 Song.flac",
        provisional_transfer_id=fallback,
        provider_state=ProviderTransferState.downloading,
        provider_enqueued_at=runner._now(),
        outcome=AttemptOutcome.selected,
    )
    polled = asyncio.Event()
    release = asyncio.Event()
    snapshots: list[tuple[AttemptOutcome, ProviderTransferState, object, object, str | None]] = []

    class FakeSlskd:
        def __init__(self, url: str, key: str) -> None:
            pass

        async def status(self, transfer_id: str):
            polled.set()
            await release.wait()
            return CapabilityState(True, "InProgress", {"id": transfer_id})

        async def cancel(
            self, username: str, filename: str, transfer_id: str | None = None
        ) -> bool:
            assert snapshots, "attempt cancellation must be durable before provider mutation"
            assert snapshots[-1][1] == ProviderTransferState.cancelled
            return True

    async def checkpoint() -> None:
        snapshots.append(
            (
                attempt.outcome,
                attempt.provider_state,
                attempt.terminal_at,
                attempt.provider_terminal_at,
                attempt.error_code,
            )
        )

    monkeypatch.setattr(runner, "SlskdAdapter", FakeSlskd)
    result = SearchResult(
        source="slskd",
        title="Song",
        metadata={"username": "peer", "filename": "Album/01 Song.flac"},
    )
    task = asyncio.create_task(
        runner._prepare_acquisition(
            result, "slskd", test_settings, attempt=attempt, checkpoint=checkpoint
        )
    )
    await polled.wait()
    task.cancel()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert attempt.outcome == AttemptOutcome.failed
    assert attempt.provider_state == ProviderTransferState.cancelled
    assert attempt.terminal_at is not None
    assert attempt.provider_terminal_at is not None
    assert attempt.error_code == "cancelled"
