from __future__ import annotations

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
