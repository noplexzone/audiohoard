from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.acquisition_attempt import (
    AcquisitionAttempt,
    ArtifactState,
    AttemptOutcome,
    CleanupState,
    ProviderTransferState,
)
from app.models.job import Job
from app.models.release import Release
from app.models.track import Track


async def test_multiple_acquisition_attempts_coexist_for_one_track(
    db_session: AsyncSession,
) -> None:
    job = Job(source="slskd", query="Artist Track")
    release = Release(job=job, source="slskd", title="Album")
    track = Track(job=job, release=release, source="slskd", title="Track")
    db_session.add_all([job, release, track])
    await db_session.flush()

    first = AcquisitionAttempt(
        job=job,
        track=track,
        provider="slskd",
        peer="peer-a",
        remote_path=r"Artist\\Album\\01 Track.flac",
    )
    second = AcquisitionAttempt(
        job=job,
        track=track,
        provider="slskd",
        peer="peer-b",
        remote_path=r"Other\\Track.flac",
    )
    db_session.add_all([first, second])
    await db_session.commit()

    attempts = list(
        (
            await db_session.scalars(
                select(AcquisitionAttempt)
                .where(AcquisitionAttempt.track_id == track.id)
                .order_by(AcquisitionAttempt.id)
            )
        ).all()
    )
    assert [attempt.peer for attempt in attempts] == ["peer-a", "peer-b"]
    assert all(attempt.provider_state == ProviderTransferState.pending for attempt in attempts)
    assert all(attempt.artifact_state == ArtifactState.none for attempt in attempts)
    assert all(attempt.outcome == AttemptOutcome.pending for attempt in attempts)
    assert all(attempt.provider_cleanup_state == CleanupState.pending for attempt in attempts)
    assert all(attempt.file_cleanup_state == CleanupState.pending for attempt in attempts)
    assert all(attempt.claim_version == 0 for attempt in attempts)


async def test_attempt_provider_checkpoint_is_idempotent(db_session: AsyncSession) -> None:
    job = Job(source="slskd", query="Artist Track")
    attempt = AcquisitionAttempt(
        job=job,
        provider="slskd",
        peer="peer-a",
        remote_path=r"Artist\\Album\\01 Track.flac",
        provider_transfer_id="2d93899b-cf9a-4567-8f10-993610f274cf",
        provider_state=ProviderTransferState.enqueued,
    )
    db_session.add_all([job, attempt])
    await db_session.commit()

    attempt.provider_transfer_id = "2d93899b-cf9a-4567-8f10-993610f274cf"
    attempt.provider_state = ProviderTransferState.enqueued
    await db_session.commit()

    persisted = await db_session.get(AcquisitionAttempt, attempt.id)
    assert persisted is not None
    assert persisted.provider_transfer_id == "2d93899b-cf9a-4567-8f10-993610f274cf"
    assert persisted.provider_state == ProviderTransferState.enqueued
