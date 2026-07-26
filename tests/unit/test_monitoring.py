from __future__ import annotations

import asyncio
import hashlib
import json
from pathlib import Path

import pytest

from app.models.import_plan import CollisionState
from app.models.job import Job, JobStatus
from app.models.monitoring import MonitoringRecord, MonitoringStatus
from app.models.release import Release
from app.models.release_candidate import MatchReviewState, ReleaseCandidate
from app.models.track import Track
from app.models.workflow import ImportWorkflowState
from app.services.monitoring import (
    MonitoringCheckAlreadyRunning,
    QualityProfile,
    evaluate_quality_candidate,
    execute_quality_upgrade,
    run_monitoring_check,
)


def test_quality_candidate_requires_confident_meaningful_gain() -> None:
    profile = QualityProfile(preferred_codecs=("flac", "mp3"), minimum_bitrate_kbps=320)
    current = {"codec": "mp3", "bitrate_kbps": 320, "reliability": 0.9}
    assert evaluate_quality_candidate(
        profile, current, {"codec": "flac", "lossless": True, "reliability": 0.9}, 0.95
    ).meaningful
    assert not evaluate_quality_candidate(
        profile, current, {"codec": "mp3", "bitrate_kbps": 321, "reliability": 0.9}, 0.95
    ).meaningful
    assert not evaluate_quality_candidate(
        profile, current, {"codec": "flac", "lossless": True, "reliability": 0.9}, 0.70
    ).meaningful


async def test_monitoring_check_is_non_overlapping_and_persists_history(db_session) -> None:
    job = Job(source="slskd", query="album", status=JobStatus.done)
    db_session.add(job)
    await db_session.flush()
    release = Release(job_id=job.id, source="slskd", title="Album")
    db_session.add(release)
    await db_session.flush()
    record = MonitoringRecord(
        release_id=release.id,
        desired_quality_json=QualityProfile(preferred_codecs=("flac",)).to_json(),
    )
    db_session.add(record)
    await db_session.flush()
    entered = asyncio.Event()
    resume = asyncio.Event()

    async def discover() -> list[ReleaseCandidate]:
        entered.set()
        await resume.wait()
        return []

    first = asyncio.create_task(run_monitoring_check(db_session, record, {}, discover))
    await entered.wait()
    with pytest.raises(MonitoringCheckAlreadyRunning):
        await run_monitoring_check(db_session, record, {}, discover)
    resume.set()
    await first
    assert record.status == MonitoringStatus.active
    assert json.loads(record.history_json or "[]")[-1]["outcome"] == "no_upgrade"
    assert record.last_checked_at is not None


async def test_upgrade_failure_preserves_existing_library_file(db_session, tmp_path: Path) -> None:
    from app.services.library_import import (
        ImportExecutionError,
        execute_release_import,
        plan_release_import,
    )

    job = Job(source="slskd", query="album", status=JobStatus.done)
    db_session.add(job)
    await db_session.flush()
    release = Release(
        job_id=job.id,
        source="slskd",
        title="Album",
        album_artist="Artist",
        year="2000",
        import_state=ImportWorkflowState.ready,
    )
    db_session.add(release)
    await db_session.flush()
    staged = tmp_path / "candidate.wav"
    staged.write_bytes(b"new candidate")
    track = Track(
        job_id=job.id,
        release_id=release.id,
        source="slskd",
        title="Song",
        artist="Artist",
        album_artist="Artist",
        album="Album",
        year="2000",
        disc=1,
        disc_total=1,
        track_no=1,
        staging_path=str(staged),
        source_path=str(staged),
        import_state=ImportWorkflowState.ready,
    )
    db_session.add(track)
    await db_session.flush()
    destination = tmp_path / "library" / "Artist" / "2000 - Album" / "01 - Song.wav"
    destination.parent.mkdir(parents=True)
    destination.write_bytes(b"old preferred file")
    plans = await plan_release_import(db_session, release, library_root=tmp_path / "library")
    plans[0].status = ImportWorkflowState.ready
    plans[0].collision_state = CollisionState.clear
    with pytest.raises(ImportExecutionError, match="tag readback failed"):
        await execute_release_import(
            db_session, release, library_root=tmp_path / "library", replace_existing_verified=True
        )
    assert destination.read_bytes() == b"old preferred file"


@pytest.mark.parametrize(
    "review_state", [MatchReviewState.rejected, MatchReviewState.needs_review]
)
async def test_monitoring_rejects_unapproved_candidates(db_session, review_state) -> None:
    job = Job(source="slskd", query="album", status=JobStatus.done)
    release = Release(job=job, source="slskd", title="Album")
    record = MonitoringRecord(
        release=release,
        desired_quality_json=QualityProfile(preferred_codecs=("flac", "mp3")).to_json(),
    )
    candidate = ReleaseCandidate(
        release=release,
        quality_json=json.dumps({"codec": "flac", "lossless": True, "reliability": 1.0}),
        match_score=1.0,
        review_state=review_state,
        selected=False,
    )
    db_session.add_all([job, release, record, candidate])
    await db_session.flush()

    async def discover() -> list[ReleaseCandidate]:
        return [candidate]

    assert (
        await run_monitoring_check(
            db_session, record, {"codec": "mp3", "bitrate_kbps": 320}, discover
        )
        is None
    )
    with pytest.raises(Exception, match="approved selected candidate"):
        await execute_quality_upgrade(
            db_session,
            record,
            candidate,
            {"codec": "mp3", "bitrate_kbps": 320},
            library_root=Path("/unused"),
        )


async def test_upgrade_rejects_stale_candidate_artifact(db_session, tmp_path: Path) -> None:
    job = Job(source="slskd", query="album", status=JobStatus.done)
    release = Release(job=job, source="slskd", title="Album")
    track = Track(job=job, release=release, source="slskd", title="Song", track_no=1)
    staged = tmp_path / "candidate.flac"
    staged.write_bytes(b"changed after approval")
    candidate = ReleaseCandidate(
        release=release,
        quality_json=json.dumps({"codec": "flac", "lossless": True, "reliability": 1.0}),
        evidence_json=json.dumps(
            {
                "artifacts": [
                    {
                        "track_id": 1,
                        "staging_path": str(staged),
                        "sha256": hashlib.sha256(b"approved bytes").hexdigest(),
                        "quality": {"codec": "flac", "lossless": True, "reliability": 1.0},
                    }
                ]
            }
        ),
        match_score=1.0,
        review_state=MatchReviewState.auto_selected,
        selected=True,
    )
    db_session.add_all([job, release, track, candidate])
    await db_session.flush()
    evidence = json.loads(candidate.evidence_json or "{}")
    evidence["artifacts"][0]["track_id"] = track.id
    candidate.evidence_json = json.dumps(evidence)
    record = MonitoringRecord(
        release=release,
        candidate_id=candidate.id,
        desired_quality_json=QualityProfile(preferred_codecs=("flac", "mp3")).to_json(),
    )
    db_session.add(record)
    await db_session.flush()

    with pytest.raises(Exception, match="hash does not match"):
        await execute_quality_upgrade(
            db_session,
            record,
            candidate,
            {"codec": "mp3", "bitrate_kbps": 320},
            library_root=tmp_path / "library",
        )


async def test_upgrade_uses_only_candidate_bound_artifact(db_session, tmp_path: Path) -> None:
    job = Job(source="slskd", query="album", status=JobStatus.done)
    release = Release(job=job, source="slskd", title="Album", album_artist="Artist", year="2000")
    unrelated = tmp_path / "retained-unrelated.flac"
    unrelated.write_bytes(b"unrelated retained bytes")
    artifact = tmp_path / "approved-candidate.flac"
    artifact.write_bytes(b"approved candidate bytes")
    track = Track(
        job=job,
        release=release,
        source="slskd",
        title="Song",
        artist="Artist",
        album_artist="Artist",
        album="Album",
        year="2000",
        disc=1,
        disc_total=1,
        track_no=1,
        staging_path=str(unrelated),
        source_path=str(unrelated),
    )
    db_session.add_all([job, release, track])
    await db_session.flush()
    quality = {"codec": "flac", "lossless": True, "reliability": 1.0}
    candidate = ReleaseCandidate(
        release=release,
        quality_json=json.dumps(quality),
        evidence_json=json.dumps(
            {
                "artifacts": [
                    {
                        "track_id": track.id,
                        "staging_path": str(artifact),
                        "sha256": hashlib.sha256(artifact.read_bytes()).hexdigest(),
                        "quality": quality,
                    }
                ]
            }
        ),
        match_score=1.0,
        review_state=MatchReviewState.manual_selected,
        selected=True,
    )
    db_session.add(candidate)
    await db_session.flush()
    record = MonitoringRecord(
        release=release,
        candidate_id=candidate.id,
        desired_quality_json=QualityProfile(preferred_codecs=("flac", "mp3")).to_json(),
    )
    db_session.add(record)
    await db_session.flush()
    destination = tmp_path / "library" / "Artist" / "2000 - Album" / "01 - Song.flac"
    destination.parent.mkdir(parents=True)
    destination.write_bytes(b"old preferred bytes")

    class AcceptTags:
        def write_and_verify(self, path: Path, tags: dict[str, str]) -> bool:
            return True

    await execute_quality_upgrade(
        db_session,
        record,
        candidate,
        {"codec": "mp3", "bitrate_kbps": 320},
        library_root=tmp_path / "library",
        tag_writer=AcceptTags(),  # type: ignore[arg-type]
    )
    assert destination.read_bytes() == b"approved candidate bytes"
    assert destination.read_bytes() != unrelated.read_bytes()


async def test_second_backup_cleanup_failure_does_not_roll_back_first_track(
    db_session, tmp_path: Path, monkeypatch
) -> None:
    from app.services import library_import
    from app.services.library_import import execute_release_import, plan_release_import

    job = Job(source="slskd", query="album", status=JobStatus.done)
    release = Release(job=job, source="slskd", title="Album", album_artist="Artist", year="2000")
    db_session.add_all([job, release])
    await db_session.flush()
    tracks = []
    for number in (1, 2):
        staged = tmp_path / f"candidate-{number}.wav"
        staged.write_bytes(f"new {number}".encode())
        track = Track(
            job=job,
            release=release,
            source="slskd",
            title=f"Song {number}",
            artist="Artist",
            album_artist="Artist",
            album="Album",
            year="2000",
            disc=1,
            disc_total=1,
            track_no=number,
            staging_path=str(staged),
            source_path=str(staged),
            import_state=ImportWorkflowState.ready,
        )
        tracks.append(track)
        db_session.add(track)
    await db_session.flush()
    plans = await plan_release_import(db_session, release, library_root=tmp_path / "library")
    for plan, track in zip(plans, tracks, strict=True):
        plan.track = track
    destinations = [Path(plan.destination_path) for plan in plans]
    for number, (plan, destination) in enumerate(zip(plans, destinations, strict=True), 1):
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(f"old {number}".encode())
        plan.status = ImportWorkflowState.ready
        plan.collision_state = CollisionState.clear

    class AcceptTags:
        def write_and_verify(self, path: Path, tags: dict[str, str]) -> bool:
            return True

    original_unlink = library_import._unlink_backup_after_commit
    calls = 0

    def fail_second_cleanup(path: Path) -> None:
        nonlocal calls
        calls += 1
        if calls == 2:
            raise OSError("second cleanup failed")
        original_unlink(path)

    monkeypatch.setattr(library_import, "_unlink_backup_after_commit", fail_second_cleanup)
    await execute_release_import(
        db_session,
        release,
        library_root=tmp_path / "library",
        tag_writer=AcceptTags(),  # type: ignore[arg-type]
        replace_existing_verified=True,
    )
    assert calls == 0
    await db_session.commit()
    assert calls == 2
    assert [path.read_bytes() for path in destinations] == [b"new 1", b"new 2"]
