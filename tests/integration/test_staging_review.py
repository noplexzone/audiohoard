from __future__ import annotations

import asyncio
import json
import shutil
import subprocess
from pathlib import Path

import pytest
from httpx import AsyncClient
from sqlalchemy import select

from app.config import Settings
from app.database import get_session_factory
from app.models.import_plan import CollisionState, ImportPlan, TagVerificationState
from app.models.job import Job, JobStatus
from app.models.release import Release
from app.models.source_candidate_block import SourceCandidateBlock
from app.models.staging_review import StagingReviewItem
from app.models.track import Track
from app.models.workflow import (
    AcoustIDVerificationState,
    AcquisitionState,
    ImportWorkflowState,
    ReviewDecision,
)
from app.settings_service import DEFAULT_MAX_PARTIAL_ATTEMPTS


async def _review_fixture(settings: Settings, name: str) -> tuple[int, int, Path]:
    settings.staging_root.mkdir(parents=True, exist_ok=True)
    audio = settings.staging_root / f"{name}.mp3"
    audio.write_bytes(b"0123456789abcdef")  # noqa: ASYNC240
    factory = get_session_factory()
    async with factory() as db:
        job = Job(source="slskd", query=name, status=JobStatus.done)
        release = Release(job=job, source="slskd", title="Album", album_artist="Artist")
        track = Track(
            job=job,
            release=release,
            source="slskd",
            title=name,
            track_no=7,
            source_path=str(audio),
            staging_path=str(audio),
            acquisition_state=AcquisitionState.downloaded,
            acoustid_verification_state=AcoustIDVerificationState.mismatch,
        )
        item = StagingReviewItem(
            track=track,
            release=release,
            expected_title=name,
            verification_reason="mismatch",
            review_state=ReviewDecision.pending,
        )
        db.add_all([job, release, track, item])
        await db.commit()
        return item.id, track.id, audio


async def test_staged_audio_requires_auth_and_supports_ranges(
    client: AsyncClient, test_settings: Settings
) -> None:
    item_id, _, _ = await _review_fixture(test_settings, "range")
    ranged = await client.get(f"/staging/audio/{item_id}", headers={"Range": "bytes=2-5"})
    assert ranged.status_code == 206
    assert ranged.content == b"2345"
    assert ranged.headers["content-range"] == "bytes 2-5/16"
    assert ranged.headers["accept-ranges"] == "bytes"

    session_cookie = client.cookies.get("session")
    client.cookies.delete("session")
    unauthenticated = await client.get(f"/staging/audio/{item_id}", follow_redirects=False)
    assert unauthenticated.status_code in {302, 303, 307, 401}
    if session_cookie:
        client.cookies.set("session", session_cookie)


async def test_staged_m4a_is_transcoded_to_seekable_browser_preview(
    client: AsyncClient, test_settings: Settings
) -> None:
    ffmpeg = shutil.which("ffmpeg")
    if ffmpeg is None:
        pytest.skip("ffmpeg is required for browser-preview transcoding")
    test_settings.staging_root.mkdir(parents=True, exist_ok=True)
    audio = test_settings.staging_root / "atmos.m4a"
    await asyncio.to_thread(
        subprocess.run,
        [
            ffmpeg,
            "-nostdin",
            "-v",
            "error",
            "-f",
            "lavfi",
            "-i",
            "sine=frequency=440:duration=0.3",
            "-c:a",
            "eac3",
            "-tag:a",
            "ec-3",
            "-f",
            "mp4",
            str(audio),
        ],
        check=True,
        timeout=30,
    )
    factory = get_session_factory()
    async with factory() as db:
        job = Job(source="slskd", query="atmos", status=JobStatus.done)
        release = Release(job=job, source="slskd", title="Album", album_artist="Artist")
        track = Track(
            job=job,
            release=release,
            source="slskd",
            title="Atmos",
            source_path=str(audio),
            staging_path=str(audio),
            acquisition_state=AcquisitionState.downloaded,
        )
        item = StagingReviewItem(
            track=track,
            release=release,
            expected_title="Atmos",
            verification_reason="unavailable",
            review_state=ReviewDecision.pending,
        )
        db.add_all([job, release, track, item])
        await db.commit()
        item_id = item.id

    response = await client.get(f"/staging/audio/{item_id}")
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("audio/mpeg")
    assert len(response.content) > 100
    ranged = await client.get(f"/staging/audio/{item_id}", headers={"Range": "bytes=2-20"})
    assert ranged.status_code == 206
    assert ranged.headers["accept-ranges"] == "bytes"
    assert len(ranged.content) == 19


async def test_review_approve_resumes_import_and_deny_removes_staged_item(
    client: AsyncClient, test_settings: Settings, monkeypatch
) -> None:
    from app.services import auto_import

    approved_item, approved_track, _ = await _review_fixture(test_settings, "approve")
    denied_item, denied_track, denied_path = await _review_fixture(test_settings, "deny")
    imported: list[int] = []

    async def fake_auto_import(db, release, **kwargs):
        del db, kwargs
        imported.append(release.id)
        return True

    monkeypatch.setattr(auto_import, "try_auto_import_release", fake_auto_import)
    approved = await client.post(
        f"/staging/review/{approved_item}/approve", follow_redirects=False
    )
    denied = await client.post(f"/staging/review/{denied_item}/deny", follow_redirects=False)
    assert approved.status_code == 303
    assert denied.status_code == 303
    assert imported
    assert not denied_path.exists()

    factory = get_session_factory()
    async with factory() as db:
        approved_row = await db.get(Track, approved_track)
        denied_row = await db.get(Track, denied_track)
        assert approved_row is not None
        assert denied_row is not None
        assert approved_row.acoustid_verification_state == AcoustIDVerificationState.approved
        assert denied_row.acoustid_verification_state == AcoustIDVerificationState.denied
        assert denied_row.acquisition_state == AcquisitionState.failed
        assert denied_row.staging_path is None
        assert denied_row.source_path is None
        assert await db.get(StagingReviewItem, denied_item) is None


async def test_review_approve_retries_sqlite_lock_before_import(
    client: AsyncClient, test_settings: Settings, monkeypatch
) -> None:
    from sqlalchemy.exc import OperationalError
    from sqlalchemy.ext.asyncio import AsyncSession

    from app.services import auto_import

    item_id, track_id, _ = await _review_fixture(test_settings, "approve-lock")
    original_commit = AsyncSession.commit
    attempts = 0
    imported: list[int] = []

    async def lock_then_commit(self) -> None:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise OperationalError(
                "UPDATE staging_review_items", {}, Exception("database is locked")
            )
        await original_commit(self)

    async def fake_auto_import(db, release, **kwargs):
        del db, kwargs
        imported.append(release.id)
        return True

    monkeypatch.setattr(AsyncSession, "commit", lock_then_commit)
    monkeypatch.setattr(auto_import, "try_auto_import_release", fake_auto_import)

    response = await client.post(f"/staging/review/{item_id}/approve", follow_redirects=False)

    assert response.status_code == 303
    assert response.headers["location"] == "/downloads?notice=approved"
    assert attempts >= 2
    assert imported
    factory = get_session_factory()
    async with factory() as db:
        item = await db.get(StagingReviewItem, item_id)
        track = await db.get(Track, track_id)
        assert item is not None and track is not None
        assert item.review_state == ReviewDecision.approved
        assert track.acoustid_verification_state == AcoustIDVerificationState.approved


async def test_review_approve_stays_successful_when_release_reload_is_locked(
    client: AsyncClient, test_settings: Settings, monkeypatch
) -> None:
    from sqlalchemy.exc import OperationalError
    from sqlalchemy.ext.asyncio import AsyncSession

    item_id, track_id, _ = await _review_fixture(test_settings, "approve-reload-lock")
    original_get = AsyncSession.get
    locked = False

    async def lock_release_reload(self, entity, ident, **kwargs):
        nonlocal locked
        if entity is Release and not locked:
            locked = True
            raise OperationalError("SELECT releases", {}, Exception("database is locked"))
        return await original_get(self, entity, ident, **kwargs)

    monkeypatch.setattr(AsyncSession, "get", lock_release_reload)

    response = await client.post(f"/staging/review/{item_id}/approve", follow_redirects=False)

    assert response.status_code == 303
    assert response.headers["location"] == "/downloads?notice=approved"
    assert locked
    factory = get_session_factory()
    async with factory() as db:
        item = await db.get(StagingReviewItem, item_id)
        track = await db.get(Track, track_id)
        assert item is not None and track is not None
        assert item.review_state == ReviewDecision.approved
        assert track.acoustid_verification_state == AcoustIDVerificationState.approved


async def test_review_approve_stays_successful_when_auto_import_is_locked(
    client: AsyncClient, test_settings: Settings, monkeypatch
) -> None:
    from sqlalchemy.exc import OperationalError

    from app.services import auto_import

    item_id, track_id, _ = await _review_fixture(test_settings, "approve-import-lock")

    async def locked_auto_import(*args, **kwargs):
        del args, kwargs
        raise OperationalError("UPDATE releases", {}, Exception("database is locked"))

    monkeypatch.setattr(auto_import, "try_auto_import_release", locked_auto_import)

    response = await client.post(f"/staging/review/{item_id}/approve", follow_redirects=False)

    assert response.status_code == 303
    assert response.headers["location"] == "/downloads?notice=approved"
    factory = get_session_factory()
    async with factory() as db:
        item = await db.get(StagingReviewItem, item_id)
        track = await db.get(Track, track_id)
        assert item is not None and track is not None
        assert item.review_state == ReviewDecision.approved
        assert track.acoustid_verification_state == AcoustIDVerificationState.approved


async def test_deny_blocks_slskd_candidate_from_track_provenance(
    client: AsyncClient, test_settings: Settings
) -> None:
    item_id, track_id, denied_path = await _review_fixture(test_settings, "deny-slskd-block")
    blocked_filename = (
        "music\\done\\country\\VA - Country Heat - 05.09.2026"
        "\\44 - Ty Myers - Valerie (Amazon Music Original).mp3"
    )
    factory = get_session_factory()
    async with factory() as db:
        track = await db.get(Track, track_id)
        assert track is not None
        track.acquisition_provenance_json = json.dumps(
            {
                "source": "slskd",
                "username": "StarCaller",
                "filename": blocked_filename,
            }
        )
        await db.commit()

    denied = await client.post(f"/staging/review/{item_id}/deny", follow_redirects=False)

    assert denied.status_code == 303
    assert not denied_path.exists()
    async with factory() as db:
        blocked = (
            await db.scalars(
                select(SourceCandidateBlock).where(
                    SourceCandidateBlock.provider == "slskd",
                    SourceCandidateBlock.peer == "StarCaller",
                )
            )
        ).one()
        assert blocked.filename == blocked_filename
        assert blocked.reason == "denied"


async def test_deny_restores_staged_file_when_database_commit_fails(
    client: AsyncClient, test_settings: Settings, monkeypatch
) -> None:
    from sqlalchemy.ext.asyncio import AsyncSession

    item_id, _, staged_path = await _review_fixture(test_settings, "deny-rollback")

    async def fail_commit(self) -> None:
        raise RuntimeError("commit failed")

    monkeypatch.setattr(AsyncSession, "commit", fail_commit)
    with pytest.raises(RuntimeError, match="commit failed"):
        await client.post(f"/staging/review/{item_id}/deny", follow_redirects=False)

    assert staged_path.exists()
    assert not list(staged_path.parent.glob(f".{staged_path.name}.denied-*"))


async def test_deny_settles_commit_before_cleanup_when_request_is_cancelled(
    client: AsyncClient, test_settings: Settings, monkeypatch
) -> None:
    import asyncio

    from sqlalchemy.ext.asyncio import AsyncSession

    item_id, _, staged_path = await _review_fixture(test_settings, "deny-cancel")
    original_commit = AsyncSession.commit
    committed = asyncio.Event()
    release_commit = asyncio.Event()

    async def commit_then_wait(self) -> None:
        await original_commit(self)
        committed.set()
        await release_commit.wait()

    monkeypatch.setattr(AsyncSession, "commit", commit_then_wait)
    request_task = asyncio.create_task(
        client.post(f"/staging/review/{item_id}/deny", follow_redirects=False)
    )
    await committed.wait()
    request_task.cancel()
    await asyncio.sleep(0)
    request_task.cancel()
    release_commit.set()
    with pytest.raises(asyncio.CancelledError):
        await request_task

    assert not staged_path.exists()
    assert not list(staged_path.parent.glob(f".{staged_path.name}.denied-*"))
    factory = get_session_factory()
    async with factory() as db:
        assert await db.get(StagingReviewItem, item_id) is None


async def test_deny_restores_staged_file_even_when_rollback_fails(
    client: AsyncClient, test_settings: Settings, monkeypatch
) -> None:
    from sqlalchemy.ext.asyncio import AsyncSession

    item_id, _, staged_path = await _review_fixture(test_settings, "deny-rollback-failure")

    async def fail_commit(self) -> None:
        raise RuntimeError("commit failed")

    async def fail_rollback(self) -> None:
        raise RuntimeError("rollback failed")

    monkeypatch.setattr(AsyncSession, "commit", fail_commit)
    monkeypatch.setattr(AsyncSession, "rollback", fail_rollback)
    with pytest.raises(RuntimeError, match="rollback failed"):
        await client.post(f"/staging/review/{item_id}/deny", follow_redirects=False)

    assert staged_path.exists()
    assert not list(staged_path.parent.glob(f".{staged_path.name}.denied-*"))


async def test_deny_preserves_unrelated_release_failure(
    client: AsyncClient, test_settings: Settings
) -> None:
    item_id, track_id, _ = await _review_fixture(test_settings, "deny-unrelated")
    factory = get_session_factory()
    async with factory() as db:
        track = await db.get(Track, track_id)
        assert track is not None
        release = await db.get(Release, track.release_id)
        assert release is not None
        release.error_detail = "import execution error: destination race"
        release.import_state = ImportWorkflowState.failed
        await db.commit()
        release_id = release.id

    denied = await client.post(f"/staging/review/{item_id}/deny", follow_redirects=False)
    page = await client.get("/review")

    assert denied.status_code == 303
    assert "import execution error: destination race" in page.text
    async with factory() as db:
        release = await db.get(Release, release_id)
        assert release is not None
        assert release.error_detail == "import execution error: destination race"


async def test_deny_does_not_leave_empty_acoustid_release_review(
    client: AsyncClient, test_settings: Settings
) -> None:
    item_id, _, _ = await _review_fixture(test_settings, "deny-only")

    denied = await client.post(f"/staging/review/{item_id}/deny", follow_redirects=False)
    page = await client.get("/downloads")

    assert denied.status_code == 303
    assert "AcoustID mismatch on track 7" not in page.text


async def test_deny_removes_non_audio_review_artifact(
    client: AsyncClient, test_settings: Settings
) -> None:
    settings = test_settings
    settings.staging_root.mkdir(parents=True, exist_ok=True)
    lyric = settings.staging_root / "track.lrc"
    lyric.write_text("[00:00.00]lyrics")  # noqa: ASYNC240
    factory = get_session_factory()
    async with factory() as db:
        job = Job(source="slskd", query="lyrics", status=JobStatus.done)
        release = Release(job=job, source="slskd", title="Album", album_artist="Artist")
        track = Track(
            job=job,
            release=release,
            source="slskd",
            title="Lyrics",
            track_no=1,
            source_path=str(lyric),
            staging_path=str(lyric),
            file_format="lrc",
            acquisition_state=AcquisitionState.downloaded,
            acoustid_verification_state=AcoustIDVerificationState.unavailable,
        )
        item = StagingReviewItem(
            track=track,
            release=release,
            expected_title="Lyrics",
            verification_reason="unavailable",
            review_state=ReviewDecision.pending,
        )
        db.add_all([job, release, track, item])
        await db.commit()
        item_id = item.id
        track_id = track.id

    audio = await client.get(f"/staging/audio/{item_id}")
    denied = await client.post(f"/staging/review/{item_id}/deny", follow_redirects=False)

    assert audio.status_code == 400
    assert denied.status_code == 303
    assert not lyric.exists()
    async with factory() as db:
        assert await db.get(StagingReviewItem, item_id) is None
        denied_track = await db.get(Track, track_id)
        assert denied_track is not None
        assert denied_track.staging_path is None
        assert denied_track.acoustid_verification_state == AcoustIDVerificationState.denied


async def test_deny_clears_review_with_unsafe_stale_staging_path(
    client: AsyncClient, test_settings: Settings
) -> None:
    outside = test_settings.staging_root.parent / "outside-review.mp3"
    outside.parent.mkdir(parents=True, exist_ok=True)
    outside.write_bytes(b"outside")  # noqa: ASYNC240
    factory = get_session_factory()
    async with factory() as db:
        job = Job(source="slskd", query="unsafe", status=JobStatus.done)
        release = Release(job=job, source="slskd", title="Album", album_artist="Artist")
        track = Track(
            job=job,
            release=release,
            source="slskd",
            title="Unsafe",
            source_path=str(outside),
            staging_path=str(outside),
            acquisition_state=AcquisitionState.downloaded,
            acoustid_verification_state=AcoustIDVerificationState.mismatch,
        )
        item = StagingReviewItem(
            track=track,
            release=release,
            expected_title="Unsafe",
            verification_reason="mismatch",
            review_state=ReviewDecision.pending,
        )
        db.add_all([job, release, track, item])
        await db.commit()
        item_id = item.id
        track_id = track.id

    response = await client.post(f"/staging/review/{item_id}/deny", follow_redirects=False)

    assert response.status_code == 303
    assert outside.exists()
    async with factory() as db:
        assert await db.get(StagingReviewItem, item_id) is None
        denied_track = await db.get(Track, track_id)
        assert denied_track is not None
        assert denied_track.source_path is None
        assert denied_track.staging_path is None
        assert denied_track.acoustid_verification_state == AcoustIDVerificationState.denied


async def test_review_page_has_only_approve_and_deny_actions(
    client: AsyncClient, test_settings: Settings, monkeypatch
) -> None:
    item_id, track_id, staged_path = await _review_fixture(test_settings, "reason")
    factory = get_session_factory()
    async with factory() as db:
        track = await db.get(Track, track_id)
        assert track is not None
        track.acquisition_provenance_json = json.dumps(
            {
                "source": "slskd",
                "username": "review-peer",
                "filename": r"Remote Album\07 Original Track.flac",
            }
        )
        await db.commit()

    async def no_reference(*args, **kwargs):
        return None

    monkeypatch.setattr("app.services.staging.resolve_reference_audio", no_reference)
    page = await client.get("/review")
    assert page.status_code == 200
    assert "Artist — Album" in page.text
    assert f"/staging/review/{item_id}/approve" in page.text
    assert f"/staging/review/{item_id}/deny" in page.text
    assert "Acquisition source</dt><dd>Soulseek (slskd)</dd>" in page.text
    assert "Original filename</dt><dd><code>07 Original Track.flac</code></dd>" in page.text
    assert "/dismiss" not in page.text

    dismissed = await client.post(f"/staging/review/{item_id}/dismiss", follow_redirects=False)
    assert dismissed.status_code == 404
    assert staged_path.exists()


async def test_missing_source_review_exposes_reacquire_and_queues_continuation(
    client: AsyncClient, test_settings: Settings, monkeypatch
) -> None:
    missing = test_settings.staging_root / "gone.mp3"
    retained = test_settings.staging_root / "retained.mp3"
    retained.parent.mkdir(parents=True, exist_ok=True)
    retained.write_bytes(b"keep me")  # noqa: ASYNC240
    factory = get_session_factory()
    async with factory() as db:
        job = Job(source="slskd", query="Artist Album", status=JobStatus.done)
        release = Release(
            job=job,
            source="slskd",
            title="Missing Source Album",
            album_artist="Artist",
            import_state=ImportWorkflowState.needs_review,
            error_detail=f"missing staged source: {missing}",
        )
        missing_track = Track(
            job=job,
            release=release,
            source="slskd",
            artist="Artist",
            album="Missing Source Album",
            title="Gone",
            staging_path=str(missing),
            acquisition_state=AcquisitionState.downloaded,
            acoustid_verification_state=AcoustIDVerificationState.verified,
        )
        retained_track = Track(
            job=job,
            release=release,
            source="slskd",
            artist="Artist",
            album="Missing Source Album",
            title="Retained",
            staging_path=str(retained),
            acquisition_state=AcquisitionState.downloaded,
            acoustid_verification_state=AcoustIDVerificationState.verified,
        )
        plan = ImportPlan(
            release=release,
            track=missing_track,
            source_path=str(missing),
            staging_path=str(missing),
            destination_path=str(test_settings.library_root / "gone.mp3"),
            collision_state=CollisionState.needs_review,
            tag_verification_state=TagVerificationState.pending,
            status=ImportWorkflowState.needs_review,
            error_detail="source path is not a regular file",
        )
        db.add_all([job, release, missing_track, retained_track, plan])
        db.add(Job(source="slskd", query="unrelated free text", status=JobStatus.pending))
        await db.commit()
        release_id = release.id
        missing_track_id = missing_track.id

    dispatched: list[int] = []

    async def fake_dispatch(job_id: int):
        dispatched.append(job_id)

    monkeypatch.setattr("app.routers.staging.job_dispatcher.dispatch", fake_dispatch)

    page = await client.get("/review")
    assert f"missing staged source: {missing}" in page.text
    assert f"/staging/release/{release_id}/reacquire" in page.text
    assert ">Re-acquire<" in page.text

    response = await client.post(
        f"/staging/release/{release_id}/reacquire", follow_redirects=False
    )
    assert response.status_code == 303
    assert response.headers["location"] == "/downloads?notice=reacquired"
    assert retained.exists()
    assert dispatched

    async with factory() as db:
        track = await db.get(Track, missing_track_id)
        release = await db.get(Release, release_id)
        assert track is not None and release is not None
        assert track.staging_path is None
        assert track.acquisition_state == AcquisitionState.failed
        assert release.import_state == ImportWorkflowState.discovered
        assert release.error_detail is None
        continuation = await db.scalar(select(Job).where(Job.parent_job_id == release.job_id))
        assert continuation is not None
        assert continuation.query == "Artist Missing Source Album Gone"
        assert continuation.catalog_album_id is None
        assert continuation.catalog_track_id is None


async def test_release_review_dismiss_hides_actionless_release_review(
    client: AsyncClient, test_settings: Settings
) -> None:
    factory = get_session_factory()
    async with factory() as db:
        job = Job(source="slskd", query="stale review", status=JobStatus.failed)
        release = Release(
            job=job,
            source="slskd",
            title="One Thing At A Time",
            album_artist="Morgan Wallen",
            import_state=ImportWorkflowState.rolled_back,
            error_detail="import execution error: tag readback failed",
        )
        db.add_all([job, release])
        await db.commit()
        release_id = release.id

    page = await client.get("/review")
    assert f"/staging/release/{release_id}/dismiss" in page.text
    assert ">Dismiss — hide<" in page.text

    response = await client.post(f"/staging/release/{release_id}/dismiss", follow_redirects=False)

    assert response.status_code == 303
    assert response.headers["location"] == "/downloads?notice=review_dismissed"
    async with factory() as db:
        release = await db.get(Release, release_id)
        assert release is not None
        assert release.review_dismissed_at is not None

    page = await client.get("/review")
    assert f"/staging/release/{release_id}/dismiss" not in page.text


async def test_release_review_dismiss_retries_sqlite_lock_before_hiding(
    client: AsyncClient, monkeypatch
) -> None:
    from sqlalchemy.exc import OperationalError
    from sqlalchemy.ext.asyncio import AsyncSession

    factory = get_session_factory()
    async with factory() as db:
        job = Job(source="slskd", query="locked stale review", status=JobStatus.failed)
        release = Release(
            job=job,
            source="slskd",
            title="Locked Review",
            album_artist="Artist",
            import_state=ImportWorkflowState.rolled_back,
            error_detail="import execution error: tag readback failed",
        )
        db.add_all([job, release])
        await db.commit()
        release_id = release.id

    original_commit = AsyncSession.commit
    attempts = 0

    async def lock_then_commit(self) -> None:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise OperationalError("UPDATE releases", {}, Exception("database is locked"))
        await original_commit(self)

    monkeypatch.setattr(AsyncSession, "commit", lock_then_commit)

    response = await client.post(f"/staging/release/{release_id}/dismiss", follow_redirects=False)

    assert response.status_code == 303
    assert response.headers["location"] == "/downloads?notice=review_dismissed"
    assert attempts >= 2

    async with factory() as db:
        release = await db.get(Release, release_id)
        assert release is not None
        assert release.review_dismissed_at is not None


async def test_reacquire_stops_after_persisted_continuation_attempt_cap(
    client: AsyncClient, test_settings: Settings, monkeypatch
) -> None:
    factory = get_session_factory()
    query = "Artist Missing Source Album Gone"
    async with factory() as db:
        root = Job(source="slskd", query=query, status=JobStatus.done, partial_attempt=0)
        release = Release(
            job=root,
            source="slskd",
            title="Missing Source Album",
            album_artist="Artist",
            import_state=ImportWorkflowState.needs_review,
            error_detail="missing staged source: gone.mp3",
        )
        track = Track(
            job=root,
            release=release,
            source="slskd",
            artist="Artist",
            album="Missing Source Album",
            title="Gone",
            acquisition_state=AcquisitionState.failed,
            acoustid_verification_state=AcoustIDVerificationState.pending,
        )
        db.add_all([root, release, track])
        await db.flush()
        for attempt in range(1, DEFAULT_MAX_PARTIAL_ATTEMPTS + 1):
            db.add(
                Job(
                    source="priority",
                    query=query,
                    status=JobStatus.failed,
                    parent_job_id=root.id,
                    partial_attempt=attempt,
                )
            )
        await db.commit()
        release_id = release.id
        root_id = root.id

    dispatched: list[int] = []

    async def fake_dispatch(job_id: int) -> None:
        dispatched.append(job_id)

    monkeypatch.setattr("app.routers.staging.job_dispatcher.dispatch", fake_dispatch)
    response = await client.post(
        f"/staging/release/{release_id}/reacquire", follow_redirects=False
    )
    assert response.status_code == 303
    assert response.headers["location"] == "/downloads?notice=invalid_state"
    assert not dispatched

    async with factory() as db:
        attempts = list(
            (
                await db.scalars(
                    select(Job.partial_attempt)
                    .where(Job.parent_job_id == root_id)
                    .order_by(Job.partial_attempt)
                )
            ).all()
        )
        assert attempts == list(range(1, DEFAULT_MAX_PARTIAL_ATTEMPTS + 1))


async def test_downloads_replaces_review_rail_with_deck_link(
    client: AsyncClient, test_settings: Settings
) -> None:
    item_id, _, _ = await _review_fixture(test_settings, "rail")

    response = await client.get("/downloads")

    assert response.status_code == 200
    assert 'class="review-rail" aria-label="Import review"' not in response.text
    assert f"/staging/review/{item_id}/approve" not in response.text
    assert 'class="review-queue-link" href="/review"' in response.text
    assert "1 tracks awaiting review" in response.text


async def test_pending_review_count_nav_badge_appears_only_when_needed(
    client: AsyncClient, test_settings: Settings
) -> None:
    empty = await client.get("/downloads")
    assert empty.status_code == 200
    assert 'class="nav-badge"' not in empty.text

    await _review_fixture(test_settings, "badge")
    pending = await client.get("/downloads")

    assert pending.status_code == 200
    assert pending.text.count('class="nav-badge">1</span>') == 2


async def test_review_alignment_matches_deezer_preview(
    client: AsyncClient, test_settings: Settings, monkeypatch
) -> None:
    from app.services.audio_alignment import AlignmentResult

    item_id, _, _ = await _review_fixture(test_settings, "alignment-deezer")

    async def align(path, url):
        assert path.name == "alignment-deezer.mp3"
        assert url.startswith("https://cdnt-preview.dzcdn.net/")
        return AlignmentResult(offset_seconds=74.25, score=0.03, confidence="high")

    monkeypatch.setattr("app.routers.staging.align_deezer_preview", align)

    response = await client.get(
        f"/staging/review/{item_id}/alignment",
        params={
            "reference_source": "deezer",
            "reference_url": "https://cdnt-preview.dzcdn.net/reference.mp3",
        },
    )

    assert response.status_code == 200
    assert response.json() == {
        "status": "matched",
        "downloaded_offset_sec": 74.25,
        "confidence": "high",
        "method": "chromaprint",
        "message": "Reference located in the downloaded file.",
        "linked_playback": True,
    }


async def test_review_alignment_estimates_itunes_without_fetching(
    client: AsyncClient, test_settings: Settings, monkeypatch
) -> None:
    item_id, _, _ = await _review_fixture(test_settings, "alignment-itunes")
    factory = get_session_factory()
    async with factory() as db:
        item = await db.get(StagingReviewItem, item_id)
        assert item is not None
        item.fingerprint_duration_sec = 240
        await db.commit()

    async def forbidden_align(*args, **kwargs):
        raise AssertionError("iTunes preview must not be downloaded or synchronized")

    monkeypatch.setattr("app.routers.staging.align_deezer_preview", forbidden_align)

    response = await client.get(
        f"/staging/review/{item_id}/alignment",
        params={
            "reference_source": "itunes",
            "reference_url": "https://audio-ssl.itunes.apple.com/reference.m4a",
        },
    )

    assert response.status_code == 200
    assert response.json()["status"] == "estimated"
    assert response.json()["downloaded_offset_sec"] == 105.0
    assert response.json()["method"] == "centered-preview-estimate"
    assert response.json()["linked_playback"] is False


async def test_review_alignment_degrades_without_reference_or_match(
    client: AsyncClient, test_settings: Settings, monkeypatch
) -> None:
    item_id, _, _ = await _review_fixture(test_settings, "alignment-none")

    missing = await client.get(f"/staging/review/{item_id}/alignment")
    unknown = await client.get("/staging/review/999999/alignment")

    assert missing.status_code == 200
    assert missing.json()["status"] == "unavailable"
    assert unknown.status_code == 404


async def test_review_alignment_requires_auth(unauthenticated_client: AsyncClient) -> None:
    response = await unauthenticated_client.get(
        "/staging/review/1/alignment", follow_redirects=False
    )

    assert response.status_code in (401, 302, 307)
