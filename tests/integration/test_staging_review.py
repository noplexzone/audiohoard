from __future__ import annotations

from pathlib import Path

from httpx import AsyncClient

from app.config import Settings
from app.database import get_session_factory
from app.models.job import Job, JobStatus
from app.models.release import Release
from app.models.staging_review import StagingReviewItem
from app.models.track import Track
from app.models.workflow import (
    AcoustIDVerificationState,
    AcquisitionState,
    ReviewDecision,
)


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


async def test_review_approve_resumes_import_and_deny_retains_staging(
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
    assert denied_path.exists()

    factory = get_session_factory()
    async with factory() as db:
        approved_row = await db.get(Track, approved_track)
        denied_row = await db.get(Track, denied_track)
        assert approved_row is not None
        assert denied_row is not None
        assert approved_row.acoustid_verification_state == AcoustIDVerificationState.approved
        assert denied_row.acoustid_verification_state == AcoustIDVerificationState.denied
        assert denied_row.acquisition_state == AcquisitionState.failed
