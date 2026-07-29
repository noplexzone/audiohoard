from __future__ import annotations

import json

import pytest
from httpx import AsyncClient
from sqlalchemy import func, select

from app.database import get_session_factory
from app.models.catalog_entities import CatalogAlbum, CatalogAlbumTrack, CatalogArtist
from app.models.job import Job, JobStatus
from app.models.monitoring import MonitoringRecord, MonitoringStatus
from app.models.release import Release
from app.models.release_candidate import MatchReviewState, ReleaseCandidate
from app.models.track import Track
from app.models.workflow import AcquisitionState, ImportWorkflowState
from app.services.monitoring import QualityProfile


async def _seed_imported_album() -> tuple[int, int]:
    async with get_session_factory()() as db:
        artist = CatalogArtist(name="Artist")
        album = CatalogAlbum(artist=artist, title="Album", track_count=1, in_library=True)
        catalog_track = CatalogAlbumTrack(album=album, position=1, disc=1, title="Song")
        job = Job(source="slskd", query="Artist Album", status=JobStatus.done, catalog_album=album)
        release = Release(job=job, source="slskd", title="Album", album_artist="Artist")
        track = Track(
            job=job,
            release=release,
            catalog_album=album,
            catalog_track=catalog_track,
            source="slskd",
            title="Song",
            file_format="mp3",
            file_size_bytes=100,
            acquisition_state=AcquisitionState.downloaded,
            import_state=ImportWorkflowState.imported,
        )
        db.add_all([artist, album, catalog_track, job, release, track])
        await db.commit()
        return album.id, release.id


@pytest.mark.asyncio
async def test_watch_upgrade_post_creates_one_monitoring_record_without_duplicates(
    client: AsyncClient,
) -> None:
    album_id, release_id = await _seed_imported_album()

    first = await client.post(f"/albums/{album_id}/watch-upgrade", follow_redirects=False)
    second = await client.post(f"/albums/{album_id}/watch-upgrade", follow_redirects=False)

    assert first.status_code == 303
    assert second.status_code == 303
    async with get_session_factory()() as db:
        count = await db.scalar(select(func.count(MonitoringRecord.id)))
        record = (
            await db.scalars(
                select(MonitoringRecord).where(MonitoringRecord.release_id == release_id)
            )
        ).one()
    assert count == 1
    assert record.status == MonitoringStatus.active
    assert json.loads(record.history_json or "[]")[0]["baseline_quality"]["codec"] == "mp3"


@pytest.mark.asyncio
async def test_maintenance_upgrades_lists_candidate_found_and_omits_active(
    client: AsyncClient,
) -> None:
    _album_id, release_id = await _seed_imported_album()
    async with get_session_factory()() as db:
        release = await db.get(Release, release_id)
        candidate = ReleaseCandidate(
            release_id=release_id,
            quality_json=json.dumps({"codec": "flac", "lossless": True, "reliability": 1.0}),
            match_score=1.0,
            review_state=MatchReviewState.auto_selected,
            selected=True,
        )
        found = MonitoringRecord(
            release=release,
            status=MonitoringStatus.candidate_found,
            candidate=candidate,
            desired_quality_json=QualityProfile(preferred_codecs=("flac", "mp3")).to_json(),
            history_json=json.dumps([{"baseline_quality": {"codec": "mp3"}}]),
        )
        active = MonitoringRecord(
            release=release,
            status=MonitoringStatus.active,
            desired_quality_json=QualityProfile(preferred_codecs=("flac", "mp3")).to_json(),
        )
        db.add_all([candidate, found, active])
        await db.commit()
        active_id = active.id

    response = await client.get("/maintenance")

    assert response.status_code == 200
    assert "Approve" in response.text
    assert "Candidate flac" in response.text
    assert f"/maintenance/upgrades/approve/{active_id}" not in response.text


@pytest.mark.asyncio
async def test_approve_without_candidate_is_user_facing_error_and_imports_nothing(
    client: AsyncClient, monkeypatch
) -> None:
    _album_id, release_id = await _seed_imported_album()
    async with get_session_factory()() as db:
        record = MonitoringRecord(
            release_id=release_id,
            status=MonitoringStatus.candidate_found,
            desired_quality_json=QualityProfile(preferred_codecs=("flac", "mp3")).to_json(),
        )
        db.add(record)
        await db.commit()
        record_id = record.id
    called = False

    async def fake_execute(*args, **kwargs):
        nonlocal called
        called = True

    monkeypatch.setattr("app.routers.maintenance.execute_quality_upgrade", fake_execute)

    response = await client.post(
        f"/maintenance/upgrades/approve/{record_id}", follow_redirects=True
    )

    assert response.status_code == 200
    assert "No approved upgrade candidate" in response.text
    assert called is False
