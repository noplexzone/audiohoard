from __future__ import annotations

from pathlib import Path

from mutagen.id3 import ID3
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import Settings
from app.jobs import runner
from app.models.catalog_entities import CatalogAlbum, CatalogAlbumTrack, CatalogArtist
from app.models.import_plan import ImportPlan
from app.models.job import Job, JobStatus
from app.models.release import Release
from app.models.staging_review import StagingReviewItem
from app.models.track import Track
from app.models.workflow import (
    AcoustIDVerificationState,
    AcquisitionState,
    ImportWorkflowState,
    ReviewDecision,
)
from app.schemas.search import SearchResult
from app.services import library_import
from app.services.library_import import CanonicalArtwork
from app.sources.base import CapabilityState

_EXPECTED_MBID = "11111111-1111-1111-1111-111111111111"
_OTHER_MBID = "22222222-2222-2222-2222-222222222222"


async def _run_mocked_slskd_pipeline(
    db: AsyncSession,
    settings: Settings,
    monkeypatch,
    tmp_path: Path,
    *,
    observed_mbid: str,
) -> tuple[Job, Release, Track, Path]:
    staging_root = tmp_path / "staging"
    library_root = tmp_path / "library"
    staged_file = staging_root / "peer" / "Album" / "01 Song.mp3"
    staged_file.parent.mkdir(parents=True)
    staged_file.write_bytes(b"mock audio payload")
    cfg = settings.model_copy(
        update={
            "staging_root": staging_root,
            "library_root": library_root,
            "slskd_url": "http://slskd.test",
            "slskd_api_key": "test-key",
            "acoustid_api_key": "test-acoustid-key",
        }
    )

    artist = CatalogArtist(name="Artist")
    album = CatalogAlbum(
        artist=artist,
        title="Album",
        year="2026",
        track_count=1,
        mbid="33333333-3333-3333-3333-333333333333",
        artwork_url="https://cdn-images.dzcdn.net/images/cover/test.jpg",
    )
    catalog_track = CatalogAlbumTrack(
        album=album,
        position=1,
        disc=1,
        title="Song",
        duration_sec=180,
        recording_mbid=_EXPECTED_MBID,
    )
    result = SearchResult(
        source="slskd",
        title="01 Song",
        artist="Artist",
        album="Album",
        format="mp3",
        size_bytes=1234,
        metadata={
            "username": "peer",
            "filename": "Album/01 Song.mp3",
            "track_no": 1,
            "disc": 1,
        },
    )
    job = Job(
        source="slskd",
        query="Artist Album",
        status=JobStatus.pending,
        catalog_album=album,
        selected_result_json=result.model_dump_json(),
    )
    db.add_all([artist, album, catalog_track, job])
    await db.flush()

    class FakeSlskdAdapter:
        def __init__(self, base_url: str, api_key: str) -> None:
            assert base_url == "http://slskd.test"
            assert api_key == "test-key"

        async def enqueue(self, username: str, filename: str, size: int | None = None) -> str:
            assert (username, filename, size) == ("peer", "Album/01 Song.mp3", 1234)
            return "peer:Album/01 Song.mp3"

        async def status(self, transfer_id: str) -> CapabilityState:
            assert transfer_id == "peer:Album/01 Song.mp3"
            return CapabilityState(
                True,
                "Completed, Succeeded",
                {"localPath": str(staged_file)},
            )

        async def cancel(self, username: str, filename: str) -> None:
            raise AssertionError("completed transfer must not be cancelled")

    async def fake_fingerprint(path: Path) -> tuple[int, str]:
        assert path == staged_file
        return 180, "mock-fingerprint"

    async def fake_acoustid_lookup(
        duration: int, fingerprint: str, api_key: str
    ) -> list[dict[str, object]]:
        assert (duration, fingerprint, api_key) == (
            180,
            "mock-fingerprint",
            "test-acoustid-key",
        )
        return [{"score": 0.99, "recordings": [{"id": observed_mbid}]}]

    async def no_deezer(track: Track, cfg: Settings) -> None:
        return None

    async def fake_artwork(url: str | None) -> CanonicalArtwork | None:
        assert url == "https://cdn-images.dzcdn.net/images/cover/test.jpg"
        return CanonicalArtwork(b"\xff\xd8\xffmock-jpeg", "image/jpeg")

    monkeypatch.setattr(runner, "SlskdAdapter", FakeSlskdAdapter)
    monkeypatch.setattr(runner, "fingerprint_file", fake_fingerprint)
    monkeypatch.setattr(runner, "_lookup_acoustid_raw", fake_acoustid_lookup)
    monkeypatch.setattr(runner, "_enrich_deezer", no_deezer)
    monkeypatch.setattr(library_import, "_fetch_canonical_artwork", fake_artwork)

    await runner.run_job(job.id, db, cfg)
    release = (await db.scalars(select(Release).where(Release.job_id == job.id))).one()
    track = (await db.scalars(select(Track).where(Track.release_id == release.id))).one()
    return job, release, track, staged_file


async def test_mocked_slskd_completed_transfer_is_staged_verified_and_auto_imported(
    db_session: AsyncSession,
    test_settings: Settings,
    monkeypatch,
    tmp_path: Path,
) -> None:
    job, release, track, staged_file = await _run_mocked_slskd_pipeline(
        db_session,
        test_settings,
        monkeypatch,
        tmp_path,
        observed_mbid=_EXPECTED_MBID,
    )

    assert job.status == JobStatus.done
    assert track.acquisition_state == AcquisitionState.downloaded
    assert track.acoustid_verification_state == AcoustIDVerificationState.verified
    assert release.import_state == ImportWorkflowState.imported
    assert track.import_state == ImportWorkflowState.imported
    plan = await db_session.scalar(select(ImportPlan).where(ImportPlan.release_id == release.id))
    assert plan is not None
    assert plan.status == ImportWorkflowState.imported
    destination = Path(plan.destination_path)
    assert destination.is_file()  # noqa: ASYNC240
    assert destination.is_relative_to(tmp_path / "library")
    tags = ID3(destination)
    assert str(tags["TIT2"]) == "Song"
    assert str(tags["TPE1"]) == "Artist"
    assert len(tags.getall("APIC")) == 1
    assert staged_file.exists(), "source cleanup must wait for transaction commit"


async def test_mocked_slskd_completed_transfer_with_acoustid_mismatch_enters_review(
    db_session: AsyncSession,
    test_settings: Settings,
    monkeypatch,
    tmp_path: Path,
) -> None:
    job, release, track, staged_file = await _run_mocked_slskd_pipeline(
        db_session,
        test_settings,
        monkeypatch,
        tmp_path,
        observed_mbid=_OTHER_MBID,
    )

    assert job.status == JobStatus.done
    assert track.acquisition_state == AcquisitionState.downloaded
    assert track.acoustid_verification_state == AcoustIDVerificationState.mismatch
    assert release.import_state == ImportWorkflowState.needs_review
    assert release.error_detail == "AcoustID mismatch on track 1"
    review = await db_session.scalar(
        select(StagingReviewItem).where(StagingReviewItem.release_id == release.id)
    )
    assert review is not None
    assert review.review_state == ReviewDecision.pending
    assert staged_file.exists()
    assert not list((tmp_path / "library").rglob("*.mp3"))
