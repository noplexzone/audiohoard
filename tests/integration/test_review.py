from pathlib import Path

from httpx import AsyncClient

from app.database import get_session_factory
from app.models.catalog_entities import CatalogAlbum, CatalogAlbumTrack, CatalogArtist
from app.models.job import Job, JobStatus
from app.models.release import Release
from app.models.staging_review import StagingReviewItem
from app.models.track import Track
from app.models.workflow import AcquisitionState, ImportWorkflowState, ReviewDecision


async def _seed_review(staging_root: Path) -> int:
    staged = staging_root / "slskd" / "release-1" / "01 Download.flac"
    staged.parent.mkdir(parents=True, exist_ok=True)
    staged.write_bytes(b"not-real-audio")
    factory = get_session_factory()
    async with factory() as db:
        artist = CatalogArtist(name="Deck Artist")
        album = CatalogAlbum(
            artist=artist,
            title="Deck Album",
            year="2026",
            artwork_url="https://example.test/cover.jpg",
            track_count=1,
        )
        catalog_track = CatalogAlbumTrack(
            album=album,
            position=1,
            disc=1,
            title="Deck Track",
            duration_sec=200,
        )
        job = Job(source="slskd", query="Deck Artist Deck Track", status=JobStatus.done)
        release = Release(
            job=job,
            source="slskd",
            title="Deck Album",
            album_artist="Deck Artist",
            track_count=1,
            import_state=ImportWorkflowState.needs_review,
        )
        track = Track(
            job=job,
            release=release,
            catalog_album=album,
            catalog_track=catalog_track,
            source="slskd",
            title="Deck Track",
            artist="Deck Artist",
            album="Deck Album",
            album_artist="Deck Artist",
            track_no=1,
            disc=1,
            duration_sec=200,
            staging_path=str(staged),
            source_path=str(staged),
            acquisition_state=AcquisitionState.downloaded,
            acquisition_provenance_json=(
                '{"source":"slskd","username":"allbren",'
                '"filename":"downloads\\\\LISA\\\\Alter Ego\\\\01 Original Name.flac"}'
            ),
        )
        item = StagingReviewItem(
            track=track,
            release=release,
            expected_title="Deck Track",
            expected_recording_mbid="11111111-1111-1111-1111-111111111111",
            observed_acoustid_mbids_json='["22222222-2222-2222-2222-222222222222"]',
            fingerprint_duration_sec=203,
            acoustid_score=0.82,
            review_state=ReviewDecision.pending,
        )
        db.add_all([artist, album, catalog_track, job, release, track, item])
        await db.commit()
        return item.id


async def test_review_renders_front_card_audio_and_tag_diff(
    client: AsyncClient, test_settings, monkeypatch
) -> None:
    item_id = await _seed_review(test_settings.staging_root)

    async def reference(*args, **kwargs):
        return {"url": "https://cdn.example.test/reference.mp3", "source": "deezer"}

    monkeypatch.setattr("app.services.staging.resolve_reference_audio", reference)
    response = await client.get("/review")

    assert response.status_code == 200
    assert f'src="/staging/audio/{item_id}"' in response.text
    assert 'src="https://cdn.example.test/reference.mp3"' in response.text
    assert 'class="tag-diff-table"' in response.text
    assert 'data-page-module="review-deck"' in response.text
    assert "Acquisition source" in response.text
    assert "Soulseek (slskd)" in response.text
    assert "Username" in response.text
    assert "allbren" in response.text
    assert "Remote folder" in response.text
    assert r"downloads\LISA\Alter Ego" in response.text
    assert "Original filename" in response.text
    assert "01 Original Name.flac" in response.text
    assert "data-swipe-surface" in response.text
    assert "Swipe right to approve · Swipe left to deny" in response.text


async def test_review_reference_badge_reflects_resolver_source(
    client: AsyncClient, test_settings, monkeypatch
) -> None:
    await _seed_review(test_settings.staging_root)
    current = {"value": {"url": "https://example.test/deezer.mp3", "source": "deezer"}}

    async def reference(*args, **kwargs):
        return current["value"]

    monkeypatch.setattr("app.services.staging.resolve_reference_audio", reference)
    deezer = await client.get("/review")
    assert ">Deezer<" in deezer.text

    current["value"] = {"url": "https://example.test/itunes.m4a", "source": "itunes"}
    itunes = await client.get("/review")
    assert ">iTunes (fallback)<" in itunes.text

    current["value"] = None
    missing = await client.get("/review")
    assert "No reference available" in missing.text


async def test_review_empty_queue_shows_caught_up(client: AsyncClient) -> None:
    response = await client.get("/review")

    assert response.status_code == 200
    assert "All caught up." in response.text
