from datetime import UTC, datetime, timedelta
from pathlib import Path

from httpx import AsyncClient

from app.database import get_session_factory
from app.models.catalog_entities import CatalogAlbum, CatalogAlbumTrack, CatalogArtist
from app.models.job import Job, JobStatus
from app.models.release import Release
from app.models.staging_review import StagingReviewItem
from app.models.track import Track
from app.models.workflow import AcquisitionState, ImportWorkflowState, ReviewDecision
from app.services.reference_audio import ReferenceAudio


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
        return ReferenceAudio(
            url="https://cdn.example.test/reference.mp3",
            provider="deezer",
            provider_track_id="42",
            match_method="exact_track_id",
            cached=False,
        )

    monkeypatch.setattr("app.services.staging.resolve_reference_audio", reference)
    response = await client.get("/review")

    assert response.status_code == 200
    assert f'src="/staging/audio/{item_id}"' in response.text
    assert 'src="https://cdn.example.test/reference.mp3"' in response.text
    assert 'class="tag-comparison-list"' in response.text
    assert 'class="tag-comparison-row tag-mismatch"' in response.text
    assert 'class="tag-comparison-value"' in response.text
    assert 'class="tag-diff-table"' not in response.text
    assert 'class="table-wrap"' not in response.text
    assert "As tagged" in response.text
    assert "Catalog" in response.text
    assert 'data-page-module="review-deck"' in response.text
    assert f'data-alignment-url="/staging/review/{item_id}/alignment"' in response.text
    assert 'data-reference-source="deezer"' in response.text
    assert 'data-reference-url="https://cdn.example.test/reference.mp3"' in response.text
    assert "Exact Deezer track match" in response.text
    assert "1 track remaining" in response.text
    assert "Track 1 of" not in response.text
    assert "data-match-section" in response.text
    assert "data-ab-toggle" in response.text
    assert "data-alignment-status" in response.text
    assert 'data-alignment-nudge="-5"' in response.text
    assert 'data-alignment-nudge="1"' in response.text
    assert "approximately 30-second mid-track clip" not in response.text
    assert "Acquisition source" in response.text
    assert "Soulseek (slskd)" in response.text
    assert "Username" in response.text
    assert "allbren" in response.text
    assert "Remote folder" in response.text
    assert r"downloads\LISA\Alter Ego" in response.text
    assert "Original filename" in response.text
    assert "01 Original Name.flac" in response.text
    assert "data-swipe-surface" in response.text
    assert "data-skip-button" not in response.text
    assert "N next" not in response.text
    assert "Swipe right to approve · Swipe left to deny" in response.text
    assert "Jump downloaded file to midpoint" not in response.text
    assert "data-jump-midpoint" not in response.text
    assert '<details class="review-secondary-details">' in response.text
    assert "Tags &amp; file details" in response.text
    assert response.text.index("review-audio-comparison") < response.text.index(
        "review-deck-actions"
    )
    assert response.text.index("review-deck-actions") < response.text.index(
        "review-secondary-details"
    )


async def test_review_skip_advances_without_deciding_and_wraps(
    client: AsyncClient, test_settings, monkeypatch
) -> None:
    first_id = await _seed_review(test_settings.staging_root)
    second_id = await _seed_review(test_settings.staging_root)

    async def no_reference(*args, **kwargs):
        return None

    monkeypatch.setattr("app.services.staging.resolve_reference_audio", no_reference)

    # Queue traversal deliberately follows stable insertion IDs, even if timestamps diverge.
    factory = get_session_factory()
    async with factory() as db:
        first = await db.get(StagingReviewItem, first_id)
        second_item = await db.get(StagingReviewItem, second_id)
        assert first is not None and second_item is not None
        first.created_at = datetime.now(UTC) + timedelta(days=1)
        second_item.created_at = datetime.now(UTC)
        await db.commit()

    first_page = await client.get("/review")
    assert f'src="/staging/audio/{first_id}"' in first_page.text
    second = await client.get(f"/review?after={first_id}")
    assert second.status_code == 200
    assert f'src="/staging/audio/{second_id}"' in second.text
    assert f'href="/review?after={second_id}"' in second.text
    assert "data-skip-button" in second.text
    assert "N next" in second.text

    wrapped = await client.get(f"/review?after={second_id}")
    assert wrapped.status_code == 200
    assert f'src="/staging/audio/{first_id}"' in wrapped.text

    factory = get_session_factory()
    async with factory() as db:
        first = await db.get(StagingReviewItem, first_id)
        second_item = await db.get(StagingReviewItem, second_id)
        assert first is not None and second_item is not None
        assert first.review_state == ReviewDecision.pending
        assert second_item.review_state == ReviewDecision.pending


async def test_review_skip_cursor_rejects_values_outside_sqlite_integer_range(
    client: AsyncClient, test_settings
) -> None:
    await _seed_review(test_settings.staging_root)

    response = await client.get("/review?after=9223372036854775808")

    assert response.status_code == 422


async def test_review_template_renders_exact_provenance_and_no_reference_state(
    client: AsyncClient, test_settings, monkeypatch
) -> None:
    await _seed_review(test_settings.staging_root)
    current = {
        "value": ReferenceAudio(
            url="https://example.test/deezer.mp3",
            provider="deezer",
            provider_track_id="42",
            match_method="exact_track_id",
            cached=False,
        )
    }

    async def reference(*args, **kwargs):
        return current["value"]

    monkeypatch.setattr("app.services.staging.resolve_reference_audio", reference)
    exact_track = await client.get("/review")
    assert "Exact Deezer track match" in exact_track.text
    assert "Provider track ID" in exact_track.text
    assert ">42<" in exact_track.text
    assert "iTunes" not in exact_track.text

    current["value"] = ReferenceAudio(
        url="https://example.test/deezer-position.mp3",
        provider="deezer",
        provider_track_id="99",
        match_method="exact_album_position",
        cached=True,
    )
    exact_position = await client.get("/review")
    assert "Exact Deezer album-position match" in exact_position.text
    assert "Cached exact reference" in exact_position.text

    current["value"] = None
    missing = await client.get("/review")
    assert "No verified reference available" in missing.text
    assert "No verified comparison clip is available" in missing.text
    assert "file metadata, fingerprint evidence, and manual listening" in missing.text
    assert 'src="/staging/audio/' in missing.text
    assert "Approve" in missing.text and "Deny" in missing.text


async def test_review_empty_queue_shows_caught_up(client: AsyncClient) -> None:
    response = await client.get("/review")

    assert response.status_code == 200
    assert "All caught up." in response.text
