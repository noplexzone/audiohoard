from __future__ import annotations

import pytest
import pytest_asyncio
from httpx import AsyncClient

import app.database as db_module
import app.services.library_import as library_import_module
from app.models.catalog_entities import CatalogAlbum, CatalogAlbumTrack, CatalogArtist
from app.models.import_plan import ImportPlan
from app.models.job import Job, JobStatus
from app.models.release import Release
from app.models.track import Track
from app.models.workflow import ImportWorkflowState
from app.services.library_import import AlbumRetagResult, CanonicalArtwork, ImportExecutionError
from app.services.quality_upgrade import QualityDuplicateResult


@pytest_asyncio.fixture
async def album_id(client: AsyncClient) -> int:
    factory = db_module.get_session_factory()
    async with factory() as session:
        artist = CatalogArtist(name="Artist")
        album = CatalogAlbum(title="Single", year="2024", release_type="single", track_count=1)
        album.tracks.append(CatalogAlbumTrack(disc=1, position=1, title="Song"))
        artist.albums.append(album)
        session.add(artist)
        await session.commit()
        return album.id


async def test_album_page_offers_confirmed_manual_metadata_repair(
    client: AsyncClient, album_id: int
) -> None:
    response = await client.get(f"/albums/{album_id}")
    assert response.status_code == 200
    assert f'action="/albums/{album_id}/retag"' in response.text
    assert "Repair metadata" in response.text
    assert 'data-confirm="Repair metadata for every downloaded file' in response.text
    assert f'action="/albums/{album_id}/quality-deduplicate"' in response.text
    assert "Clean quality duplicates" in response.text
    assert "/static/js/album.js" in response.text


async def test_manual_metadata_repair_redirects_with_success_notice(
    client: AsyncClient, album_id: int, monkeypatch
) -> None:
    async def fake_retag(db, requested_album_id, *, library_root, tag_writer=None):
        assert requested_album_id == album_id
        return AlbumRetagResult(files_retagged=3, folder=library_root / "Artist" / "Single")

    monkeypatch.setattr("app.routers.catalog.retag_catalog_album", fake_retag)
    csrf_token = client.headers.pop("X-CSRF-Token")
    response = await client.post(
        f"/albums/{album_id}/retag",
        data={"csrf_token": csrf_token},
        follow_redirects=False,
    )
    assert response.status_code == 303
    assert response.headers["location"] == f"/albums/{album_id}?retag=ok&count=3"

    page = await client.get(response.headers["location"])
    assert "Retagged 3 audio files from Audiohoard metadata." in page.text


async def test_manual_metadata_repair_redirects_with_safe_error_notice(
    client: AsyncClient, album_id: int, monkeypatch
) -> None:
    async def fail_retag(db, requested_album_id, *, library_root, tag_writer=None):
        raise ImportExecutionError("album folder contains untracked audio")

    monkeypatch.setattr("app.routers.catalog.retag_catalog_album", fail_retag)
    response = await client.post(f"/albums/{album_id}/retag", follow_redirects=False)
    assert response.status_code == 303
    assert response.headers["location"].startswith(f"/albums/{album_id}?retag=error&detail=")

    page = await client.get(response.headers["location"])
    assert "Metadata repair failed: album folder contains untracked audio" in page.text


async def test_callback_registration_failure_rolls_back_renamed_plan_after_request(
    client: AsyncClient, test_settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    factory = db_module.get_session_factory()
    async with factory() as session:
        artist = CatalogArtist(name="Morgan Wallen", mbid="route-artist-mbid")
        album = CatalogAlbum(
            title="I’m The Problem", year="2025", release_type="album", track_count=2
        )
        artist.albums.append(album)
        album.tracks.extend(
            [
                CatalogAlbumTrack(disc=1, position=1, title="I'm the Problem"),
                CatalogAlbumTrack(disc=3, position=9, title="LA Night", recording_mbid="la"),
            ]
        )
        job = Job(source="slskd", query="morgan", status=JobStatus.done)
        release = Release(job=job, source="slskd", title=album.title, album_artist=artist.name)
        session.add_all([artist, job, release])
        await session.flush()
        folder = test_settings.library_root / artist.name / f"{album.title} ({album.year})"
        folder.mkdir(parents=True)
        original_path = folder / "09 - LA Night.flac"
        stream_info = (
            (4096).to_bytes(2, "big")
            + (4096).to_bytes(2, "big")
            + (0).to_bytes(3, "big")
            + (0).to_bytes(3, "big")
            + ((44100 << 44) | (15 << 36)).to_bytes(8, "big")
            + bytes(16)
        )
        original_path.write_bytes(b"fLaC" + bytes([0x80, 0, 0, 34]) + stream_info)
        original_bytes = original_path.read_bytes()
        track = Track(
            job=job,
            release=release,
            source="slskd",
            title="LA Night",
            album=album.title,
            album_artist=artist.name,
            catalog_album_id=album.id,
            catalog_track_id=album.tracks[1].id,
            disc=3,
            track_no=9,
            import_state=ImportWorkflowState.imported,
        )
        plan = ImportPlan(
            release=release,
            track=track,
            source_path=str(original_path),
            destination_path=str(original_path),
            status=ImportWorkflowState.imported,
        )
        session.add_all([track, plan])
        await session.commit()
        album_id = album.id
        plan_id = plan.id

    async def no_artwork(_url: str | None) -> CanonicalArtwork | None:
        return None

    def fail_registration(*_args, **_kwargs) -> None:
        raise RuntimeError("callback registration failed")

    monkeypatch.setattr(library_import_module, "_fetch_canonical_artwork", no_artwork)
    monkeypatch.setattr(library_import_module, "register_transaction_callbacks", fail_registration)
    response = await client.post(f"/albums/{album_id}/retag", follow_redirects=False)

    canonical_path = folder / "3-09 - LA Night.flac"
    assert response.status_code == 303
    assert "retag=error" in response.headers["location"]
    assert original_path.read_bytes() == original_bytes
    assert not canonical_path.exists()
    assert not list(folder.glob(".*.retag-backup"))
    async with factory() as session:
        durable_plan = await session.get(ImportPlan, plan_id)
        assert durable_plan is not None
        assert durable_plan.destination_path == str(original_path)


async def test_quality_duplicate_cleanup_redirects_with_count(
    client: AsyncClient, album_id: int, monkeypatch
) -> None:
    async def fake_cleanup(
        db, requested_album_id, *, library_root, quality_profile, defer_filesystem_delete
    ):
        assert requested_album_id == album_id
        assert defer_filesystem_delete is True
        return QualityDuplicateResult(deleted_files=2, review_required=1)

    monkeypatch.setattr("app.routers.catalog.reconcile_album_quality_duplicates", fake_cleanup)
    response = await client.post(f"/albums/{album_id}/quality-deduplicate", follow_redirects=False)

    assert response.status_code == 303
    assert response.headers["location"] == f"/albums/{album_id}?quality=ok&deleted=2&review=1"

    page = await client.get(response.headers["location"])
    assert "Removed 2 lower-quality duplicates." in page.text
    assert "1 ambiguous duplicate file(s) still need review." in page.text
