from __future__ import annotations

import pytest_asyncio
from httpx import AsyncClient

import app.database as db_module
from app.models.catalog_entities import CatalogAlbum, CatalogAlbumTrack, CatalogArtist
from app.services.library_import import AlbumRetagResult, ImportExecutionError
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
