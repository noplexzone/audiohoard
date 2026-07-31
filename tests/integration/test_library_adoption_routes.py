from __future__ import annotations

import pytest
from httpx import AsyncClient
from sqlalchemy import select

import app.database as db_module
from app.models.catalog_entities import CatalogAlbum, CatalogArtist
from app.models.import_plan import ImportPlan, LibraryFileState
from app.models.job import Job, JobStatus
from app.models.library_adoption import AdoptionScopeKind, LibraryAdoptionScan
from app.models.release import Release
from app.models.track import Track
from app.models.workflow import AcquisitionState, ImportWorkflowState


def _app(client: AsyncClient):
    return client._transport.app  # type: ignore[attr-defined,no-any-return]


class _Runner:
    def __init__(self) -> None:
        self.wake_count = 0

    def wake(self) -> None:
        self.wake_count += 1


@pytest.mark.asyncio
async def test_full_library_scan_action_persists_and_wakes_runner(client: AsyncClient) -> None:
    runner = _Runner()
    _app(client).state.library_adoption_runner = runner

    response = await client.post("/maintenance/scan", follow_redirects=False)

    assert response.status_code == 303
    assert response.headers["location"].startswith("/maintenance?adoption=queued")
    assert runner.wake_count == 1
    async with db_module.get_session_factory()() as db:
        scan = (await db.scalars(select(LibraryAdoptionScan))).one()
        assert scan.scope_kind == AdoptionScopeKind.full


@pytest.mark.asyncio
async def test_catalog_artist_and_release_scan_actions_are_scoped(client: AsyncClient) -> None:
    runner = _Runner()
    _app(client).state.library_adoption_runner = runner
    async with db_module.get_session_factory()() as db:
        artist = CatalogArtist(name="Scoped Artist")
        album = CatalogAlbum(artist=artist, title="Scoped EP", release_type="ep")
        db.add(artist)
        await db.commit()
        artist_id, album_id = artist.id, album.id

    artist_response = await client.post(
        f"/maintenance/scan/artists/{artist_id}", follow_redirects=False
    )
    album_response = await client.post(
        f"/maintenance/scan/albums/{album_id}", follow_redirects=False
    )

    assert artist_response.status_code == 303
    assert artist_response.headers["location"] == f"/artists/catalog/{artist_id}?scan=queued"
    assert album_response.status_code == 303
    assert album_response.headers["location"] == f"/albums/{album_id}?scan=queued"
    assert runner.wake_count == 2
    async with db_module.get_session_factory()() as db:
        scopes = list(
            (
                await db.scalars(
                    select(LibraryAdoptionScan.scope_kind).order_by(LibraryAdoptionScan.id)
                )
            ).all()
        )
        assert scopes == [AdoptionScopeKind.catalog_artist, AdoptionScopeKind.catalog_album]


@pytest.mark.asyncio
async def test_unknown_scoped_scan_does_not_broaden(client: AsyncClient) -> None:
    _app(client).state.library_adoption_runner = _Runner()

    response = await client.post("/maintenance/scan/artists/999999", follow_redirects=False)

    assert response.status_code == 404
    async with db_module.get_session_factory()() as db:
        assert (await db.scalars(select(LibraryAdoptionScan))).all() == []


@pytest.mark.asyncio
async def test_dashboard_and_catalog_templates_expose_exact_scan_actions(
    client: AsyncClient,
) -> None:
    dashboard = await client.get("/")

    assert dashboard.status_code == 200
    assert "Scan full library" in dashboard.text
    assert 'action="/maintenance/scan"' in dashboard.text


@pytest.mark.asyncio
async def test_track_detail_uses_destination_as_library_path(client: AsyncClient) -> None:
    async with db_module.get_session_factory()() as db:
        job = Job(source="slskd", query="path semantics", status=JobStatus.done)
        release = Release(job=job, source="slskd", title="Album", album_artist="Artist")
        track = Track(
            job=job,
            release=release,
            source="slskd",
            title="Song",
            artist="Artist",
            album="Album",
            source_path="/staging/original.flac",
            staging_path="/staging/original.flac",
            acquisition_state=AcquisitionState.downloaded,
            import_state=ImportWorkflowState.imported,
            file_size_bytes=123,
        )
        plan = ImportPlan(
            release=release,
            track=track,
            source_path="/staging/original.flac",
            staging_path="/staging/original.flac",
            destination_path="/music/Artist/Album/01 - Song.flac",
            status=ImportWorkflowState.imported,
            file_state=LibraryFileState.present,
        )
        db.add_all([job, plan])
        await db.commit()
        track_id = track.id

    response = await client.get(f"/tracks/{track_id}/ui")

    assert response.status_code == 200
    assert "Library path" in response.text
    assert "/music/Artist/Album/01 - Song.flac" in response.text
    assert "Original source" in response.text
    assert "/staging/original.flac" in response.text
