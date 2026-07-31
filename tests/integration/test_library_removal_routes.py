from __future__ import annotations

from pathlib import Path

from httpx import AsyncClient

from app.config import Settings
from app.database import get_session_factory
from app.models.catalog_entities import CatalogAlbum, CatalogAlbumTrack, CatalogArtist
from app.models.import_plan import ImportPlan, LibraryFileState
from app.models.job import Job, JobStatus
from app.models.release import Release
from app.models.track import Track
from app.models.workflow import AcquisitionState, ImportWorkflowState


async def _seed_imported_album(
    settings: Settings, *, names: tuple[str, ...]
) -> tuple[int, int, list[int], list[Path]]:
    root = settings.library_root
    artist = CatalogArtist(name="Removal Artist")
    album = CatalogAlbum(
        artist=artist,
        title="Removal Album",
        track_count=len(names),
        in_library=True,
    )
    job = Job(source="slskd", query="removal", status=JobStatus.done, catalog_album=album)
    release = Release(
        job=job,
        source="slskd",
        title=album.title,
        album_artist=artist.name,
        track_count=len(names),
        import_state=ImportWorkflowState.imported,
    )
    tracks: list[Track] = []
    paths: list[Path] = []
    plans: list[ImportPlan] = []
    for position, name in enumerate(names, 1):
        catalog_track = CatalogAlbumTrack(
            album=album,
            position=position,
            disc=1,
            title=name,
        )
        path = root / artist.name / album.title / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(name.encode())
        track = Track(
            job=job,
            release=release,
            catalog_album=album,
            catalog_track=catalog_track,
            source="slskd",
            title=name,
            acquisition_state=AcquisitionState.downloaded,
            import_state=ImportWorkflowState.imported,
            file_size_bytes=path.stat().st_size,
        )
        tracks.append(track)
        paths.append(path)
        plans.append(
            ImportPlan(
                release=release,
                track=track,
                source_path=str(path),
                destination_path=str(path),
                status=ImportWorkflowState.imported,
                file_state=LibraryFileState.present,
            )
        )
    async with get_session_factory()() as db:
        db.add_all([artist, album, job, release, *tracks, *plans])
        await db.commit()
        return album.id, release.id, [track.id for track in tracks], paths


async def test_track_delete_requires_auth(
    unauthenticated_client: AsyncClient,
    test_settings: Settings,
) -> None:
    _, _, track_ids, _ = await _seed_imported_album(test_settings, names=("auth.mp3",))
    response = await unauthenticated_client.post(
        f"/library/tracks/{track_ids[0]}/delete",
        json={"confirmation": "delete"},
    )
    assert response.status_code == 401


async def test_track_delete_requires_csrf_and_confirmation(
    client: AsyncClient,
    test_settings: Settings,
) -> None:
    _, _, track_ids, paths = await _seed_imported_album(test_settings, names=("one.mp3",))
    endpoint = f"/library/tracks/{track_ids[0]}/delete"

    csrf = client.headers.pop("X-CSRF-Token")
    try:
        forbidden = await client.post(endpoint, json={"confirmation": "delete"})
    finally:
        client.headers["X-CSRF-Token"] = csrf
    assert forbidden.status_code == 403

    unconfirmed = await client.post(endpoint, json={"confirmation": "no"})
    assert unconfirmed.status_code == 422
    assert paths[0].exists()


async def test_track_delete_returns_json_and_is_idempotent(
    client: AsyncClient, test_settings: Settings
) -> None:
    _, _, track_ids, paths = await _seed_imported_album(test_settings, names=("single.mp3",))
    endpoint = f"/library/tracks/{track_ids[0]}/delete"
    first = await client.post(
        endpoint,
        json={"confirmation": "delete"},
        headers={"Accept": "application/json"},
    )
    assert first.status_code == 200
    assert first.json()["deleted_files"] == 1
    assert first.json()["already_removed"] is False
    assert not paths[0].exists()

    second = await client.post(
        endpoint,
        json={"confirmation": "delete"},
        headers={"Accept": "application/json"},
    )
    assert second.status_code == 200
    assert second.json()["deleted_files"] == 0
    assert second.json()["already_removed"] is True


async def test_imported_release_delete_removes_every_file_and_returns_json(
    client: AsyncClient, test_settings: Settings
) -> None:
    _, release_id, track_ids, paths = await _seed_imported_album(
        test_settings,
        names=("legacy-one.mp3", "legacy-two.mp3"),
    )

    response = await client.post(
        "/library/releases/delete",
        data={
            "confirmation": "delete",
            "release_id": str(release_id),
            "artist_name": "Removal Artist",
            "album_title": "Removal Album",
            "year": "",
        },
        headers={"Accept": "application/json", "X-Requested-With": "fetch"},
    )

    assert response.status_code == 200
    assert response.json()["deleted_files"] == 2
    assert response.json()["track_ids"] == track_ids
    assert not any(path.exists() for path in paths)


async def test_imported_release_delete_requires_confirmation(
    client: AsyncClient, test_settings: Settings
) -> None:
    _, release_id, _, paths = await _seed_imported_album(
        test_settings, names=("unconfirmed-release.mp3",)
    )

    response = await client.post(
        "/library/releases/delete",
        data={
            "confirmation": "no",
            "release_id": str(release_id),
            "artist_name": "Removal Artist",
            "album_title": "Removal Album",
        },
    )

    assert response.status_code == 422
    assert paths[0].exists()


async def test_album_delete_removes_every_file_and_redirects(
    client: AsyncClient, test_settings: Settings
) -> None:
    album_id, _, _, paths = await _seed_imported_album(
        test_settings,
        names=("one.mp3", "two.mp3"),
    )
    response = await client.post(
        f"/library/albums/{album_id}/delete",
        data={"confirmation": "delete"},
        headers={"Accept": "text/html"},
    )
    assert response.status_code == 303
    assert response.headers["location"] == f"/albums/{album_id}?removed=1"
    assert not any(path.exists() for path in paths)
