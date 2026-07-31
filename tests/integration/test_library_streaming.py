from __future__ import annotations

import asyncio
import os
from pathlib import Path

import pytest
from httpx import AsyncClient

from app.config import Settings
from app.database import get_session_factory
from app.models.import_plan import ImportPlan, LibraryFileState
from app.models.job import Job, JobStatus
from app.models.release import Release
from app.models.track import Track
from app.models.workflow import AcquisitionState, ImportWorkflowState


async def _imported_track(
    settings: Settings,
    *,
    name: str = "song.mp3",
    content: bytes = b"0123456789abcdef",
    state: LibraryFileState = LibraryFileState.present,
) -> tuple[int, Path]:
    path = settings.library_root / "Artist" / "Album" / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    async with get_session_factory()() as db:
        job = Job(source="slskd", query=name, status=JobStatus.done)
        release = Release(
            job=job,
            source="slskd",
            title="Album",
            album_artist="Artist",
            import_state=ImportWorkflowState.imported,
        )
        track = Track(
            job=job,
            release=release,
            source="slskd",
            title=name,
            acquisition_state=AcquisitionState.downloaded,
            import_state=ImportWorkflowState.imported,
        )
        plan = ImportPlan(
            release=release,
            track=track,
            source_path=str(path),
            destination_path=str(path),
            status=ImportWorkflowState.imported,
            file_state=state,
        )
        db.add_all([job, release, track, plan])
        await db.commit()
        return track.id, path


async def test_library_audio_requires_auth_and_serves_get_head_and_ranges(
    client: AsyncClient, test_settings: Settings
) -> None:
    track_id, _ = await _imported_track(test_settings)

    full = await client.get(f"/tracks/{track_id}/audio")
    assert full.status_code == 200
    assert full.content == b"0123456789abcdef"
    assert full.headers["content-type"] == "audio/mpeg"
    assert full.headers["content-length"] == "16"
    assert full.headers["accept-ranges"] == "bytes"
    assert full.headers["cache-control"] == "private, no-cache"
    assert full.headers["x-content-type-options"] == "nosniff"

    for value, expected, content_range in (
        ("bytes=2-5", b"2345", "bytes 2-5/16"),
        ("bytes=12-", b"cdef", "bytes 12-15/16"),
        ("bytes=-4", b"cdef", "bytes 12-15/16"),
    ):
        response = await client.get(f"/tracks/{track_id}/audio", headers={"Range": value})
        assert response.status_code == 206
        assert response.content == expected
        assert response.headers["content-range"] == content_range
        assert response.headers["content-length"] == str(len(expected))

    head = await client.head(f"/tracks/{track_id}/audio")
    assert head.status_code == 200
    assert head.content == b""
    assert head.headers["content-length"] == "16"

    session = client.cookies.get("session")
    client.cookies.delete("session")
    denied = await client.get(f"/tracks/{track_id}/audio", follow_redirects=False)
    assert denied.status_code in {302, 303, 307, 401}
    if session:
        client.cookies.set("session", session)


@pytest.mark.parametrize("range_value", ["bytes=nope", "bytes=1-2,4-5", "bytes=99-"])
async def test_library_audio_returns_strict_416(
    client: AsyncClient, test_settings: Settings, range_value: str
) -> None:
    track_id, _ = await _imported_track(test_settings, name=range_value.replace("/", "_") + ".mp3")
    response = await client.get(f"/tracks/{track_id}/audio", headers={"Range": range_value})
    assert response.status_code == 416
    assert response.headers["content-range"] == "bytes */16"
    assert response.headers["content-length"] == "0"
    assert response.content == b""


async def test_only_present_imported_plans_are_streamable_and_newest_unsafe_falls_back(
    client: AsyncClient, test_settings: Settings
) -> None:
    missing_id, _ = await _imported_track(
        test_settings, name="not-present.mp3", state=LibraryFileState.missing
    )
    assert (await client.get(f"/tracks/{missing_id}/audio")).status_code == 404

    track_id, safe = await _imported_track(test_settings, name="safe.mp3", content=b"safe")
    outside = test_settings.library_root.parent / "outside.mp3"
    outside.write_bytes(b"outside")
    async with get_session_factory()() as db:
        track = await db.get(Track, track_id)
        assert track is not None and track.release_id is not None
        db.add(
            ImportPlan(
                release_id=track.release_id,
                track_id=track.id,
                source_path=str(outside),
                destination_path=str(outside),
                status=ImportWorkflowState.imported,
                file_state=LibraryFileState.present,
            )
        )
        await db.commit()
    response = await client.get(f"/tracks/{track_id}/audio")
    assert response.status_code == 200
    assert response.content == safe.read_bytes()
    assert b"outside" not in response.content


async def test_concurrent_mp3_transcodes_publish_one_cache_entry(
    client: AsyncClient, test_settings: Settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    from app.services import media_streaming

    test_settings.artwork_cache_root = test_settings.library_root.parent / "data" / "artwork"
    track_id, _ = await _imported_track(test_settings, name="source.flac", content=b"source")
    calls = 0

    async def fake_run_ffmpeg(source_fd: int, destination: Path) -> None:
        nonlocal calls
        calls += 1
        assert os.pread(source_fd, 6, 0) == b"source"
        await asyncio.sleep(0.05)
        await asyncio.to_thread(destination.write_bytes, b"mp3-preview")

    monkeypatch.setattr(media_streaming, "_run_ffmpeg", fake_run_ffmpeg)
    first, second = await asyncio.gather(
        client.get(f"/tracks/{track_id}/audio?transcode=mp3"),
        client.get(f"/tracks/{track_id}/audio?transcode=mp3"),
    )
    assert calls == 1
    assert first.status_code == second.status_code == 200
    assert first.content == second.content == b"mp3-preview"
    assert first.headers["content-type"] == "audio/mpeg"
    cache = test_settings.artwork_cache_root.parent / "library-audio"
    assert len(list(cache.glob("*.mp3"))) == 1
    assert not list(cache.glob(".*.tmp.mp3"))


async def test_failed_transcode_leaves_no_cache_or_literal_error(
    client: AsyncClient, test_settings: Settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    from app.services import media_streaming

    test_settings.artwork_cache_root = test_settings.library_root.parent / "data" / "artwork"
    track_id, source = await _imported_track(test_settings, name="failure.flac", content=b"source")

    async def fail(source_fd: int, destination: Path) -> None:
        del source_fd
        await asyncio.to_thread(destination.write_bytes, b"partial")
        raise RuntimeError(f"ffmpeg failed for {source}")

    monkeypatch.setattr(media_streaming, "_run_ffmpeg", fail)
    response = await client.get(f"/tracks/{track_id}/audio?transcode=mp3")
    assert response.status_code == 422
    assert str(source) not in response.text
    cache = test_settings.artwork_cache_root.parent / "library-audio"
    assert not list(cache.glob("*.mp3"))
    assert not list(cache.glob(".*.tmp.mp3"))


async def test_source_change_invalidates_old_preview(
    client: AsyncClient, test_settings: Settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    from app.services import media_streaming

    test_settings.artwork_cache_root = test_settings.library_root.parent / "data" / "artwork"
    track_id, source = await _imported_track(test_settings, name="changes.flac", content=b"first")
    calls = 0

    async def fake_run_ffmpeg(source_fd: int, destination: Path) -> None:
        nonlocal calls
        calls += 1
        await asyncio.to_thread(destination.write_bytes, os.pread(source_fd, 64, 0))

    monkeypatch.setattr(media_streaming, "_run_ffmpeg", fake_run_ffmpeg)
    first = await client.get(f"/tracks/{track_id}/audio?transcode=mp3")
    source.write_bytes(b"second-version")
    second = await client.get(f"/tracks/{track_id}/audio?transcode=mp3")

    assert calls == 2
    assert first.content == b"first"
    assert second.content == b"second-version"
    cache = test_settings.artwork_cache_root.parent / "library-audio"
    assert len(list(cache.glob("*.mp3"))) == 1


async def test_global_transcode_semaphore_bounds_different_keys(
    client: AsyncClient, test_settings: Settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    from app.services import media_streaming

    test_settings.artwork_cache_root = test_settings.library_root.parent / "data" / "artwork"
    first_id, _ = await _imported_track(test_settings, name="first.flac", content=b"one")
    second_id, _ = await _imported_track(test_settings, name="second.flac", content=b"two")
    active = 0
    maximum = 0

    async def fake_run_ffmpeg(source_fd: int, destination: Path) -> None:
        nonlocal active, maximum
        active += 1
        maximum = max(maximum, active)
        try:
            await asyncio.sleep(0.03)
            await asyncio.to_thread(destination.write_bytes, os.pread(source_fd, 64, 0))
        finally:
            active -= 1

    monkeypatch.setattr(media_streaming, "_TRANSCODE_SEMAPHORE", asyncio.Semaphore(1))
    monkeypatch.setattr(media_streaming, "_run_ffmpeg", fake_run_ffmpeg)
    responses = await asyncio.gather(
        client.get(f"/tracks/{first_id}/audio?transcode=mp3"),
        client.get(f"/tracks/{second_id}/audio?transcode=mp3"),
    )
    assert [response.status_code for response in responses] == [200, 200]
    assert maximum == 1


async def test_cancelled_transcode_closes_source_and_removes_temporary(
    test_settings: Settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    from app.services import media_streaming

    root = test_settings.library_root
    root.mkdir(parents=True)
    source = root / "cancel.flac"
    source.write_bytes(b"source")
    asset = media_streaming.open_path_beneath_root(source, root)
    fd = asset.fd
    started = asyncio.Event()

    async def never_finishes(source_fd: int, destination: Path) -> None:
        del source_fd
        await asyncio.to_thread(destination.write_bytes, b"partial")
        started.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(media_streaming, "_run_ffmpeg", never_finishes)
    cache = test_settings.library_root.parent / "data" / "library-audio"
    task = asyncio.create_task(
        media_streaming.open_or_create_mp3_preview(asset, track_id=42, cache_root=cache)
    )
    await started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert not list(cache.glob("*.mp3"))
    assert not list(cache.glob(".*.tmp.mp3"))
    with pytest.raises(OSError):
        os.fstat(fd)
