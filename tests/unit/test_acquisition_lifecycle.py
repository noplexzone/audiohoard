"""Unit tests for external-client download completion lifecycle (Task 2).

Tests cover slskd and SABnzbd polling: success, failure, timeout, missing
artifact, traversal rejection, and cancellation propagation.
"""

from __future__ import annotations

import asyncio
import json
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from app.config import Settings
from app.jobs import runner
from app.models.catalog_entities import CatalogAlbum, CatalogArtist
from app.models.job import Job, JobStatus
from app.services.download_queue import error_label, project_download_groups
from app.sources.base import CapabilityState
from app.sources.youtube import ProviderError  # noqa: F401 (used by type-check below)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def staging_root(tmp_path: Path) -> Path:
    root = tmp_path / "staging"
    root.mkdir()
    return root


@pytest.fixture
def fast_settings(test_settings: Settings, staging_root: Path) -> Settings:
    return test_settings.model_copy(
        update={
            "staging_root": staging_root,
            "slskd_poll_interval": 0.01,
            "slskd_poll_timeout": 0.2,
            "sabnzbd_poll_interval": 0.01,
            "sabnzbd_poll_timeout": 0.2,
        }
    )


# ---------------------------------------------------------------------------
# Fake adapters
# ---------------------------------------------------------------------------


class _FakeSlskdAdapter:
    def __init__(self, states: list[CapabilityState]) -> None:
        self._iter = iter(states)
        self.cancel_calls: list[tuple[str, str, str | None]] = []

    async def status(self, transfer_id: str) -> CapabilityState:  # noqa: ARG002
        return next(self._iter)

    async def cancel(self, username: str, filename: str, transfer_id: str | None = None) -> None:
        self.cancel_calls.append((username, filename, transfer_id))


class _FakeSabAdapter:
    def __init__(
        self,
        queue_states: list[CapabilityState],
        history_states: list[CapabilityState],
    ) -> None:
        self._queue = iter(queue_states)
        self._history = iter(history_states)

    async def status(self, nzo_id: str) -> CapabilityState:  # noqa: ARG002
        return next(self._queue)

    async def history_status(self, nzo_id: str) -> CapabilityState:  # noqa: ARG002
        return next(self._history)


def test_download_group_projects_catalog_album_detail_and_fallback_label() -> None:
    now = datetime.now(UTC)
    artist = CatalogArtist(id=7, name="Projected Artist")
    album = CatalogAlbum(
        id=11,
        artist=artist,
        title="Projected Album",
        artwork_url="https://example.test/cover.jpg",
        year="2026",
        release_type="album",
        track_count=0,
    )
    catalog_job = Job(
        id=21,
        source="slskd",
        query="catalog query",
        status=JobStatus.pending,
        catalog_album=album,
        catalog_album_id=album.id,
        queue_hidden=False,
        created_at=now,
        updated_at=now,
    )
    fallback_job = Job(
        id=22,
        source="slskd",
        query="Fallback Query",
        status=JobStatus.pending,
        queue_hidden=False,
        created_at=now,
        updated_at=now,
    )

    groups = project_download_groups([catalog_job, fallback_job], {})
    catalog_group = next(group for group in groups if group.catalog_album_id is not None)
    fallback_group = next(group for group in groups if group.catalog_album_id is None)

    assert catalog_group.catalog_album_id == 11
    assert catalog_group.artist_name == "Projected Artist"
    assert catalog_group.album_title == "Projected Album"
    assert catalog_group.artwork_url == "https://example.test/cover.jpg"
    assert (
        fallback_group.catalog_album_id,
        fallback_group.artwork_url,
        fallback_group.artist_name,
        fallback_group.album_title,
        fallback_group.year,
        fallback_group.release_kind,
    ) == (None, None, None, None, None, None)
    assert fallback_group.label == "Fallback Query"


def test_error_label_humanizes_known_and_unknown_codes() -> None:
    assert error_label("candidate_identity_mismatch") == "No matching file found"
    assert error_label("new_provider_error") == "New provider error"


# ---------------------------------------------------------------------------
# _resolve_staged_path — traversal
# ---------------------------------------------------------------------------


def test_resolve_staged_path_accepts_valid_path(staging_root: Path) -> None:
    target = staging_root / "song.flac"
    target.write_bytes(b"audio")
    result = runner._resolve_staged_path(target, staging_root)
    assert result == target.resolve()


def test_resolve_staged_path_rejects_traversal(staging_root: Path, tmp_path: Path) -> None:
    outside = tmp_path / "secret.txt"
    outside.write_text("bad")
    with pytest.raises(ProviderError) as exc_info:
        runner._resolve_staged_path(outside, staging_root)
    assert exc_info.value.code == "path_traversal"


def test_resolve_staged_path_rejects_dotdot_path(staging_root: Path) -> None:
    # Construct a path that would escape via ..
    escaped = staging_root / ".." / "escaped.txt"
    with pytest.raises(ProviderError) as exc_info:
        runner._resolve_staged_path(escaped, staging_root)
    assert exc_info.value.code == "path_traversal"


# ---------------------------------------------------------------------------
# _poll_slskd_transfer — success
# ---------------------------------------------------------------------------


async def test_slskd_retry_adopts_existing_completed_transfer(
    fast_settings: Settings,
    staging_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    filename = "Music\\Artist\\Album\\01 Song.flac"
    staged = staging_root / "01 Song.flac"
    staged.write_bytes(b"flacdata")
    enqueue_calls: list[tuple[str, str, int | None]] = []

    class ExistingTransferAdapter:
        def __init__(self, base_url: str, api_key: str) -> None:
            pass

        async def status(self, transfer_id: str) -> CapabilityState:
            assert transfer_id == f"peer1:{filename}"
            return CapabilityState(True, "Completed, Succeeded", {"id": "provider-uuid"})

        async def enqueue(self, username: str, name: str, size: int | None = None) -> str:
            enqueue_calls.append((username, name, size))
            return "duplicate"

        async def cancel(self, username: str, name: str) -> None:
            pass

    monkeypatch.setattr(runner, "SlskdAdapter", ExistingTransferAdapter)
    track = runner.Track(
        job_id=1,
        source="slskd",
        source_job_id=f"peer1:{filename}",
        acquisition_state=runner.AcquisitionState.failed,
        acquisition_provenance_json=json.dumps(
            {"source": "slskd", "username": "peer1", "filename": filename}
        ),
    )
    result = runner.SearchResult(
        source="slskd",
        title="Song",
        size_bytes=123,
        metadata={"username": "peer1", "filename": filename},
    )

    transfer_id, status = await runner._prepare_acquisition(
        result,
        "slskd",
        fast_settings,
        track,
    )

    assert enqueue_calls == []
    assert transfer_id == "provider-uuid"
    assert status == "downloaded"
    assert track.source_job_id == "provider-uuid"
    assert track.acquisition_state == runner.AcquisitionState.downloaded
    assert track.source_path == str(staged.resolve())


async def test_slskd_success_waits_for_completed_state(staging_root: Path) -> None:
    filename = "music\\Artist\\Album\\01 Song.flac"
    staged = staging_root / "01 Song.flac"
    staged.write_bytes(b"flacdata")

    adapter = _FakeSlskdAdapter(
        [CapabilityState(True, "InProgress"), CapabilityState(True, "Completed")]
    )

    result = await runner._poll_slskd_transfer(
        transfer_id="t1",
        username="peer",
        filename=filename,
        adapter=adapter,
        staging_root=staging_root,
        poll_interval=0.01,
        poll_timeout=5.0,
    )
    assert result == (staged.resolve(), "t1")


async def test_slskd_queued_transfer_stays_under_dispatcher_job_permit(
    staging_root: Path,
) -> None:
    staged = staging_root / "song.flac"
    staged.write_bytes(b"flacdata")
    adapter = _FakeSlskdAdapter(
        [
            CapabilityState(True, "Queued"),
            CapabilityState(True, "InProgress"),
            CapabilityState(True, "Completed"),
        ]
    )

    result = await runner._poll_slskd_transfer(
        transfer_id="t1",
        username="peer",
        filename="song.flac",
        adapter=adapter,
        staging_root=staging_root,
        poll_interval=0.001,
        poll_timeout=1,
    )

    assert result[0] == staged


async def test_slskd_queued_timeout_is_retryable(tmp_path: Path) -> None:
    class QueuedAdapter:
        async def status(self, _transfer_id: str) -> CapabilityState:
            return CapabilityState(available=True, reason="Queued")

    with pytest.raises(ProviderError) as exc_info:
        await runner._poll_slskd_transfer(
            transfer_id="transfer-queued-timeout",
            username="alice",
            filename="song.flac",
            adapter=QueuedAdapter(),  # type: ignore[arg-type]
            staging_root=tmp_path,
            poll_interval=0.01,
            poll_timeout=0.02,
        )

    assert exc_info.value.code == "transfer_timeout"
    assert exc_info.value.retryable is True


async def test_slskd_success_finds_nested_completed_file(staging_root: Path) -> None:
    nested = staging_root / "peer" / "Artist" / "Album"
    nested.mkdir(parents=True)
    staged = nested / "01 Song.flac"
    staged.write_bytes(b"flacdata")
    adapter = _FakeSlskdAdapter([CapabilityState(True, "Completed")])

    result = await runner._poll_slskd_transfer(
        transfer_id="t1",
        username="peer",
        filename="Artist\\Album\\01 Song.flac",
        adapter=adapter,
        staging_root=staging_root,
        poll_interval=0.01,
        poll_timeout=5.0,
    )

    assert result == (staged.resolve(), "t1")


async def test_slskd_success_leaves_transfer_for_terminal_cleanup_after_checkpoint(
    fast_settings: Settings, staging_root: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    staged = staging_root / "song.flac"
    staged.write_bytes(b"audio")
    events: list[str] = []

    class CleanupAdapter:
        def __init__(self, base_url: str, api_key: str) -> None:
            pass

        async def enqueue(self, username: str, filename: str, size: int | None = None) -> str:
            return "transfer-1"

        async def status(self, transfer_id: str) -> CapabilityState:
            return CapabilityState(True, "Completed")

        async def cancel(self, username: str, filename: str) -> None:
            events.append("cancel")

    monkeypatch.setattr(runner, "SlskdAdapter", CleanupAdapter)
    track = runner.Track(
        job_id=1,
        source="slskd",
        acquisition_state=runner.AcquisitionState.queued,
    )
    result = runner.SearchResult(
        source="slskd",
        title="Song",
        size_bytes=5,
        metadata={"username": "peer", "filename": "song.flac"},
    )

    async def checkpoint() -> None:
        assert track.source_job_id == "transfer-1"
        if events:
            assert track.staging_path == str(staged.resolve())
            assert track.acquisition_state == runner.AcquisitionState.downloaded
        else:
            assert track.staging_path is None
            assert track.acquisition_state == runner.AcquisitionState.acquiring
        events.append("checkpoint")

    await runner._prepare_acquisition(result, "slskd", fast_settings, track, checkpoint=checkpoint)

    assert events == ["checkpoint", "checkpoint"]


async def test_slskd_rejects_ambiguous_completed_file(staging_root: Path) -> None:
    for peer in ("peer-a", "peer-b"):
        directory = staging_root / peer
        directory.mkdir()
        (directory / "song.flac").write_bytes(b"audio")
    adapter = _FakeSlskdAdapter([CapabilityState(True, "Completed")])

    with pytest.raises(ProviderError) as exc_info:
        await runner._poll_slskd_transfer(
            transfer_id="t1",
            username="peer",
            filename="song.flac",
            adapter=adapter,
            staging_root=staging_root,
            poll_interval=0.01,
            poll_timeout=5.0,
        )

    assert exc_info.value.code == "artifact_ambiguous"


# ---------------------------------------------------------------------------
# _poll_slskd_transfer — failure
# ---------------------------------------------------------------------------


async def test_slskd_failure_raises_provider_error(staging_root: Path) -> None:
    adapter = _FakeSlskdAdapter([CapabilityState(True, "Failed")])

    with pytest.raises(ProviderError) as exc_info:
        await runner._poll_slskd_transfer(
            transfer_id="t1",
            username="peer",
            filename="song.flac",
            adapter=adapter,
            staging_root=staging_root,
            poll_interval=0.01,
            poll_timeout=5.0,
        )
    assert exc_info.value.code == "transfer_failed"
    assert exc_info.value.retryable is True


async def test_slskd_not_found_raises_provider_error(staging_root: Path) -> None:
    adapter = _FakeSlskdAdapter([CapabilityState(False, "transfer not found")])

    with pytest.raises(ProviderError) as exc_info:
        await runner._poll_slskd_transfer(
            transfer_id="t1",
            username="peer",
            filename="song.flac",
            adapter=adapter,
            staging_root=staging_root,
            poll_interval=0.01,
            poll_timeout=5.0,
        )
    assert exc_info.value.code == "transfer_failed"


# ---------------------------------------------------------------------------
# _poll_slskd_transfer — timeout
# ---------------------------------------------------------------------------


async def test_slskd_timeout_raises_provider_error(staging_root: Path) -> None:
    class NeverDoneAdapter:
        call_count = 0
        cancel_calls: list[tuple[str, str]] = []

        async def status(self, transfer_id: str) -> CapabilityState:  # noqa: ARG002
            NeverDoneAdapter.call_count += 1
            return CapabilityState(True, "InProgress")

        async def cancel(self, username: str, filename: str) -> None:
            NeverDoneAdapter.cancel_calls.append((username, filename))

    t0 = time.monotonic()
    with pytest.raises(ProviderError) as exc_info:
        await runner._poll_slskd_transfer(
            transfer_id="t1",
            username="peer",
            filename="song.flac",
            adapter=NeverDoneAdapter(),
            staging_root=staging_root,
            poll_interval=0.01,
            poll_timeout=0.1,
        )
    elapsed = time.monotonic() - t0
    assert exc_info.value.code == "transfer_timeout"
    assert exc_info.value.retryable is True
    assert elapsed < 2.0
    assert NeverDoneAdapter.call_count > 0
    assert NeverDoneAdapter.cancel_calls == []


# ---------------------------------------------------------------------------
# _poll_slskd_transfer — missing artifact
# ---------------------------------------------------------------------------


async def test_slskd_missing_artifact_raises_provider_error(staging_root: Path) -> None:
    adapter = _FakeSlskdAdapter([CapabilityState(True, "Completed")])

    with pytest.raises(ProviderError) as exc_info:
        await runner._poll_slskd_transfer(
            transfer_id="t1",
            username="peer",
            filename="missing_song.flac",
            adapter=adapter,
            staging_root=staging_root,
            poll_interval=0.01,
            poll_timeout=5.0,
        )
    assert exc_info.value.code == "artifact_missing"


# ---------------------------------------------------------------------------
# _poll_slskd_transfer — cancellation
# ---------------------------------------------------------------------------


async def test_slskd_cancellation_calls_adapter_cancel(
    staging_root: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cancel_calls: list[tuple[str, str, str | None]] = []

    class CancelRaisingAdapter:
        async def status(self, transfer_id: str) -> CapabilityState:  # noqa: ARG002
            return CapabilityState(True, "InProgress", {"id": transfer_id})

        async def cancel(
            self, username: str, filename: str, transfer_id: str | None = None
        ) -> None:
            cancel_calls.append((username, filename, transfer_id))

    async def cancelled_sleep(delay: float) -> None:  # noqa: ARG001
        raise asyncio.CancelledError

    monkeypatch.setattr(runner.asyncio, "sleep", cancelled_sleep)

    with pytest.raises(asyncio.CancelledError):
        await runner._poll_slskd_transfer(
            transfer_id="t1",
            username="peer",
            filename="song.flac",
            adapter=CancelRaisingAdapter(),
            staging_root=staging_root,
            poll_interval=0.01,
            poll_timeout=5.0,
        )
    assert cancel_calls == [("peer", "song.flac", "t1")]


# ---------------------------------------------------------------------------
# _poll_sab_job — success
# ---------------------------------------------------------------------------


async def test_sab_success_finds_file_in_history(staging_root: Path) -> None:
    download_dir = staging_root / "Artist - Album"
    download_dir.mkdir()
    audio_file = download_dir / "01 Track.flac"
    audio_file.write_bytes(b"flacdata")

    adapter = _FakeSabAdapter(
        queue_states=[
            CapabilityState(True, "Downloading"),
            CapabilityState(False, "not found"),
        ],
        history_states=[
            CapabilityState(
                True,
                "Completed",
                extra={"nzo_id": "SAB1", "storage": str(download_dir)},
            )
        ],
    )

    result = await runner._poll_sab_job(
        nzo_id="SAB1",
        adapter=adapter,
        staging_root=staging_root,
        poll_interval=0.01,
        poll_timeout=5.0,
    )
    assert result == audio_file.resolve()


# ---------------------------------------------------------------------------
# _poll_sab_job — failure
# ---------------------------------------------------------------------------


async def test_sab_history_failure_raises_provider_error(staging_root: Path) -> None:
    adapter = _FakeSabAdapter(
        queue_states=[CapabilityState(False, "not found")],
        history_states=[CapabilityState(True, "Failed", extra={"nzo_id": "SAB1", "storage": ""})],
    )

    with pytest.raises(ProviderError) as exc_info:
        await runner._poll_sab_job(
            nzo_id="SAB1",
            adapter=adapter,
            staging_root=staging_root,
            poll_interval=0.01,
            poll_timeout=5.0,
        )
    assert exc_info.value.code == "transfer_failed"
    assert exc_info.value.retryable is True


async def test_sab_queue_failure_raises_provider_error(staging_root: Path) -> None:
    adapter = _FakeSabAdapter(
        queue_states=[CapabilityState(True, "Failed")],
        history_states=[],
    )

    with pytest.raises(ProviderError) as exc_info:
        await runner._poll_sab_job(
            nzo_id="SAB1",
            adapter=adapter,
            staging_root=staging_root,
            poll_interval=0.01,
            poll_timeout=5.0,
        )
    assert exc_info.value.code == "transfer_failed"


# ---------------------------------------------------------------------------
# _poll_sab_job — timeout
# ---------------------------------------------------------------------------


async def test_sab_timeout_raises_provider_error(staging_root: Path) -> None:
    class StuckAdapter:
        call_count = 0

        async def status(self, nzo_id: str) -> CapabilityState:  # noqa: ARG002
            StuckAdapter.call_count += 1
            return CapabilityState(True, "Downloading")

        async def history_status(self, nzo_id: str) -> CapabilityState:  # noqa: ARG002
            return CapabilityState(False, "not found in history")

    t0 = time.monotonic()
    with pytest.raises(ProviderError) as exc_info:
        await runner._poll_sab_job(
            nzo_id="SAB1",
            adapter=StuckAdapter(),
            staging_root=staging_root,
            poll_interval=0.01,
            poll_timeout=0.1,
        )
    elapsed = time.monotonic() - t0
    assert exc_info.value.code == "transfer_timeout"
    assert exc_info.value.retryable is True
    assert elapsed < 2.0
    assert StuckAdapter.call_count > 0


# ---------------------------------------------------------------------------
# _poll_sab_job — missing artifact
# ---------------------------------------------------------------------------


async def test_sab_missing_artifact_raises_provider_error(staging_root: Path) -> None:
    empty_dir = staging_root / "empty"
    empty_dir.mkdir()

    adapter = _FakeSabAdapter(
        queue_states=[CapabilityState(False, "not found")],
        history_states=[
            CapabilityState(
                True,
                "Completed",
                extra={"nzo_id": "SAB1", "storage": str(empty_dir)},
            )
        ],
    )

    with pytest.raises(ProviderError) as exc_info:
        await runner._poll_sab_job(
            nzo_id="SAB1",
            adapter=adapter,
            staging_root=staging_root,
            poll_interval=0.01,
            poll_timeout=5.0,
        )
    assert exc_info.value.code == "artifact_missing"


# ---------------------------------------------------------------------------
# _poll_sab_job — traversal from SABnzbd history storage path
# ---------------------------------------------------------------------------


async def test_sab_traversal_in_storage_path_raises_provider_error(
    staging_root: Path, tmp_path: Path
) -> None:
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "track.flac").write_bytes(b"audio")

    adapter = _FakeSabAdapter(
        queue_states=[CapabilityState(False, "not found")],
        history_states=[
            CapabilityState(
                True,
                "Completed",
                extra={"nzo_id": "SAB1", "storage": str(outside)},
            )
        ],
    )

    with pytest.raises(ProviderError) as exc_info:
        await runner._poll_sab_job(
            nzo_id="SAB1",
            adapter=adapter,
            staging_root=staging_root,
            poll_interval=0.01,
            poll_timeout=5.0,
        )
    assert exc_info.value.code == "path_traversal"


# ---------------------------------------------------------------------------
# _poll_sab_job — lost job (disappeared from queue and history)
# ---------------------------------------------------------------------------


async def test_sab_lost_job_raises_provider_error(staging_root: Path) -> None:
    adapter = _FakeSabAdapter(
        queue_states=[CapabilityState(False, "not found")],
        history_states=[CapabilityState(False, "not found in history")],
    )

    with pytest.raises(ProviderError) as exc_info:
        await runner._poll_sab_job(
            nzo_id="SAB1",
            adapter=adapter,
            staging_root=staging_root,
            poll_interval=0.01,
            poll_timeout=5.0,
        )
    assert exc_info.value.code == "transfer_lost"


# ---------------------------------------------------------------------------
# Integration: _prepare_acquisition wires polling for slskd
# ---------------------------------------------------------------------------


async def test_prepare_acquisition_slskd_polls_and_populates_track(
    fast_settings: Settings,
) -> None:
    staging = fast_settings.staging_root
    audio_file = staging / "song.flac"
    audio_file.write_bytes(b"flacdata")

    from app.models.track import Track
    from app.models.workflow import AcquisitionState
    from app.schemas.search import SearchResult

    track = Track(
        job_id=1,
        source="slskd",
        acquisition_state=AcquisitionState.queued,
    )

    polled: list[str] = []

    async def fake_poll(
        transfer_id: str,
        username: str,
        filename: str,
        adapter: Any,
        staging_root: Path,
        poll_interval: float,
        poll_timeout: float,
        on_provider_id: object | None = None,
    ) -> tuple[Path, str]:
        polled.append(transfer_id)
        return audio_file, "provider-uuid"

    import unittest.mock

    enqueue_mock = unittest.mock.AsyncMock(return_value="tid-42")

    class FakeSlskd:
        def __init__(self, url: str, key: str) -> None:
            pass

        async def enqueue(self, username: str, filename: str, size: int | None = None) -> str:
            return await enqueue_mock(username, filename, size)

    original_poll = runner._poll_slskd_transfer
    original_adapter = runner.SlskdAdapter
    runner._poll_slskd_transfer = fake_poll  # type: ignore[assignment]
    runner.SlskdAdapter = FakeSlskd  # type: ignore[assignment]
    try:
        result = SearchResult(
            source="slskd",
            title="Song",
            url="slskd://peer/music/song.flac",
            metadata={"username": "peer", "filename": "music/song.flac"},
        )
        job_id, status = await runner._prepare_acquisition(result, "slskd", fast_settings, track)
    finally:
        runner._poll_slskd_transfer = original_poll  # type: ignore[assignment]
        runner.SlskdAdapter = original_adapter  # type: ignore[assignment]

    assert polled == ["tid-42"]
    assert job_id == "provider-uuid"
    assert track.source_job_id == "provider-uuid"
    assert status == "downloaded"
    assert track.acquisition_state == AcquisitionState.downloaded
    assert track.source_path == str(audio_file)
    assert track.staging_path == str(audio_file)


# ---------------------------------------------------------------------------
# Integration: _prepare_acquisition wires polling for SABnzbd
# ---------------------------------------------------------------------------


async def test_prepare_acquisition_prowlarr_polls_and_populates_track(
    fast_settings: Settings,
) -> None:
    fast_settings = fast_settings.model_copy(update={"prowlarr_url": "https://prowlarr.test"})
    staging = fast_settings.staging_root
    audio_file = staging / "track.flac"
    audio_file.write_bytes(b"flacdata")

    from app.models.track import Track
    from app.models.workflow import AcquisitionState
    from app.schemas.search import SearchResult

    track = Track(
        job_id=1,
        source="prowlarr",
        acquisition_state=AcquisitionState.queued,
    )

    polled: list[str] = []

    async def fake_poll(
        nzo_id: str,
        adapter: Any,
        staging_root: Path,
        poll_interval: float,
        poll_timeout: float,
    ) -> Path:
        polled.append(nzo_id)
        return audio_file

    class FakeSab:
        def __init__(self, url: str, key: str) -> None:
            pass

        async def enqueue(self, nzb_url: str, name: str | None = None) -> str:
            return "SAB99"

    original_poll = runner._poll_sab_job
    original_sab = runner.SabnzbdAdapter
    runner._poll_sab_job = fake_poll  # type: ignore[assignment]
    runner.SabnzbdAdapter = FakeSab  # type: ignore[assignment]
    try:
        result = SearchResult(
            source="prowlarr",
            title="Artist - Album",
            url="https://prowlarr.test/download/file.nzb",
            format="nzb",
        )
        job_id, status = await runner._prepare_acquisition(
            result, "prowlarr", fast_settings, track
        )
    finally:
        runner._poll_sab_job = original_poll  # type: ignore[assignment]
        runner.SabnzbdAdapter = original_sab  # type: ignore[assignment]

    assert polled == ["SAB99"]
    assert status == "downloaded"
    assert track.acquisition_state == AcquisitionState.downloaded
    assert track.source_path == str(audio_file)
    assert track.staging_path == str(audio_file)


async def test_prepare_acquisition_resumes_slskd_without_enqueue(
    fast_settings: Settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    from app.models.track import Track
    from app.models.workflow import AcquisitionState
    from app.schemas.search import SearchResult

    audio_file = fast_settings.staging_root / "resumed.flac"
    audio_file.write_bytes(b"audio")
    enqueue_calls: list[tuple[str, str, int | None]] = []
    poll_calls: list[str] = []

    class FakeSlskd:
        def __init__(self, url: str, key: str) -> None:
            pass

        async def enqueue(self, username: str, filename: str, size: int | None = None) -> str:
            enqueue_calls.append((username, filename, size))
            return "unexpected-new-transfer"

    async def fake_poll(
        transfer_id: str,
        username: str,
        filename: str,
        adapter: Any,
        staging_root: Path,
        poll_interval: float,
        poll_timeout: float,
        on_provider_id: object | None = None,
    ) -> tuple[Path, str]:
        poll_calls.append(transfer_id)
        return audio_file, transfer_id

    monkeypatch.setattr(runner, "SlskdAdapter", FakeSlskd)
    monkeypatch.setattr(runner, "_poll_slskd_transfer", fake_poll)
    track = Track(
        job_id=1,
        source="slskd",
        source_job_id="existing-transfer",
        source_status="acquiring",
        acquisition_state=AcquisitionState.acquiring,
        acquisition_provenance_json=json.dumps(
            {"source": "slskd", "username": "peer", "filename": "music/song.flac"}
        ),
    )
    result = SearchResult(
        source="slskd",
        title="Song",
        metadata={"username": "peer", "filename": "music/song.flac"},
    )

    source_job_id, status = await runner._prepare_acquisition(
        result, "slskd", fast_settings, track
    )

    assert enqueue_calls == []
    assert poll_calls == ["existing-transfer"]
    assert source_job_id == "existing-transfer"
    assert status == "downloaded"
    assert track.acquisition_state == AcquisitionState.downloaded


@pytest.mark.parametrize(
    "provenance",
    [
        None,
        "{not-json",
        {"source": "slskd", "username": "old-peer", "filename": "New/01 Song.flac"},
        {"source": "slskd", "username": "new-peer", "filename": "Old/01 Song.flac"},
    ],
    ids=["missing-provenance", "malformed-provenance", "mismatched-peer", "mismatched-path"],
)
async def test_prepare_acquisition_does_not_adopt_stale_slskd_transfer(
    fast_settings: Settings,
    monkeypatch: pytest.MonkeyPatch,
    provenance: dict[str, str] | str | None,
) -> None:
    from app.models.track import Track
    from app.models.workflow import AcquisitionState
    from app.schemas.search import SearchResult

    audio_file = fast_settings.staging_root / "new-transfer.flac"
    audio_file.write_bytes(b"audio")
    status_calls: list[str] = []
    enqueue_calls: list[tuple[str, str, int | None]] = []
    poll_calls: list[str] = []

    class FakeSlskd:
        def __init__(self, url: str, key: str) -> None:
            pass

        async def status(self, transfer_id: str) -> CapabilityState:
            status_calls.append(transfer_id)
            return CapabilityState(True, "Completed")

        async def enqueue(self, username: str, filename: str, size: int | None = None) -> str:
            enqueue_calls.append((username, filename, size))
            return "new-transfer"

    async def fake_poll(
        transfer_id: str,
        username: str,
        filename: str,
        adapter: Any,
        staging_root: Path,
        poll_interval: float,
        poll_timeout: float,
        on_provider_id: object | None = None,
    ) -> tuple[Path, str]:
        poll_calls.append(transfer_id)
        return audio_file, transfer_id

    monkeypatch.setattr(runner, "SlskdAdapter", FakeSlskd)
    monkeypatch.setattr(runner, "_poll_slskd_transfer", fake_poll)
    track = Track(
        job_id=1,
        source="slskd",
        source_job_id="old-transfer",
        source_status="failed",
        acquisition_state=AcquisitionState.failed,
        acquisition_provenance_json=(
            provenance
            if isinstance(provenance, str)
            else json.dumps(provenance)
            if provenance is not None
            else None
        ),
    )
    result = SearchResult(
        source="slskd",
        title="Song",
        metadata={"username": "new-peer", "filename": "New/01 Song.flac"},
    )

    source_job_id, status = await runner._prepare_acquisition(
        result, "slskd", fast_settings, track
    )

    assert status_calls == []
    assert enqueue_calls == [("new-peer", "New/01 Song.flac", None)]
    assert poll_calls == ["new-transfer"]
    assert source_job_id == "new-transfer"
    assert status == "downloaded"
    assert track.source_job_id == "new-transfer"
    assert json.loads(track.acquisition_provenance_json or "{}") == {
        "filename": "New/01 Song.flac",
        "source": "slskd",
        "username": "new-peer",
    }


async def test_prepare_acquisition_checkpoints_enqueue_before_poll(
    fast_settings: Settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    from app.models.track import Track
    from app.models.workflow import AcquisitionState
    from app.schemas.search import SearchResult

    audio_file = fast_settings.staging_root / "checkpointed.flac"
    audio_file.write_bytes(b"audio")
    events: list[str] = []

    class FakeSlskd:
        def __init__(self, url: str, key: str) -> None:
            pass

        async def enqueue(self, username: str, filename: str, size: int | None = None) -> str:
            events.append("enqueue")
            return "new-transfer"

        async def cancel(self, username: str, filename: str) -> None:
            events.append("cancel")

    async def checkpoint() -> None:
        assert track.source_job_id == "new-transfer"
        assert track.acquisition_provenance_json is not None
        if events.count("checkpoint") == 0:
            assert track.source_status == "acquiring"
            assert track.acquisition_state == AcquisitionState.acquiring
        else:
            assert track.staging_path == str(audio_file)
            assert track.acquisition_state == AcquisitionState.downloaded
        events.append("checkpoint")

    async def fake_poll(
        transfer_id: str,
        username: str,
        filename: str,
        adapter: Any,
        staging_root: Path,
        poll_interval: float,
        poll_timeout: float,
        on_provider_id: object | None = None,
    ) -> tuple[Path, str]:
        events.append("poll")
        return audio_file, transfer_id

    monkeypatch.setattr(runner, "SlskdAdapter", FakeSlskd)
    monkeypatch.setattr(runner, "_poll_slskd_transfer", fake_poll)
    track = Track(job_id=1, source="slskd", acquisition_state=AcquisitionState.acquiring)
    result = SearchResult(
        source="slskd",
        title="Song",
        metadata={"username": "peer", "filename": "music/song.flac"},
    )

    await runner._prepare_acquisition(result, "slskd", fast_settings, track, checkpoint=checkpoint)

    assert events == ["enqueue", "checkpoint", "poll", "checkpoint"]


async def test_prepare_acquisition_resumes_sab_without_enqueue(
    fast_settings: Settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    from app.models.track import Track
    from app.models.workflow import AcquisitionState
    from app.schemas.search import SearchResult

    settings = fast_settings.model_copy(update={"prowlarr_url": "https://prowlarr.test"})
    audio_file = settings.staging_root / "resumed-sab.flac"
    audio_file.write_bytes(b"audio")
    enqueue_calls: list[str] = []
    poll_calls: list[str] = []

    class FakeSab:
        def __init__(self, url: str, key: str) -> None:
            pass

        async def enqueue(self, nzb_url: str, name: str | None = None) -> str:
            enqueue_calls.append(nzb_url)
            return "unexpected-new-job"

    async def fake_poll(
        nzo_id: str,
        adapter: Any,
        staging_root: Path,
        poll_interval: float,
        poll_timeout: float,
    ) -> Path:
        poll_calls.append(nzo_id)
        return audio_file

    monkeypatch.setattr(runner, "SabnzbdAdapter", FakeSab)
    monkeypatch.setattr(runner, "_poll_sab_job", fake_poll)
    track = Track(
        job_id=1,
        source="prowlarr",
        source_job_id="existing-sab-job",
        source_status="acquiring",
        acquisition_state=AcquisitionState.acquiring,
    )
    result = SearchResult(
        source="prowlarr",
        title="Artist - Album",
        url="https://prowlarr.test/download/file.nzb",
        format="nzb",
    )

    source_job_id, status = await runner._prepare_acquisition(result, "prowlarr", settings, track)

    assert enqueue_calls == []
    assert poll_calls == ["existing-sab-job"]
    assert source_job_id == "existing-sab-job"
    assert status == "downloaded"
