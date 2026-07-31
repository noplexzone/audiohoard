from __future__ import annotations

import asyncio
import contextlib
import json
import sqlite3
from collections.abc import AsyncGenerator, Sequence
from datetime import UTC, datetime, timedelta

import pytest
import pytest_asyncio
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

import app.database as _db_module
from app.config import Settings
from app.database import Base
from app.jobs import runner
from app.models.catalog_entities import CatalogAlbum, CatalogAlbumTrack, CatalogArtist
from app.models.job import Job, JobStatus
from app.models.release import Release
from app.models.track import IdentityResolutionState, Track
from app.models.workflow import (
    AcoustIDVerificationState,
    AcquisitionState,
    ImportWorkflowState,
)
from app.naming.convention import NamingError
from app.schemas.search import SearchRequest, SearchResult
from app.sources.base import CapabilityState
from app.sources.youtube import ProviderError


async def _create_job(db_session: AsyncSession, source: str = "youtube") -> Job:
    job = Job(source=source, query="test query", status=JobStatus.pending)
    db_session.add(job)
    await db_session.flush()
    return job


def test_targeted_query_variants_normalize_and_simplify_titles() -> None:
    assert runner._targeted_query_variants(
        "Colter Wall", "You're Lucky She's Lonely", "You’re Lucky She’s Lonely"
    ) == ["Colter Wall You're Lucky She's Lonely"]
    assert runner._targeted_query_variants("Ty Myers", "Valerie", "Valerie") == [
        "Ty Myers Valerie"
    ]
    assert runner._targeted_query_variants(
        "Ty Myers", "The Select (Deluxe)", "Thought It Was Love (Acoustic)"
    ) == [
        "Ty Myers Thought It Was Love (Acoustic)",
        "Ty Myers Thought It Was Love",
    ]


def test_targeted_query_variants_include_required_collaborators_first() -> None:
    assert runner._targeted_query_variants(
        "Morgan Wallen",
        "Miami",
        "Miami",
        required_terms=("Lil Wayne", "Rick Ross"),
    ) == [
        "Morgan Wallen Miami Lil Wayne Rick Ross",
        "Morgan Wallen Miami",
    ]


def test_targeted_catalog_result_requires_musicbrainz_collaborators() -> None:
    target = CatalogAlbumTrack(id=12, position=1, disc=1, title="Miami")
    plain_album = SearchResult(
        source="slskd",
        title="Miami",
        artist="Morgan Wallen",
        metadata={"filename": r"Morgan Wallen\I’m The Problem\2-16 - Miami.mp3"},
    )
    remix = SearchResult(
        source="slskd",
        title="Miami",
        artist="Morgan Wallen",
        metadata={
            "filename": ("Morgan Wallen ft Lil Wayne & Rick Ross - Miami (Clean) (2025).mp3")
        },
    )

    assert (
        runner._targeted_catalog_result_matches(
            plain_album, target, required_terms=("Lil Wayne", "Rick Ross")
        )
        is False
    )
    assert (
        runner._targeted_catalog_result_matches(
            remix, target, required_terms=("Lil Wayne", "Rick Ross")
        )
        is True
    )


def test_required_identity_terms_parse_featured_artists_without_primary_artist() -> None:
    assert runner._required_identity_terms_from_text(
        "Miami (feat. Lil Wayne & Rick Ross) - Remix", "Morgan Wallen"
    ) == ["Lil Wayne", "Rick Ross"]
    assert (
        runner._required_identity_terms_from_text(
            "Heartless (feat. Morgan Wallen) (Wallen Album Mix)", "Morgan Wallen"
        )
        == []
    )


def test_collaborator_terms_require_featured_artist_not_other_lead() -> None:
    assert runner._collaborator_terms(
        "Thomas Wesley & Julia Michaels feat. Morgan Wallen", "Morgan Wallen"
    ) == ["Julia Michaels"]


def test_targeted_catalog_result_accepts_promo_filename_with_mix_suffixes() -> None:
    target = CatalogAlbumTrack(id=13, position=1, disc=1, title="Miami")
    promo = SearchResult(
        source="slskd",
        title="11A - 136",
        artist="MORGAN WALLEN FT LIL WAYNE & RICK ROSS",
        metadata={
            "filename": (
                "PROMO ONLY SERIES\\MORGAN WALLEN FT LIL WAYNE & RICK ROSS "
                "- MIAMI (CLEAN) (2025) - 11A - 136.mp3"
            )
        },
    )

    assert runner._targeted_catalog_result_matches(
        promo, target, required_terms=("Lil Wayne", "Rick Ross")
    )


def test_targeted_catalog_result_accepts_numeric_version_separator_variants() -> None:
    target = CatalogAlbumTrack(id=14, position=1, disc=1, title="Spin You Around (1/24)")
    folder_result = SearchResult(
        source="slskd",
        title="Spin You Around 1 24",
        artist="Morgan Wallen",
        metadata={
            "filename": (
                "Morgan Wallen - Single - 2024 - Spin You Around 1 24\\"
                "0101 - Spin You Around 1 24.flac"
            )
        },
    )
    bracket_result = SearchResult(
        source="slskd",
        title="Spin You Around",
        artist="Morgan Wallen",
        metadata={"filename": "095. Morgan Wallen - Spin You Around (1_24).mp3"},
    )
    plain_result = SearchResult(
        source="slskd",
        title="Spin You Around",
        artist="Morgan Wallen",
        metadata={"filename": "Morgan Wallen - Spin You Around.mp3"},
    )

    assert runner._targeted_catalog_result_matches(folder_result, target)
    assert runner._targeted_catalog_result_matches(bracket_result, target)
    assert not runner._targeted_catalog_result_matches(plain_result, target)


def test_targeted_catalog_result_rejects_unrequested_amazon_original() -> None:
    target = CatalogAlbumTrack(id=15, position=1, disc=1, title="Valerie")
    result = SearchResult(
        source="slskd",
        title="50 Ty Myers Valerie",
        artist=None,
        metadata={"filename": "50 Ty Myers Valerie (Amazon Music Original).mp3"},
    )

    assert not runner._targeted_catalog_result_matches(result, target)


async def test_queries_for_job_uses_recording_artist_credit_collaborators(
    test_settings: Settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    artist = CatalogArtist(name="Morgan Wallen")
    album = CatalogAlbum(id=22, title="Miami", track_count=1)
    track = CatalogAlbumTrack(
        id=33, position=1, disc=1, title="Miami", recording_mbid="recording-miami"
    )
    artist.albums.append(album)
    album.tracks.append(track)
    job = Job(
        source="slskd",
        query="Morgan Wallen Miami",
        catalog_album_id=album.id,
        catalog_track_id=track.id,
    )

    class FakeMeta:
        title = "Miami"
        artist = "Morgan Wallen feat. Lil Wayne & Rick Ross"

    class FakeMusicBrainzClient:
        def __init__(self, user_agent: str) -> None:
            assert user_agent

        async def lookup_recording(self, mbid: str) -> FakeMeta:
            assert mbid == "recording-miami"
            return FakeMeta()

    monkeypatch.setattr(runner, "MusicBrainzClient", FakeMusicBrainzClient)

    assert await runner._queries_for_job(job, album, test_settings) == [
        "Morgan Wallen Miami Lil Wayne Rick Ross",
        "Morgan Wallen Miami",
    ]


def test_targeted_catalog_result_rejects_contradictory_titles() -> None:
    target = CatalogAlbumTrack(id=7, position=11, disc=1, title="Me to Me")
    wrong = SearchResult(
        source="slskd",
        title="Whiskey Glasses",
        artist="Morgan Wallen",
        metadata={"filename": "Billboard Top 100 2019\\52 Whiskey Glasses.mp3"},
    )
    valid = SearchResult(
        source="slskd",
        title="Me to Me",
        artist="Morgan Wallen",
        metadata={"filename": "11 - Morgan Wallen - Me To Me.mp3"},
    )

    assert runner._targeted_catalog_result_matches(wrong, target) is False
    assert runner._targeted_catalog_result_matches(valid, target) is True


def test_targeted_catalog_result_rejects_different_track_from_same_album() -> None:
    target = CatalogAlbumTrack(id=9, position=3, disc=1, title="Dangerous")
    wrong = SearchResult(
        source="slskd",
        title="Sand in My Boots",
        artist="Morgan Wallen",
        metadata={"filename": "Dangerous_ The Double Album\\1-01 Sand in My Boots.mp3"},
    )

    assert runner._targeted_catalog_result_matches(wrong, target) is False


def test_targeted_catalog_result_preserves_identity_changing_versions() -> None:
    studio = CatalogAlbumTrack(id=10, position=1, disc=1, title="Song")
    live = CatalogAlbumTrack(id=11, position=1, disc=1, title="Song (Live)")
    live_result = SearchResult(
        source="slskd",
        title="Song",
        artist="Artist",
        metadata={"filename": "01 - Artist - Song (Live).flac"},
    )
    studio_result = SearchResult(
        source="slskd",
        title="Song",
        artist="Artist",
        metadata={"filename": "01 - Artist - Song [FLAC].flac"},
    )

    assert runner._targeted_catalog_result_matches(live_result, studio) is False
    assert runner._targeted_catalog_result_matches(live_result, live) is True
    assert runner._targeted_catalog_result_matches(studio_result, studio) is True


async def test_fingerprint_verification_uses_measured_not_catalog_duration(
    db_session: AsyncSession,
    test_settings: Settings,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    source = tmp_path / "wrong.mp3"
    source.write_bytes(b"audio")
    job = Job(source="slskd", query="target", status=JobStatus.running)
    release = Release(job=job, source="slskd", title="Album", album_artist="Artist")
    track = Track(
        job=job,
        release=release,
        source="slskd",
        title="Target",
        duration_sec=139,
        source_path=str(source),
        mbid="expected-mbid",
        identity_state=IdentityResolutionState.resolved,
    )
    db_session.add_all([job, release, track])
    await db_session.flush()
    observed: dict[str, object] = {}

    async def fake_fingerprint(path):
        assert path == source
        return 234, "fingerprint"

    async def fake_lookup(duration: int, fingerprint: str, api_key: str):
        observed["lookup_duration"] = duration
        assert fingerprint == "fingerprint"
        assert api_key
        return []

    async def fake_verify(track_arg, *, fingerprint_duration_sec, **kwargs):
        observed["review_duration"] = fingerprint_duration_sec
        return AcoustIDVerificationState.unavailable

    monkeypatch.setattr(runner, "fingerprint_file", fake_fingerprint)
    monkeypatch.setattr(runner, "_lookup_acoustid_raw", fake_lookup)
    monkeypatch.setattr(
        "app.services.acoustid_verification.run_acoustid_verification", fake_verify
    )
    test_settings.acoustid_api_key = "configured"

    await runner._run_fingerprint_and_verify(track, test_settings, db_session)

    assert track.duration_sec == 139
    assert observed == {"lookup_duration": 234, "review_duration": 234}


async def test_targeted_catalog_mismatch_is_rejected_before_acquisition(
    db_session: AsyncSession,
    test_settings: Settings,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    artist = CatalogArtist(name="Morgan Wallen")
    album = CatalogAlbum(title="Dangerous: The Double Album", track_count=1)
    target = CatalogAlbumTrack(position=3, disc=1, title="Dangerous")
    artist.albums.append(album)
    album.tracks.append(target)
    db_session.add(artist)
    await db_session.flush()
    job = Job(
        source="slskd",
        query="Morgan Wallen Dangerous",
        status=JobStatus.pending,
        catalog_album_id=album.id,
        catalog_track_id=target.id,
    )
    db_session.add(job)
    await db_session.flush()
    acquisition_calls = 0

    async def fake_fetch(*args: object, **kwargs: object) -> Sequence[SearchResult]:
        return [
            SearchResult(
                source="slskd",
                title="Sand in My Boots",
                artist="Morgan Wallen",
                album="Dangerous: The Double Album",
                metadata={
                    "username": "peer",
                    "filename": "Dangerous_ The Double Album\\1-01 Sand in My Boots.mp3",
                },
            )
        ]

    async def fail_if_acquired(*args: object, **kwargs: object) -> tuple[None, None]:
        nonlocal acquisition_calls
        acquisition_calls += 1
        return None, None

    async def no_continuation(*args: object, **kwargs: object) -> None:
        return None

    monkeypatch.setattr(runner, "_fetch_results", fake_fetch)
    monkeypatch.setattr(runner, "_prepare_acquisition", fail_if_acquired)
    monkeypatch.setattr(runner, "_spawn_continuation_jobs", no_continuation)

    await runner.run_job(job.id, db_session, test_settings)

    tracks = list((await db_session.scalars(select(Track).where(Track.job_id == job.id))).all())
    assert acquisition_calls == 0
    assert tracks == []
    assert job.status == JobStatus.partial


async def test_run_job_marks_failed_when_result_processing_fails(
    db_session: AsyncSession, test_settings: Settings, monkeypatch: object
) -> None:
    from pytest import MonkeyPatch

    mp = monkeypatch
    assert isinstance(mp, MonkeyPatch)
    job = await _create_job(db_session)

    async def fake_fetch_results(job: Job, cfg: Settings) -> Sequence[SearchResult]:
        return [
            SearchResult(source="youtube", title="Song", artist="Artist", url="/tmp/song.flac")
        ]

    async def fail_musicbrainz(track: Track, cfg: Settings) -> None:
        raise RuntimeError("metadata boom")

    async def noop_acquisition(
        result: SearchResult, source: str, cfg: Settings, track: Track | None = None
    ) -> tuple[None, None]:
        return None, None

    mp.setattr(runner, "_fetch_results", fake_fetch_results)
    mp.setattr(runner, "_prepare_acquisition", noop_acquisition)
    mp.setattr(runner, "_enrich_musicbrainz", fail_musicbrainz)

    await runner.run_job(job.id, db_session, test_settings)

    assert job.status == JobStatus.failed
    assert job.result_json is not None
    assert "result_processing_failed" in job.result_json


async def test_background_cleanup_failure_does_not_rewrite_committed_success(
    db_session: AsyncSession, test_settings: Settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    from app.services import acquisition_cleanup

    job = await _create_job(db_session)
    await db_session.commit()
    job_id = job.id

    async def complete_job(
        current_job_id: int,
        session: AsyncSession,
        cfg: Settings,
        *,
        commit_progress: bool = False,
    ) -> None:
        assert current_job_id == job_id
        assert commit_progress is True
        current = await session.get(Job, current_job_id)
        assert current is not None
        current.status = JobStatus.done

    async def fail_cleanup(*args: object, **kwargs: object) -> tuple[list[int], int]:
        raise RuntimeError("cleanup unavailable")

    factory = async_sessionmaker(db_session.bind, expire_on_commit=False)
    monkeypatch.setattr(runner, "get_session_factory", lambda: factory)
    monkeypatch.setattr(runner, "_run_job_in_session", complete_job)
    monkeypatch.setattr(acquisition_cleanup, "cleanup_terminal_acquisitions", fail_cleanup)

    await runner.run_job(job_id, settings=test_settings)

    db_session.expire_all()
    persisted = await db_session.get(Job, job_id)
    assert persisted is not None
    assert persisted.status == JobStatus.done


async def test_provider_error_persists_typed_failure_without_secret(
    db_session: AsyncSession, test_settings: Settings, monkeypatch: object, caplog: object
) -> None:
    from pytest import LogCaptureFixture, MonkeyPatch

    assert isinstance(monkeypatch, MonkeyPatch)
    assert isinstance(caplog, LogCaptureFixture)
    job = await _create_job(db_session)

    async def fail(job: Job, cfg: Settings) -> Sequence[SearchResult]:
        raise ProviderError("timeout", "secret URL https://x/?token=bad", "search", True)

    monkeypatch.setattr(runner, "_fetch_results", fail)
    await runner.run_job(job.id, db_session, test_settings)
    assert job.status == JobStatus.failed
    assert (
        job.result_json
        == '{"error": {"code": "timeout", "operation": "search", "retryable": true}}'
    )
    assert "secret" not in caplog.text


async def test_cancellation_persists_job_and_track_state(
    db_session: AsyncSession, test_settings: Settings, monkeypatch: object
) -> None:
    from pytest import MonkeyPatch

    assert isinstance(monkeypatch, MonkeyPatch)
    job = await _create_job(db_session)
    track = Track(job_id=job.id, source="youtube", acquisition_state=AcquisitionState.acquiring)
    db_session.add(track)

    async def cancel(job: Job, cfg: Settings) -> Sequence[SearchResult]:
        raise asyncio.CancelledError

    monkeypatch.setattr(runner, "_fetch_results", cancel)
    with pytest.raises(asyncio.CancelledError):
        await runner.run_job(job.id, db_session, test_settings)
    assert job.status == JobStatus.cancelled
    assert track.acquisition_state == AcquisitionState.cancelled


async def test_prowlarr_result_is_enqueued_to_sabnzbd(
    db_session: AsyncSession, test_settings: Settings, monkeypatch: object, tmp_path: object
) -> None:
    from pathlib import Path

    from pytest import MonkeyPatch

    mp = monkeypatch
    assert isinstance(mp, MonkeyPatch)
    assert isinstance(tmp_path, Path)
    job = await _create_job(db_session, source="prowlarr")
    staging = tmp_path / "staging"
    staging.mkdir()
    audio_file = staging / "track.flac"
    audio_file.write_bytes(b"audio")
    test_settings = test_settings.model_copy(
        update={"prowlarr_url": "https://prowlarr.test", "staging_root": staging}
    )
    calls: list[str] = []

    async def fake_fetch_results(job: Job, cfg: Settings) -> Sequence[SearchResult]:
        return [
            SearchResult(
                source="prowlarr",
                title="Artist - Album",
                url="https://prowlarr.test/download/file.nzb",
                format="nzb",
            )
        ]

    async def noop(track: Track, cfg: Settings) -> None:
        return None

    async def noop_preview(track: Track, db: AsyncSession, cfg: Settings) -> None:
        return None

    class FakeSabnzbdAdapter:
        def __init__(self, base_url: str, api_key: str) -> None:
            calls.append(f"configured:{base_url}:{api_key}")

        async def enqueue(self, nzb_url: str, name: str | None = None) -> str:
            calls.append(f"enqueue:{nzb_url}:{name}")
            return "SAB123"

    async def fake_poll_sab_job(
        nzo_id: str,
        adapter: object,
        staging_root: Path,
        poll_interval: float,
        poll_timeout: float,
    ) -> Path:
        calls.append(f"poll:{nzo_id}")
        return audio_file

    mp.setattr(runner, "_fetch_results", fake_fetch_results)
    mp.setattr(runner, "_enrich_musicbrainz", noop)
    mp.setattr(runner, "_enrich_deezer", noop)
    mp.setattr(runner, "_run_fingerprint", noop)
    mp.setattr(runner, "_compute_path_preview", noop_preview)
    mp.setattr(runner, "SabnzbdAdapter", FakeSabnzbdAdapter)
    mp.setattr(runner, "_poll_sab_job", fake_poll_sab_job)

    await runner.run_job(job.id, db_session, test_settings)

    track = (await db_session.execute(select(Track))).scalar_one()
    assert job.status == JobStatus.done
    assert track.source_job_id == "SAB123"
    assert track.source_status == "downloaded"
    assert "enqueue:https://prowlarr.test/download/file.nzb:Artist - Album" in calls
    assert "poll:SAB123" in calls


async def test_path_preview_naming_error_marks_job_failed(
    db_session: AsyncSession, test_settings: Settings, monkeypatch: object
) -> None:
    from pytest import MonkeyPatch

    mp = monkeypatch
    assert isinstance(mp, MonkeyPatch)
    job = await _create_job(db_session)

    async def fake_fetch_results(job: Job, cfg: Settings) -> Sequence[SearchResult]:
        return [
            SearchResult(source="youtube", title="Song", artist="Artist", url="/tmp/song.flac")
        ]

    async def noop(track: Track, cfg: Settings) -> None:
        return None

    def fail_render_path(*args: object, **kwargs: object) -> str:
        raise NamingError("bad naming")

    async def noop_acquisition(
        result: SearchResult, source: str, cfg: Settings, track: Track | None = None
    ) -> tuple[None, None]:
        return None, None

    mp.setattr(runner, "_fetch_results", fake_fetch_results)
    mp.setattr(runner, "_prepare_acquisition", noop_acquisition)
    mp.setattr(runner, "_enrich_musicbrainz", noop)
    mp.setattr(runner, "_enrich_deezer", noop)
    mp.setattr(runner, "_run_fingerprint", noop)
    mp.setattr(runner, "render_path", fail_render_path)

    await runner.run_job(job.id, db_session, test_settings)

    assert job.status == JobStatus.failed
    assert job.result_json is not None
    assert "result_processing_failed" in job.result_json


async def test_prowlarr_rejects_non_nzb_and_loopback_urls(
    test_settings: Settings, monkeypatch: object
) -> None:
    from pytest import MonkeyPatch

    mp = monkeypatch
    assert isinstance(mp, MonkeyPatch)
    test_settings = test_settings.model_copy(update={"prowlarr_url": "https://prowlarr.test"})
    calls: list[str] = []

    class FakeSabnzbdAdapter:
        def __init__(self, base_url: str, api_key: str) -> None:
            pass

        async def enqueue(self, nzb_url: str, name: str | None = None) -> str:
            calls.append(nzb_url)
            return "SAB123"

        async def status(self, nzo_id: str) -> CapabilityState:
            return CapabilityState(available=True, reason="Downloading")

    mp.setattr(runner, "SabnzbdAdapter", FakeSabnzbdAdapter)
    invalid_results = [
        SearchResult(
            source="prowlarr",
            title="Magnet",
            url="magnet:?xt=urn:btih:abc",
            format="nzb",
        ),
        SearchResult(
            source="prowlarr",
            title="Localhost",
            url="http://127.0.0.1/file.nzb",
            format="nzb",
        ),
        SearchResult(
            source="prowlarr",
            title="Html",
            url="https://indexer.local/file.html",
            format="html",
        ),
        SearchResult(
            source="prowlarr",
            title="Untrusted DNS host",
            url="https://attacker.example/file.nzb",
            format="nzb",
        ),
    ]

    for result in invalid_results:
        try:
            await runner._prepare_acquisition(result, "prowlarr", test_settings)
        except RuntimeError as exc:
            assert "NZB" in str(exc) or "URL" in str(exc)
        else:
            raise AssertionError(f"accepted invalid result: {result.url}")

    assert calls == []


async def test_musicbrainz_empty_result_marks_track_unresolved(
    db_session: AsyncSession, test_settings: Settings, monkeypatch: object
) -> None:
    from pytest import MonkeyPatch

    mp = monkeypatch
    assert isinstance(mp, MonkeyPatch)
    job = await _create_job(db_session)
    track = Track(job_id=job.id, title="No Match", source="youtube")
    db_session.add(track)
    await db_session.flush()

    class EmptyMusicBrainzClient:
        def __init__(self, user_agent: str) -> None:
            pass

        async def search_recording(
            self, title: str, artist: str | None = None, album: str | None = None
        ) -> list[object]:
            return []

    mp.setattr(runner, "MusicBrainzClient", EmptyMusicBrainzClient)

    await runner._enrich_musicbrainz(track, test_settings)

    assert track.mbid is None
    assert track.identity_state == IdentityResolutionState.unresolved


async def test_run_job_uses_database_backed_effective_settings(
    db_session: AsyncSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    job = await _create_job(db_session)
    expected = Settings(secret_key="effective-secret", slskd_url="http://db-slskd")
    observed: list[Settings] = []

    async def _effective(db: AsyncSession, env: Settings) -> Settings:
        assert db is db_session
        return expected

    async def _run(job_id: int, db: AsyncSession, cfg: Settings) -> None:
        observed.append(cfg)

    monkeypatch.setattr(runner, "build_effective_settings", _effective)
    monkeypatch.setattr(runner, "_run_job_in_session", _run)
    await runner.run_job(job.id, db_session)
    assert observed == [expected]


async def test_priority_job_falls_back_when_first_source_empty(
    db_session: AsyncSession, test_settings: Settings, monkeypatch: object
) -> None:
    from pytest import MonkeyPatch

    from app.settings_service import save_runtime_settings

    assert isinstance(monkeypatch, MonkeyPatch)
    await save_runtime_settings(
        db_session,
        [{"name": "slskd", "enabled": True}, {"name": "youtube", "enabled": True}],
        10,
        metadata_providers=[{"name": "musicbrainz", "enabled": True}],
        primary_metadata_provider="musicbrainz",
    )
    job = Job(source="priority", query="artist album", status=JobStatus.pending)
    db_session.add(job)
    await db_session.flush()
    calls: list[str] = []

    class FakeAdapter:
        def __init__(self, source: str) -> None:
            self.source = source

        async def health(self) -> CapabilityState:
            return CapabilityState(available=True)

        async def search(self, request: object) -> Sequence[SearchResult]:
            calls.append(self.source)
            if self.source == "slskd":
                return []
            return [SearchResult(source="youtube", title="Served", url="https://youtu.be/1")]

    monkeypatch.setattr(runner, "_source_adapter", lambda source, cfg: FakeAdapter(source))

    results = await runner._fetch_results(job, test_settings, db_session)

    assert calls == ["slskd", "youtube"]
    assert job.source == "youtube"
    assert results[0].source == "youtube"
    assert '"attempted_sources": ["slskd", "youtube"]' in (job.result_json or "")
    assert '"served_source": "youtube"' in (job.result_json or "")


async def test_priority_job_falls_back_when_first_source_unhealthy(
    db_session: AsyncSession, test_settings: Settings, monkeypatch: object
) -> None:
    from pytest import MonkeyPatch

    from app.settings_service import save_runtime_settings

    assert isinstance(monkeypatch, MonkeyPatch)
    await save_runtime_settings(
        db_session,
        [{"name": "slskd", "enabled": True}, {"name": "youtube", "enabled": True}],
        10,
        metadata_providers=[{"name": "musicbrainz", "enabled": True}],
        primary_metadata_provider="musicbrainz",
    )
    job = Job(source="priority", query="artist album", status=JobStatus.pending)
    db_session.add(job)
    await db_session.flush()

    class FakeAdapter:
        def __init__(self, source: str) -> None:
            self.source = source

        async def health(self) -> CapabilityState:
            if self.source == "slskd":
                return CapabilityState(available=False, reason="offline")
            return CapabilityState(available=True)

        async def search(self, request: object) -> Sequence[SearchResult]:
            return [SearchResult(source=self.source, title="Served", url="https://youtu.be/1")]

    monkeypatch.setattr(runner, "_source_adapter", lambda source, cfg: FakeAdapter(source))

    results = await runner._fetch_results(job, test_settings, db_session)

    assert job.source == "youtube"
    assert results[0].source == "youtube"
    assert '"status": "unhealthy"' in (job.result_json or "")


async def test_provider_fallback_releases_sqlite_writer_before_next_network_call(
    tmp_path,
    test_settings: Settings,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from app.settings_service import save_runtime_settings

    database_path = tmp_path / "provider-fallback.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{database_path}")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with factory() as session:
            await save_runtime_settings(
                session,
                [
                    {"name": "slskd", "enabled": True},
                    {"name": "youtube", "enabled": True},
                ],
                10,
                metadata_providers=[{"name": "musicbrainz", "enabled": True}],
                primary_metadata_provider="musicbrainz",
            )
            job = Job(source="priority", query="artist album", status=JobStatus.running)
            session.add(job)
            await session.commit()

            class FakeAdapter:
                def __init__(self, source: str) -> None:
                    self.source = source

                async def health(self) -> CapabilityState:
                    if self.source == "youtube":
                        with sqlite3.connect(database_path, timeout=0.1) as concurrent:
                            concurrent.execute("BEGIN IMMEDIATE")
                            concurrent.rollback()
                    return CapabilityState(available=True)

                async def search(self, request: object) -> Sequence[SearchResult]:
                    if self.source == "slskd":
                        return []
                    return [
                        SearchResult(source="youtube", title="Served", url="https://youtu.be/1")
                    ]

            monkeypatch.setattr(runner, "_source_adapter", lambda source, cfg: FakeAdapter(source))

            results = await runner._fetch_results(
                job,
                test_settings,
                session,
                checkpoint=session.commit,
            )

            assert results[0].source == "youtube"
    finally:
        await engine.dispose()


async def test_fetch_hook_internal_typeerror_is_not_retried(
    db_session: AsyncSession, test_settings: Settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls = 0

    async def failing_hook(
        job: Job,
        cfg: Settings,
        db: AsyncSession,
        *,
        checkpoint=None,
    ) -> list[SearchResult]:
        nonlocal calls
        calls += 1
        raise TypeError("argument of type 'NoneType' is not iterable")

    monkeypatch.setattr(runner, "_fetch_results", failing_hook)
    job = Job(source="priority", query="artist album", status=JobStatus.running)

    with pytest.raises(TypeError, match="NoneType"):
        await runner._call_fetch_results(
            job, test_settings, db_session, checkpoint=db_session.commit
        )

    assert calls == 1


async def test_catalog_track_slskd_query_uses_track_title_not_album_title(
    db_session: AsyncSession, test_settings: Settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    from app.settings_service import save_runtime_settings

    await save_runtime_settings(
        db_session,
        [{"name": "slskd", "enabled": True}],
        10,
        metadata_providers=[{"name": "musicbrainz", "enabled": True}],
        primary_metadata_provider="musicbrainz",
    )
    artist = CatalogArtist(name="Morgan Wallen")
    album = CatalogAlbum(artist=artist, title="Miami - Single", year="2025", track_count=1)
    track = CatalogAlbumTrack(
        album=album,
        disc=1,
        position=1,
        title="Miami (feat. Lil Wayne & Rick Ross)",
    )
    db_session.add_all([artist, album, track])
    await db_session.flush()
    job = Job(
        source="slskd",
        query="Morgan Wallen Miami - Single",
        status=JobStatus.pending,
        catalog_album_id=album.id,
        catalog_track_id=track.id,
    )
    db_session.add(job)
    await db_session.flush()
    queries: list[str] = []

    class FakeAdapter:
        async def search(self, request: object) -> Sequence[SearchResult]:
            queries.append(request.query)
            return [
                SearchResult(
                    source="slskd",
                    title="Miami (feat. Lil Wayne & Rick Ross)",
                    artist="Morgan Wallen",
                    url="slskd://peer/Miami.mp3",
                    metadata={
                        "username": "peer",
                        "filename": "Miami (feat. Lil Wayne & Rick Ross).mp3",
                    },
                )
            ]

    monkeypatch.setattr(runner, "_source_adapter", lambda source, cfg: FakeAdapter())

    await runner._fetch_results(job, test_settings, db_session)

    assert queries == ["Morgan Wallen Miami (feat. Lil Wayne & Rick Ross)"]


def test_targeted_slskd_match_keeps_featured_artists_distinct() -> None:
    target = CatalogAlbumTrack(
        id=42, disc=1, position=1, title="Miami (feat. Lil Wayne & Rick Ross)"
    )
    plain_result = SearchResult(
        source="slskd",
        title="Miami",
        artist="Morgan Wallen",
        url="slskd://peer/Miami.mp3",
        metadata={"filename": "Morgan Wallen - Miami.mp3"},
    )
    featured_result = SearchResult(
        source="slskd",
        title="Miami (feat. Lil Wayne & Rick Ross)",
        artist="Morgan Wallen",
        url="slskd://peer/Miami-featured.mp3",
        metadata={"filename": "Morgan Wallen - Miami (feat. Lil Wayne & Rick Ross).mp3"},
    )

    assert not runner._targeted_catalog_result_matches(plain_result, target)
    assert runner._targeted_catalog_result_matches(featured_result, target)


async def test_priority_job_records_clear_failure_when_all_sources_exhausted(
    db_session: AsyncSession, test_settings: Settings, monkeypatch: object
) -> None:
    from pytest import MonkeyPatch

    from app.settings_service import save_runtime_settings

    assert isinstance(monkeypatch, MonkeyPatch)
    await save_runtime_settings(
        db_session,
        [{"name": "slskd", "enabled": True}, {"name": "youtube", "enabled": True}],
        10,
        metadata_providers=[{"name": "musicbrainz", "enabled": True}],
        primary_metadata_provider="musicbrainz",
    )
    job = Job(source="priority", query="artist album", status=JobStatus.pending)
    db_session.add(job)
    await db_session.flush()

    class FakeAdapter:
        def __init__(self, source: str) -> None:
            self.source = source

        async def health(self) -> CapabilityState:
            return CapabilityState(available=True)

        async def search(self, request: object) -> Sequence[SearchResult]:
            if self.source == "slskd":
                return []
            raise ProviderError("timeout", "provider timed out", "search", True)

    monkeypatch.setattr(runner, "_source_adapter", lambda source, cfg: FakeAdapter(source))

    await runner.run_job(job.id, db_session, test_settings)

    assert job.status == JobStatus.failed
    assert job.source == "priority"
    assert '"status": "empty"' in (job.result_json or "")
    assert '"code": "timeout"' in (job.result_json or "")
    assert '"code": "sources_exhausted"' in (job.result_json or "")


async def test_targeted_search_uses_simplified_fallback_after_empty_result(
    db_session: AsyncSession, test_settings: Settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    from app.settings_service import save_runtime_settings

    await save_runtime_settings(
        db_session,
        [{"name": "youtube", "enabled": True}],
        10,
        metadata_providers=[{"name": "musicbrainz", "enabled": True}],
        primary_metadata_provider="musicbrainz",
    )
    artist = CatalogArtist(name="Ty Myers")
    album = CatalogAlbum(artist=artist, title="The Select (Deluxe)", track_count=1)
    track = CatalogAlbumTrack(album=album, position=1, disc=1, title="Firefly (Acoustic)")
    db_session.add_all([artist, album, track])
    await db_session.flush()
    job = Job(
        source="youtube",
        query="legacy query",
        status=JobStatus.pending,
        catalog_album_id=album.id,
        catalog_track_id=track.id,
    )
    db_session.add(job)
    await db_session.flush()
    queries: list[str] = []

    class FakeAdapter:
        async def search(self, request: SearchRequest) -> Sequence[SearchResult]:
            query = request.query
            queries.append(query)
            if query.endswith("(Acoustic)"):
                return []
            return [SearchResult(source="youtube", title="Firefly", url="https://example.test")]

    monkeypatch.setattr(runner, "_source_adapter", lambda source, cfg: FakeAdapter())
    results = await runner._fetch_results(job, test_settings, db_session)

    assert queries == ["Ty Myers Firefly (Acoustic)", "Ty Myers Firefly"]
    assert len(results) == 1
    assert '"query": "Ty Myers Firefly (Acoustic)"' in (job.result_json or "")
    assert '"query": "Ty Myers Firefly"' in (job.result_json or "")


async def test_album_prowlarr_results_are_candidates_not_tracks(
    db_session: AsyncSession,
    test_settings: Settings,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    artist = CatalogArtist(name="Artist")
    album = CatalogAlbum(
        title="Album",
        year="2024",
        mbid="00000000-0000-0000-0000-000000000001",
        track_count=1,
    )
    artist.albums.append(album)
    catalog_track = CatalogAlbumTrack(position=1, disc=1, title="Song")
    album.tracks.append(catalog_track)
    db_session.add(artist)
    await db_session.flush()
    job = Job(
        source="prowlarr",
        query="Artist Album",
        status=JobStatus.pending,
        catalog_album_id=album.id,
    )
    db_session.add(job)
    await db_session.flush()
    acquired: list[str] = []

    async def fake_fetch(*args: object, **kwargs: object) -> Sequence[SearchResult]:
        return [
            SearchResult(source="prowlarr", title="Song", url="https://indexer/one.nzb"),
            SearchResult(source="prowlarr", title="Other release", url="https://indexer/two.nzb"),
        ]

    async def fake_acquire(
        result: SearchResult,
        source: str,
        cfg: Settings,
        track: Track | None = None,
    ) -> tuple[str, str]:
        acquired.append(result.url)
        return "sab-1", "downloaded"

    async def noop_track(track: Track, cfg: Settings) -> None:
        return None

    async def noop_preview(track: Track, db: AsyncSession, cfg: Settings) -> None:
        return None

    monkeypatch.setattr(runner, "_fetch_results", fake_fetch)
    monkeypatch.setattr(runner, "_prepare_acquisition", fake_acquire)
    monkeypatch.setattr(runner, "_enrich_musicbrainz", noop_track)
    monkeypatch.setattr(runner, "_enrich_deezer", noop_track)
    monkeypatch.setattr(runner, "_run_fingerprint", noop_track)
    monkeypatch.setattr(runner, "_compute_path_preview", noop_preview)

    await runner.run_job(job.id, db_session, test_settings)

    tracks = list((await db_session.scalars(select(Track).where(Track.job_id == job.id))).all())
    release = (await db_session.scalars(select(Release).where(Release.job_id == job.id))).one()
    assert acquired == ["https://indexer/one.nzb"]
    assert len(tracks) == 1
    assert tracks[0].catalog_track_id == catalog_track.id
    assert tracks[0].identity_state == IdentityResolutionState.unresolved
    assert release.release_mbid == album.mbid
    assert release.year == "2024"
    assert release.track_count == 1


def test_catalog_track_matching_never_falls_back_to_result_position() -> None:
    first = CatalogAlbumTrack(id=1, position=1, disc=1, title="First")
    second = CatalogAlbumTrack(id=2, position=2, disc=1, title="Second")
    result = SearchResult(source="prowlarr", title="Unrelated NZB", url="https://indexer/file.nzb")

    matched = runner._catalog_track_for_result(result, [first, second], None)

    assert matched is None


def test_single_track_catalog_match_falls_back_to_only_track() -> None:
    only = CatalogAlbumTrack(id=7, position=1, disc=1, title="AGATS2 (Insecure)")
    result = SearchResult(
        source="slskd",
        title="Juice WRLD AGATS2 01 AGATS2",
        artist="Juice WRLD",
    )

    matched = runner._catalog_track_for_result(result, [only], None)

    assert matched is only


async def test_catalog_album_parent_reconciled_after_continuation_completes(
    db_session: AsyncSession,
) -> None:
    artist = CatalogArtist(name="Juice WRLD")
    album = CatalogAlbum(title="whoa (mind in awe) [Remix]", track_count=1)
    artist.albums.append(album)
    track = CatalogAlbumTrack(position=1, disc=1, title="whoa (mind in awe) (Remix)")
    album.tracks.append(track)
    parent = Job(
        source="priority",
        query="Juice WRLD whoa",
        status=JobStatus.partial,
        result_json=json.dumps({"missing_catalog_track_ids": [999], "missing_tracks": ["whoa"]}),
        catalog_album=album,
    )
    child = Job(
        source="slskd", query="Juice WRLD whoa", status=JobStatus.done, catalog_album=album
    )
    release = Release(job=child, source="slskd", title=album.title, album_artist=artist.name)
    imported = Track(
        job=child,
        release=release,
        catalog_album=album,
        catalog_track=track,
        source="slskd",
        title=track.title,
        artist=artist.name,
        album=album.title,
        import_state=ImportWorkflowState.imported,
    )
    db_session.add_all([artist, parent, child, release, imported])
    await db_session.flush()

    await runner._reconcile_catalog_album_jobs(db_session, album.id, {track.id})

    assert parent.status == JobStatus.done
    payload = json.loads(parent.result_json or "{}")
    assert payload["missing_catalog_track_ids"] == []
    assert payload["missing_tracks"] == []


def test_catalogless_continuation_reuses_track_across_provider_fallback() -> None:
    existing = Track(source="slskd", title="Recovered Song", album="Recovered Album")

    matched = runner._existing_track_for_result(
        [existing], "Recovered Song", "Recovered Album", None
    )

    assert matched is existing


# ---------------------------------------------------------------------------
# Background-path transaction/failure slice tests
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture
async def bg_factory(test_settings: Settings) -> AsyncGenerator[async_sessionmaker, None]:
    """In-memory DB wired as the global session factory for background-path tests."""
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    _db_module._session_factory = factory
    yield factory
    _db_module._session_factory = None
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
    await engine.dispose()


async def test_background_run_second_session_observes_running_while_provider_blocked(
    bg_factory: async_sessionmaker,
    test_settings: Settings,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with bg_factory() as s:
        job = Job(source="youtube", query="test", status=JobStatus.pending)
        s.add(job)
        await s.commit()
        job_id = job.id

    provider_entered = asyncio.Event()

    async def slow_fetch(j: Job, cfg: Settings, db: AsyncSession) -> list[SearchResult]:
        provider_entered.set()
        await asyncio.sleep(60)
        return []

    monkeypatch.setattr(runner, "_fetch_results", slow_fetch)

    run_task = asyncio.create_task(runner.run_job(job_id, settings=test_settings))

    await asyncio.wait_for(provider_entered.wait(), timeout=5.0)

    observed_status: JobStatus | None = None
    async with bg_factory() as s:
        loaded = await s.get(Job, job_id)
        if loaded is not None:
            observed_status = loaded.status

    run_task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await run_task

    assert observed_status == JobStatus.running


async def test_background_settings_exception_persists_failed_not_pending(
    bg_factory: async_sessionmaker,
    test_settings: Settings,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with bg_factory() as s:
        job = Job(source="youtube", query="test", status=JobStatus.pending)
        s.add(job)
        await s.commit()
        job_id = job.id

    async def boom(db: AsyncSession, env: Settings) -> Settings:
        raise RuntimeError("settings DB exploded")

    monkeypatch.setattr(runner, "build_effective_settings", boom)

    await runner.run_job(job_id, settings=None)

    async with bg_factory() as s:
        loaded = await s.get(Job, job_id)
        assert loaded is not None
        assert loaded.status == JobStatus.failed
        assert loaded.result_json is not None
        assert "settings_error" in loaded.result_json


async def test_background_unexpected_exception_persists_failed_not_pending(
    bg_factory: async_sessionmaker,
    test_settings: Settings,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with bg_factory() as s:
        job = Job(source="youtube", query="test", status=JobStatus.pending)
        s.add(job)
        await s.commit()
        job_id = job.id

    async def explode(j: Job, cfg: Settings, db: AsyncSession) -> list[SearchResult]:
        raise RuntimeError("completely unexpected")

    monkeypatch.setattr(runner, "_fetch_results", explode)

    await runner.run_job(job_id, settings=test_settings)

    async with bg_factory() as s:
        loaded = await s.get(Job, job_id)
        assert loaded is not None
        assert loaded.status == JobStatus.failed
        assert loaded.result_json is not None
        assert "job_failed" in loaded.result_json


async def test_background_init_get_raises_persists_failed(
    bg_factory: async_sessionmaker,
    test_settings: Settings,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with bg_factory() as s:
        job = Job(source="youtube", query="test", status=JobStatus.pending)
        s.add(job)
        await s.commit()
        job_id = job.id

    call_count = 0
    _orig_get = AsyncSession.get

    async def failing_first_get(self: AsyncSession, *args: object, **kwargs: object) -> object:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise RuntimeError("DB read failed")
        return await _orig_get(self, *args, **kwargs)

    monkeypatch.setattr(AsyncSession, "get", failing_first_get)

    await runner.run_job(job_id, settings=None)

    async with bg_factory() as s:
        loaded = await s.get(Job, job_id)
        assert loaded is not None
        assert loaded.status == JobStatus.failed
        assert loaded.result_json is not None
        assert "init_error" in loaded.result_json


async def test_background_second_session_sees_running_and_acquiring_track_during_prepare(
    bg_factory: async_sessionmaker,
    test_settings: Settings,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with bg_factory() as s:
        job = Job(source="youtube", query="test", status=JobStatus.pending)
        s.add(job)
        await s.commit()
        job_id = job.id

    prepare_entered = asyncio.Event()

    async def slow_prepare(
        result: SearchResult,
        source: str,
        cfg: Settings,
        track: Track | None = None,
    ) -> tuple[None, None]:
        prepare_entered.set()
        await asyncio.sleep(60)
        return None, None

    async def fake_fetch(j: Job, cfg: Settings, db: AsyncSession) -> list[SearchResult]:
        return [
            SearchResult(source="youtube", title="Song", artist="Artist", url="/tmp/song.flac")
        ]

    async def noop_enrich(track: Track, cfg: Settings) -> None:
        return None

    async def noop_preview(track: Track, db: AsyncSession, cfg: Settings) -> None:
        return None

    monkeypatch.setattr(runner, "_fetch_results", fake_fetch)
    monkeypatch.setattr(runner, "_prepare_acquisition", slow_prepare)
    monkeypatch.setattr(runner, "_enrich_musicbrainz", noop_enrich)
    monkeypatch.setattr(runner, "_enrich_deezer", noop_enrich)
    monkeypatch.setattr(runner, "_run_fingerprint", noop_enrich)
    monkeypatch.setattr(runner, "_compute_path_preview", noop_preview)

    run_task = asyncio.create_task(runner.run_job(job_id, settings=test_settings))
    await asyncio.wait_for(prepare_entered.wait(), timeout=5.0)

    observed_status: JobStatus | None = None
    observed_acq_state: AcquisitionState | None = None
    async with bg_factory() as s:
        loaded = await s.get(Job, job_id)
        if loaded is not None:
            observed_status = loaded.status
        track_rows = list((await s.scalars(select(Track).where(Track.job_id == job_id))).all())
        if track_rows:
            observed_acq_state = track_rows[0].acquisition_state

    run_task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await run_task

    assert observed_status == JobStatus.running
    assert len(track_rows) == 1
    assert observed_acq_state == AcquisitionState.acquiring


async def test_background_phase2_get_raises_persists_failed(
    bg_factory: async_sessionmaker,
    test_settings: Settings,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Phase-2 db.get or commit failure must leave job as failed, never pending."""
    async with bg_factory() as s:
        job = Job(source="youtube", query="test", status=JobStatus.pending)
        s.add(job)
        await s.commit()
        job_id = job.id

    # Phase-1 get succeeds (call_count==1), phase-2 get raises (call_count==2),
    # recovery get succeeds (call_count==3+).
    call_count = 0
    _orig_get = AsyncSession.get

    async def patched_get(self: AsyncSession, *args: object, **kwargs: object) -> object:
        nonlocal call_count
        call_count += 1
        if call_count == 2:
            raise RuntimeError("phase-2 db.get exploded")
        return await _orig_get(self, *args, **kwargs)

    monkeypatch.setattr(AsyncSession, "get", patched_get)

    await runner.run_job(job_id, settings=None)

    async with bg_factory() as s:
        loaded = await s.get(Job, job_id)
        assert loaded is not None
        assert loaded.status == JobStatus.failed
        assert loaded.result_json is not None
        assert "running_transition_error" in loaded.result_json


# ---------------------------------------------------------------------------
# Dispatcher robustness / watchdog tests
# ---------------------------------------------------------------------------


async def test_dispatcher_done_callback_logs_error_on_exception(
    bg_factory: async_sessionmaker,
    test_settings: Settings,
    caplog: pytest.LogCaptureFixture,
) -> None:
    from app.jobs.dispatcher import JobDispatcher

    async with bg_factory() as s:
        job = Job(source="youtube", query="test", status=JobStatus.pending)
        s.add(job)
        await s.commit()
        job_id = job.id

    async def boom(jid: int) -> None:
        raise RuntimeError("dispatcher-boom")

    dispatcher = JobDispatcher(runner=boom, session_factory=bg_factory)
    task = await dispatcher.dispatch(job_id)
    with contextlib.suppress(Exception):
        await asyncio.gather(task, return_exceptions=True)
    await asyncio.sleep(0)  # let callbacks fire

    assert any(str(job_id) in r.message and r.levelname == "ERROR" for r in caplog.records), (
        f"Expected ERROR log for job {job_id}; got: {[r.message for r in caplog.records]}"
    )


async def test_dispatcher_watchdog_first_redispatch_pending(
    bg_factory: async_sessionmaker,
    test_settings: Settings,
) -> None:
    from app.jobs.dispatcher import JobDispatcher

    dispatched: list[int] = []

    async def noop(jid: int) -> None:
        dispatched.append(jid)

    async with bg_factory() as s:
        job = Job(source="youtube", query="test", status=JobStatus.pending)
        job.updated_at = datetime.now(UTC) - timedelta(seconds=600)
        s.add(job)
        await s.commit()
        job_id = job.id

    dispatcher = JobDispatcher(runner=noop, session_factory=bg_factory)
    await dispatcher._watchdog_tick(threshold_seconds=300)
    await asyncio.sleep(0)  # let dispatched task run

    assert job_id in dispatched

    async with bg_factory() as s:
        loaded = await s.get(Job, job_id)
        assert loaded is not None
        assert loaded.result_json is not None
        data = json.loads(loaded.result_json)
        assert "watchdog_recovery" in data
        assert data["watchdog_recovery"]["attempt"] == 1


async def test_dispatcher_watchdog_first_redispatch_running(
    bg_factory: async_sessionmaker,
    test_settings: Settings,
) -> None:
    from app.jobs.dispatcher import JobDispatcher

    dispatched: list[int] = []

    async def noop(jid: int) -> None:
        dispatched.append(jid)

    async with bg_factory() as s:
        job = Job(source="youtube", query="test", status=JobStatus.running)
        job.updated_at = datetime.now(UTC) - timedelta(seconds=600)
        s.add(job)
        await s.commit()
        job_id = job.id

    dispatcher = JobDispatcher(runner=noop, session_factory=bg_factory)
    await dispatcher._watchdog_tick(threshold_seconds=300)
    await asyncio.sleep(0)  # let dispatched task run

    assert job_id in dispatched

    async with bg_factory() as s:
        loaded = await s.get(Job, job_id)
        assert loaded is not None
        assert loaded.status == JobStatus.pending
        data = json.loads(loaded.result_json or "{}")
        assert "watchdog_recovery" in data


async def test_dispatcher_watchdog_recurrence_marks_failed(
    bg_factory: async_sessionmaker,
    test_settings: Settings,
) -> None:
    from app.jobs.dispatcher import JobDispatcher

    dispatched: list[int] = []

    async def noop(jid: int) -> None:
        dispatched.append(jid)

    async with bg_factory() as s:
        job = Job(source="youtube", query="test", status=JobStatus.pending)
        job.updated_at = datetime.now(UTC) - timedelta(seconds=600)
        job.result_json = json.dumps({"watchdog_recovery": {"attempt": 1}})
        s.add(job)
        await s.commit()
        job_id = job.id

    dispatcher = JobDispatcher(runner=noop, session_factory=bg_factory)
    await dispatcher._watchdog_tick(threshold_seconds=300)

    assert job_id not in dispatched

    async with bg_factory() as s:
        loaded = await s.get(Job, job_id)
        assert loaded is not None
        assert loaded.status == JobStatus.failed
        data = json.loads(loaded.result_json or "{}")
        assert data.get("error", {}).get("code") == "dispatch_lost"


async def test_dispatcher_watchdog_active_task_untouched(
    bg_factory: async_sessionmaker,
    test_settings: Settings,
) -> None:
    from app.jobs.dispatcher import JobDispatcher

    dispatched: list[int] = []
    gate = asyncio.Event()

    async def hold(jid: int) -> None:
        dispatched.append(jid)
        await gate.wait()

    async with bg_factory() as s:
        job = Job(source="youtube", query="test", status=JobStatus.running)
        job.updated_at = datetime.now(UTC) - timedelta(seconds=600)
        s.add(job)
        await s.commit()
        job_id = job.id

    dispatcher = JobDispatcher(runner=hold, session_factory=bg_factory)
    task = await dispatcher.dispatch(job_id)
    await asyncio.sleep(0)

    dispatched.clear()
    await dispatcher._watchdog_tick(threshold_seconds=300)

    gate.set()
    await task

    assert job_id not in dispatched

    async with bg_factory() as s:
        loaded = await s.get(Job, job_id)
        assert loaded is not None
        assert loaded.result_json is None or "watchdog_recovery" not in (loaded.result_json or "")


async def test_dispatcher_watchdog_durable_marker_survives_reload(
    bg_factory: async_sessionmaker,
    test_settings: Settings,
) -> None:
    from app.jobs.dispatcher import JobDispatcher

    async with bg_factory() as s:
        job = Job(source="youtube", query="test", status=JobStatus.pending)
        job.updated_at = datetime.now(UTC) - timedelta(seconds=600)
        s.add(job)
        await s.commit()
        job_id = job.id

    async def noop(jid: int) -> None:
        pass

    dispatcher = JobDispatcher(runner=noop, session_factory=bg_factory)
    await dispatcher._watchdog_tick(threshold_seconds=300)

    # Simulate a new dispatcher instance (process restart scenario)
    dispatcher2 = JobDispatcher(runner=noop, session_factory=bg_factory)

    async with bg_factory() as s:
        loaded = await s.get(Job, job_id)
        assert loaded is not None
        # Simulate the job becoming stale again
        loaded.updated_at = datetime.now(UTC) - timedelta(seconds=600)
        await s.commit()

    await dispatcher2._watchdog_tick(threshold_seconds=300)

    async with bg_factory() as s:
        loaded = await s.get(Job, job_id)
        assert loaded is not None
        assert loaded.status == JobStatus.failed
        data = json.loads(loaded.result_json or "{}")
        assert data.get("error", {}).get("code") == "dispatch_lost"


async def test_restarted_job_reuses_downloaded_track_and_release(
    db_session: AsyncSession,
    test_settings: Settings,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    artist = CatalogArtist(name="Artist")
    album = CatalogAlbum(title="Album", track_count=1)
    artist.albums.append(album)
    catalog_track = CatalogAlbumTrack(position=1, disc=1, title="Song")
    album.tracks.append(catalog_track)
    db_session.add(artist)
    await db_session.flush()
    job = Job(
        source="slskd",
        query="Artist Album Song",
        status=JobStatus.pending,
        catalog_album_id=album.id,
        catalog_track_id=catalog_track.id,
    )
    db_session.add(job)
    await db_session.flush()
    fetch_count = 0
    acquisition_count = 0

    async def fake_fetch(*args: object, **kwargs: object) -> Sequence[SearchResult]:
        nonlocal fetch_count
        fetch_count += 1
        return [
            SearchResult(
                source="slskd",
                title="Song" if fetch_count == 1 else "Provider renamed result",
                artist="Artist",
                album="Album",
                metadata={"username": "peer", "filename": "song.flac"},
            )
        ]

    async def fake_acquire(
        result: SearchResult,
        source: str,
        cfg: Settings,
        track: Track | None = None,
        *,
        checkpoint: object | None = None,
    ) -> tuple[str, str]:
        nonlocal acquisition_count
        acquisition_count += 1
        assert track is not None
        track.acquisition_state = AcquisitionState.downloaded
        return "transfer-1", "downloaded"

    async def noop_track(track: Track, cfg: Settings) -> None:
        return None

    async def noop_preview(track: Track, db: AsyncSession, cfg: Settings) -> None:
        return None

    monkeypatch.setattr(runner, "_fetch_results", fake_fetch)
    monkeypatch.setattr(runner, "_prepare_acquisition", fake_acquire)
    monkeypatch.setattr(runner, "_enrich_musicbrainz", noop_track)
    monkeypatch.setattr(runner, "_enrich_deezer", noop_track)
    monkeypatch.setattr(runner, "_run_fingerprint", noop_track)
    monkeypatch.setattr(runner, "_compute_path_preview", noop_preview)

    await runner.run_job(job.id, db_session, test_settings)
    job.status = JobStatus.pending
    await db_session.flush()
    await runner.run_job(job.id, db_session, test_settings)

    tracks = list((await db_session.scalars(select(Track).where(Track.job_id == job.id))).all())
    releases = list(
        (await db_session.scalars(select(Release).where(Release.job_id == job.id))).all()
    )
    assert job.status == JobStatus.done
    assert acquisition_count == 1
    assert len(tracks) == 1
    assert len(releases) == 1
    assert tracks[0].catalog_track_id == catalog_track.id


async def test_background_enqueue_checkpoint_is_visible_before_poll(
    bg_factory: async_sessionmaker,
    test_settings: Settings,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with bg_factory() as s:
        job = Job(source="slskd", query="Artist Song", status=JobStatus.pending)
        s.add(job)
        await s.commit()
        job_id = job.id
        initial_updated_at = job.updated_at

    poll_entered = asyncio.Event()

    async def fake_fetch(j: Job, cfg: Settings, db: AsyncSession) -> list[SearchResult]:
        return [
            SearchResult(
                source="slskd",
                title="Song",
                artist="Artist",
                metadata={"username": "peer", "filename": "song.flac"},
            )
        ]

    class FakeSlskd:
        def __init__(self, url: str, key: str) -> None:
            pass

        async def enqueue(self, username: str, filename: str, size: int | None = None) -> str:
            return "durable-transfer"

    async def blocked_poll(*args: object, **kwargs: object) -> None:
        poll_entered.set()
        await asyncio.sleep(60)

    monkeypatch.setattr(runner, "_fetch_results", fake_fetch)
    monkeypatch.setattr(runner, "SlskdAdapter", FakeSlskd)
    monkeypatch.setattr(runner, "_poll_slskd_transfer", blocked_poll)

    run_task = asyncio.create_task(runner.run_job(job_id, settings=test_settings))
    await asyncio.wait_for(poll_entered.wait(), timeout=5.0)

    async with bg_factory() as s:
        loaded_job = await s.get(Job, job_id)
        track = (await s.scalars(select(Track).where(Track.job_id == job_id))).one()
        assert loaded_job is not None
        assert loaded_job.updated_at != initial_updated_at
        assert track.source_job_id == "durable-transfer"
        assert track.source_status == "acquiring"
        assert track.acquisition_provenance_json is not None

    run_task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await run_task


async def test_denied_slskd_provenance_filters_future_results(
    db_session: AsyncSession, test_settings: Settings
) -> None:
    job = await _create_job(db_session, source="slskd")
    blocked_filename = "music\\done\\country\\44 - Ty Myers - Valerie (Amazon Music Original).mp3"
    denied = Track(
        job_id=job.id,
        source="slskd",
        title="Valerie",
        acquisition_state=AcquisitionState.failed,
        acoustid_verification_state=AcoustIDVerificationState.denied,
        acquisition_provenance_json=json.dumps(
            {
                "source": "slskd",
                "username": "StarCaller",
                "filename": blocked_filename,
            }
        ),
    )
    db_session.add(denied)
    await db_session.flush()

    results = [
        SearchResult(
            source="slskd",
            title="Valerie",
            artist="Ty Myers",
            url="slskd://blocked",
            metadata={"username": "StarCaller", "filename": blocked_filename},
        ),
        SearchResult(
            source="slskd",
            title="Valerie",
            artist="Ty Myers",
            url="slskd://other",
            metadata={"username": "OtherPeer", "filename": "Ty Myers - Valerie.flac"},
        ),
    ]

    filtered = await runner._without_blocked_slskd_results(results, db_session)

    assert [result.url for result in filtered] == ["slskd://other"]


async def test_slskd_acquisition_rejects_lrc_result_before_enqueue(
    test_settings: Settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    enqueued: list[str] = []

    class FakeSlskd:
        def __init__(self, url: str, key: str) -> None:
            pass

        async def enqueue(self, username: str, filename: str, size: int | None = None) -> str:
            enqueued.append(filename)
            return "should-not-enqueue"

    monkeypatch.setattr(runner, "SlskdAdapter", FakeSlskd)

    with pytest.raises(ProviderError) as exc_info:
        await runner._prepare_acquisition(
            SearchResult(
                source="slskd",
                title="Song",
                artist="Artist",
                format="lrc",
                metadata={"username": "peer", "filename": "Artist - Song.lrc"},
            ),
            "slskd",
            test_settings,
        )

    assert exc_info.value.code == "invalid_result"
    assert enqueued == []


async def test_first_complete_album_run_is_done_not_partial(
    db_session: AsyncSession, test_settings: Settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Regression: newly created Release must be in root_release_ids so acquired_ids
    counts all downloaded tracks and the job is marked done, not partial."""
    artist = CatalogArtist(name="The Artist")
    album = CatalogAlbum(artist=artist, title="The Album", track_count=2, year=2024)
    cat_tracks = [
        CatalogAlbumTrack(album=album, position=1, disc=1, title="Track One"),
        CatalogAlbumTrack(album=album, position=2, disc=1, title="Track Two"),
    ]
    job = Job(
        source="slskd",
        query="The Artist The Album",
        status=JobStatus.pending,
        catalog_album=album,
    )
    db_session.add_all([artist, album, *cat_tracks, job])
    await db_session.flush()

    async def fake_fetch_results(j: Job, cfg: Settings, db: AsyncSession) -> list[SearchResult]:
        return [
            SearchResult(
                source="slskd",
                title=t.title,
                artist="The Artist",
                album="The Album",
                url=f"slskd://user/{i:02d} {t.title}.flac",
                metadata={"username": "user", "filename": f"The Album/{i:02d} {t.title}.flac"},
            )
            for i, t in enumerate(cat_tracks, start=1)
        ]

    events: list[str] = []

    async def fake_prepare(
        result: SearchResult,
        source: str,
        cfg: Settings,
        track: Track | None = None,
        *,
        checkpoint=None,
    ) -> tuple[None, str]:
        events.append(f"prepare:{result.title}")
        if track is not None:
            track.source_path = "/staging/track.flac"
            track.staging_path = track.source_path
            track.acquisition_state = AcquisitionState.downloaded
        return None, "downloaded"

    async def noop_mb(track: Track, cfg: Settings) -> None:
        pass

    async def noop_deezer(track: Track, cfg: Settings) -> None:
        pass

    async def noop_fingerprint_verify(track: Track, cfg: Settings, db: AsyncSession) -> None:
        pass

    async def noop_preview(track: Track, db: AsyncSession, cfg: Settings) -> None:
        pass

    async def noop_auto_import(release: Release, db: AsyncSession, cfg: Settings) -> None:
        events.append("import")

    monkeypatch.setattr(runner, "_fetch_results", fake_fetch_results)
    monkeypatch.setattr(runner, "_prepare_acquisition", fake_prepare)
    monkeypatch.setattr(runner, "_enrich_musicbrainz", noop_mb)
    monkeypatch.setattr(runner, "_enrich_deezer", noop_deezer)
    monkeypatch.setattr(runner, "_run_fingerprint_and_verify", noop_fingerprint_verify)
    monkeypatch.setattr(runner, "_compute_path_preview", noop_preview)
    monkeypatch.setattr(runner, "_try_auto_import", noop_auto_import)

    await runner.run_job(job.id, db_session, test_settings)

    assert events == [
        "prepare:Track One",
        "import",
        "prepare:Track Two",
        "import",
        "import",
    ]
    assert job.status == JobStatus.done, (
        f"expected done but got {job.status}; result_json={job.result_json}"
    )
    continuation_jobs = list(
        (await db_session.scalars(select(Job).where(Job.parent_job_id == job.id))).all()
    )
    assert len(continuation_jobs) == 0, "complete first run must not spawn continuation jobs"
