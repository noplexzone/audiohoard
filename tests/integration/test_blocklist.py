from __future__ import annotations

from datetime import UTC, datetime, timedelta

from httpx import AsyncClient
from sqlalchemy import select

from app.database import get_session_factory
from app.models.acquisition_attempt import AcquisitionAttempt
from app.models.catalog_entities import CatalogAlbum, CatalogAlbumTrack, CatalogArtist
from app.models.job import Job, JobStatus
from app.models.release import Release
from app.models.source_candidate_block import SourceCandidateBlock
from app.models.track import Track
from app.models.workflow import AcoustIDVerificationState


async def test_rejected_sources_page_lists_and_allows_source_again(client: AsyncClient) -> None:
    factory = get_session_factory()
    async with factory() as db:
        block = SourceCandidateBlock(
            provider="slskd",
            peer="StarCaller",
            filename="music\\done\\country\\44 - Wrong Track.mp3",
            reason="denied",
        )
        job = Job(source="slskd", query="Wrong Track", status=JobStatus.done)
        release = Release(job=job, source="slskd", title="Album")
        provenance = (
            '{"filename":"music\\done\\country\\44 - Wrong Track.mp3",'
            '"source":"slskd","username":"StarCaller"}'
        )
        track = Track(
            job=job,
            release=release,
            source="slskd",
            title="Wrong Track",
            acoustid_verification_state=AcoustIDVerificationState.denied,
            acquisition_provenance_json=provenance,
        )
        db.add_all([block, job, release, track])
        await db.commit()
        block_id = block.id
        track_id = track.id

    page = await client.get("/blocklist")

    assert page.status_code == 200
    assert "Rejected Sources" in page.text
    assert "exact acquisition artifacts" in page.text
    assert "StarCaller" in page.text
    assert "44 - Wrong Track.mp3" in page.text
    assert "Permanent" in page.text
    assert f'action="/blocklist/{block_id}/allow"' in page.text

    removed = await client.post(f"/blocklist/{block_id}/allow", follow_redirects=False)

    assert removed.status_code == 303
    assert removed.headers["location"] == "/blocklist?allowed=1"
    async with factory() as db:
        rows = (await db.scalars(select(SourceCandidateBlock))).all()
        assert rows == []
        track = await db.get(Track, track_id)
        assert track is not None
        assert track.acoustid_verification_state == AcoustIDVerificationState.denied
        assert track.acquisition_provenance_json == provenance


async def test_rejected_sources_page_shows_related_attempt_context_and_retry(
    client: AsyncClient, monkeypatch
) -> None:
    factory = get_session_factory()
    failed_at = datetime.now(UTC)
    async with factory() as db:
        artist = CatalogArtist(name="Context Artist", monitored=True)
        album = CatalogAlbum(artist=artist, title="Context Album", monitored=True)
        track = CatalogAlbumTrack(album=album, position=1, disc=1, title="Context Track")
        job = Job(
            source="slskd",
            query="Context Artist Context Track",
            status=JobStatus.failed,
            catalog_album=album,
            catalog_track=track,
        )
        db.add_all([artist, job])
        await db.flush()
        attempt = AcquisitionAttempt(
            job=job,
            catalog_album=album,
            catalog_track=track,
            provider="slskd",
            peer="RetryPeer",
            remote_path="folder/Context Track.flac",
            error_code="transfer_timeout",
        )
        block = SourceCandidateBlock(
            provider="slskd",
            peer="RetryPeer",
            filename="folder/Context Track.flac",
            reason="transfer_timeout",
            retry_count=2,
            last_failure_at=failed_at,
            blocked_until=failed_at + timedelta(minutes=30),
        )
        db.add_all([attempt, block])
        await db.commit()
        block_id = block.id
        job_id = job.id

    page = await client.get("/blocklist?status=temporary&provider=slskd")

    assert page.status_code == 200
    assert "Context Artist" in page.text
    assert "Context Album" in page.text
    assert "Context Track" in page.text
    assert "Temporary" in page.text
    assert "Retry eligible" in page.text
    assert f'href="/albums/{album.id}"' in page.text
    assert f'href="/jobs/{job_id}"' in page.text
    assert "Search alternatives" in page.text

    retried: list[int] = []

    async def fake_retry(selected_job_id: int) -> None:
        retried.append(selected_job_id)

    monkeypatch.setattr("app.routers.blocklist.job_dispatcher.retry", fake_retry)
    response = await client.post(f"/blocklist/{block_id}/allow-retry", follow_redirects=False)

    assert response.status_code == 303
    assert response.headers["location"] == "/blocklist?allowed=1&retried=1"
    assert retried == [job_id]
    async with factory() as db:
        assert await db.get(SourceCandidateBlock, block_id) is None


async def test_rejected_sources_page_is_paginated_and_expired_temporary_rows_are_usable(
    client: AsyncClient,
) -> None:
    factory = get_session_factory()
    now = datetime.now(UTC)
    async with factory() as db:
        db.add_all(
            [
                SourceCandidateBlock(
                    provider="slskd",
                    peer=f"Peer {index:02d}",
                    filename=f"file-{index:02d}.flac",
                    reason="timeout",
                    retry_count=1,
                    last_failure_at=now - timedelta(hours=1),
                    blocked_until=now - timedelta(minutes=45),
                )
                for index in range(3)
            ]
        )
        await db.commit()

    page = await client.get("/blocklist?per_page=2&page=1")

    assert page.status_code == 200
    assert "Page 1 of 2" in page.text
    assert page.text.count('<span class="badge ok">Eligible now</span>') == 2
    assert "per_page=2" in page.text
    assert "page=2" in page.text


async def test_rejected_sources_page_empty_state(client: AsyncClient) -> None:
    page = await client.get("/blocklist")

    assert page.status_code == 200
    assert "No rejected sources" in page.text
