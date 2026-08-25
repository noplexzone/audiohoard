from __future__ import annotations

from html.parser import HTMLParser

from httpx import ASGITransport, AsyncClient
from sqlalchemy import func, select

import app.database as db_module
from app.models.catalog_entities import (
    CatalogAlbum,
    CatalogAlbumTrack,
    CatalogArtist,
    CatalogArtistIdentity,
)
from app.models.discography_batch import (
    DiscographyBatch,
    DiscographyBatchItem,
    DiscographyBatchItemState,
    DiscographyBatchState,
    DiscographyScopeKind,
)
from app.models.job import Job


class _PreviewFormParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.fields: list[tuple[str, str]] | None = None
        self._current: list[tuple[str, str]] | None = None

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attributes = {key: value or "" for key, value in attrs}
        if tag == "form" and attributes.get("action") == "/discography-batches/preview":
            self._current = []
        elif tag == "input" and self._current is not None and attributes.get("name"):
            self._current.append((attributes["name"], attributes.get("value", "")))

    def handle_endtag(self, tag: str) -> None:
        if tag == "form" and self._current is not None:
            self.fields = self._current
            self._current = None


def _preview_form_fields(body: str) -> list[tuple[str, str]]:
    parser = _PreviewFormParser()
    parser.feed(body)
    assert parser.fields is not None
    return parser.fields


async def _seed_wanted_album(title: str = "Native Preview") -> tuple[int, int]:
    factory = db_module.get_session_factory()
    async with factory() as session:
        artist = CatalogArtist(name="Preview Artist", monitored=True)
        album = CatalogAlbum(
            artist=artist,
            title=title,
            year="2026",
            release_type="Album",
            track_count=1,
            monitored=True,
        )
        album.tracks.append(CatalogAlbumTrack(position=1, disc=1, title="Missing Track"))
        session.add(
            CatalogArtistIdentity(
                artist=artist,
                provider="musicbrainz",
                provider_artist_id="preview-artist",
                name=artist.name,
            )
        )
        await session.commit()
        return artist.id, album.id


async def _job_count() -> int:
    factory = db_module.get_session_factory()
    async with factory() as session:
        return int(await session.scalar(select(func.count(Job.id))) or 0)


async def _batch_count() -> int:
    factory = db_module.get_session_factory()
    async with factory() as session:
        return int(await session.scalar(select(func.count(DiscographyBatch.id))) or 0)


async def test_discography_batch_routes_require_auth(
    unauthenticated_client: AsyncClient,
) -> None:
    get_response = await unauthenticated_client.get(
        "/discography-batches/1", follow_redirects=False
    )
    post_response = await unauthenticated_client.post(
        "/discography-batches/preview",
        data={"scope_kind": "wanted_selected", "catalog_album_ids": "1"},
        follow_redirects=False,
    )
    assert get_response.status_code in {302, 307, 401}
    assert post_response.status_code in {302, 307, 401}


async def test_discography_batch_preview_rejects_missing_csrf(client: AsyncClient) -> None:
    client.headers.pop("X-CSRF-Token", None)
    response = await client.post(
        "/discography-batches/preview",
        data={"scope_kind": "wanted_selected", "catalog_album_ids": "1"},
    )
    assert response.status_code == 403


async def test_wanted_forms_preview_exact_native_scopes_and_keep_direct_action(
    client: AsyncClient,
) -> None:
    _, album_id = await _seed_wanted_album()
    response = await client.get("/wanted?q=Preview&sort=artist&status=needs-search")
    assert response.status_code == 200
    body = response.text
    assert 'action="/discography-batches/preview"' in body
    assert 'value="wanted_selected"' in body
    assert 'value="wanted_page"' in body
    assert 'value="wanted_all_matching"' in body
    assert f'name="catalog_album_ids" value="{album_id}"' in body
    assert 'name="q" value="Preview"' in body
    assert 'name="sort" value="artist"' in body
    assert 'name="status" value="needs-search"' in body
    assert "Preview selected" in body
    assert "Preview this page" in body
    assert "Preview all" in body
    assert f'formaction="/albums/{album_id}/download"' in body
    assert "Queue missing" in body


async def test_default_artist_preview_form_posts_canonical_all_scope_without_jobs(
    client: AsyncClient,
) -> None:
    artist_id, _ = await _seed_wanted_album("Default artist preview")
    page = await client.get(f"/artists/catalog/{artist_id}")
    assert page.status_code == 200
    fields = _preview_form_fields(page.text)
    assert ("release_type", "all") in fields
    before = await _job_count()

    response = await client.post(
        "/discography-batches/preview", data=dict(fields), follow_redirects=False
    )

    assert response.status_code == 303, response.text
    assert response.headers["location"].startswith("/discography-batches/")
    assert await _job_count() == before


async def test_selected_preview_creates_batch_items_but_no_jobs(client: AsyncClient) -> None:
    _, album_id = await _seed_wanted_album()
    before = await _job_count()
    response = await client.post(
        "/discography-batches/preview",
        data={
            "scope_kind": "wanted_selected",
            "catalog_album_ids": [str(album_id), str(album_id)],
        },
        follow_redirects=False,
    )
    assert response.status_code == 303
    assert response.headers["location"].startswith("/discography-batches/")
    assert await _job_count() == before
    page = await client.get(response.headers["location"])
    assert page.status_code == 200
    assert "Review batch" in page.text
    assert "Native Preview" in page.text
    assert "estimated job" in page.text


async def test_invalid_preview_returns_safe_actionable_html(client: AsyncClient) -> None:
    response = await client.post(
        "/discography-batches/preview",
        data={"scope_kind": "artist", "artist_id": "not-an-id"},
    )
    assert response.status_code == 400
    assert response.headers["content-type"].startswith("text/html")
    assert "Review the batch scope" in response.text
    assert "ValueError" not in response.text
    assert "Traceback" not in response.text


async def test_empty_selected_and_page_preview_do_not_persist_batches(
    client: AsyncClient,
) -> None:
    before = await _batch_count()
    for scope_kind in ("wanted_selected", "wanted_page"):
        response = await client.post(
            "/discography-batches/preview", data={"scope_kind": scope_kind}
        )
        assert response.status_code == 400
        assert "Select at least one release" in response.text
        assert await _batch_count() == before


async def test_confirm_unchanged_queues_and_wakes_runner(client: AsyncClient) -> None:
    _, album_id = await _seed_wanted_album()
    preview = await client.post(
        "/discography-batches/preview",
        data={"scope_kind": "wanted_selected", "catalog_album_ids": str(album_id)},
        follow_redirects=False,
    )
    batch_id = int(preview.headers["location"].rsplit("/", 1)[1])
    transport = client._transport
    assert isinstance(transport, ASGITransport)
    wake_count = 0

    class RunnerSpy:
        def wake(self) -> None:
            nonlocal wake_count
            wake_count += 1

    transport.app.state.discography_batch_runner = RunnerSpy()
    response = await client.post(
        f"/discography-batches/{batch_id}/confirm", follow_redirects=False
    )
    assert response.status_code == 303
    assert "notice=confirmed" in response.headers["location"]
    factory = db_module.get_session_factory()
    async with factory() as session:
        batch = await session.get(DiscographyBatch, batch_id)
        assert batch is not None
        assert batch.state == DiscographyBatchState.queued
    assert wake_count == 1
    assert await _job_count() == 0


async def test_confirm_changed_refreshes_preview_without_jobs(client: AsyncClient) -> None:
    _, album_id = await _seed_wanted_album("First Snapshot")
    preview = await client.post(
        "/discography-batches/preview",
        data={"scope_kind": "wanted_selected", "catalog_album_ids": str(album_id)},
        follow_redirects=False,
    )
    batch_id = int(preview.headers["location"].rsplit("/", 1)[1])
    factory = db_module.get_session_factory()
    async with factory() as session:
        album = await session.get(CatalogAlbum, album_id)
        assert album is not None
        album.monitored = False
        await session.commit()
    response = await client.post(f"/discography-batches/{batch_id}/confirm")
    assert response.status_code == 200
    assert "scope changed; review refreshed preview" in response.text.lower()
    assert await _job_count() == 0


async def test_batch_page_escapes_snapshots_and_errors_without_lazy_load(
    client: AsyncClient,
) -> None:
    factory = db_module.get_session_factory()
    raw = '<script>alert("snapshot")</script>'
    error = '<img src=x onerror=alert("error")>'
    async with factory() as session:
        batch = DiscographyBatch(
            scope_kind=DiscographyScopeKind.wanted_selected,
            scope_json='{"album_ids":[]}',
            scope_hash="0" * 64,
            state=DiscographyBatchState.completed_with_failures,
            error_detail=error,
        )
        batch.items.append(
            DiscographyBatchItem(
                release_identity="snapshot:unsafe",
                artist_name=raw,
                release_title=raw,
                state=DiscographyBatchItemState.failed,
                reason_code="hydration_failed",
                error_detail=error,
            )
        )
        session.add(batch)
        await session.commit()
        batch_id = batch.id
    response = await client.get(f"/discography-batches/{batch_id}")
    assert response.status_code == 200
    assert raw not in response.text
    assert error not in response.text
    assert "&lt;script&gt;alert" in response.text
    assert "&lt;img src=x onerror=alert" in response.text
    assert "Retry selected" in response.text


async def test_native_pause_resume_cancel_controls_and_runner_wake(client: AsyncClient) -> None:
    factory = db_module.get_session_factory()
    async with factory() as session:
        batch = DiscographyBatch(
            scope_kind=DiscographyScopeKind.wanted_selected,
            scope_json='{"album_ids":[]}',
            scope_hash="0" * 64,
            state=DiscographyBatchState.queued,
        )
        batch.items.append(
            DiscographyBatchItem(
                release_identity="control:item",
                artist_name="Control Artist",
                release_title="Control Album",
                state=DiscographyBatchItemState.pending,
            )
        )
        session.add(batch)
        await session.commit()
        batch_id = batch.id

    paused = await client.post(f"/discography-batches/{batch_id}/pause", follow_redirects=False)
    assert paused.status_code == 303
    assert "notice=paused" in paused.headers["location"]

    transport = client._transport
    assert isinstance(transport, ASGITransport)
    wake_count = 0

    class RunnerSpy:
        def wake(self) -> None:
            nonlocal wake_count
            wake_count += 1

    transport.app.state.discography_batch_runner = RunnerSpy()
    resumed = await client.post(f"/discography-batches/{batch_id}/resume", follow_redirects=False)
    assert resumed.status_code == 303
    assert wake_count == 1
    cancelled = await client.post(
        f"/discography-batches/{batch_id}/cancel", follow_redirects=False
    )
    assert cancelled.status_code == 303
    async with factory() as session:
        batch = await session.get(DiscographyBatch, batch_id)
        assert batch is not None
        assert batch.state == DiscographyBatchState.cancelled


async def test_retry_route_resets_only_selected_failed_item_and_wakes(
    client: AsyncClient,
) -> None:
    factory = db_module.get_session_factory()
    async with factory() as session:
        batch = DiscographyBatch(
            scope_kind=DiscographyScopeKind.wanted_selected,
            scope_json='{"album_ids":[]}',
            scope_hash="0" * 64,
            state=DiscographyBatchState.completed_with_failures,
        )
        failed = DiscographyBatchItem(
            release_identity="retry:failed",
            artist_name="Retry Artist",
            release_title="Failed Album",
            state=DiscographyBatchItemState.failed,
            error_detail="provider timeout",
        )
        untouched = DiscographyBatchItem(
            release_identity="retry:untouched",
            artist_name="Retry Artist",
            release_title="Untouched Album",
            state=DiscographyBatchItemState.failed,
        )
        batch.items.extend([failed, untouched])
        session.add(batch)
        await session.commit()
        batch_id, failed_id, untouched_id = batch.id, failed.id, untouched.id

    transport = client._transport
    assert isinstance(transport, ASGITransport)
    wake_count = 0

    class RunnerSpy:
        def wake(self) -> None:
            nonlocal wake_count
            wake_count += 1

    transport.app.state.discography_batch_runner = RunnerSpy()
    response = await client.post(
        f"/discography-batches/{batch_id}/retry",
        data={"item_ids": str(failed_id)},
        follow_redirects=False,
    )
    assert response.status_code == 303
    assert wake_count == 1
    async with factory() as session:
        failed = await session.get(DiscographyBatchItem, failed_id)
        untouched = await session.get(DiscographyBatchItem, untouched_id)
        assert failed is not None and failed.state == DiscographyBatchItemState.pending
        assert failed.error_detail is None
        assert untouched is not None and untouched.state == DiscographyBatchItemState.failed


async def test_cancelled_batch_renders_exact_retry_eligibility_and_retries_selected(
    client: AsyncClient,
) -> None:
    factory = db_module.get_session_factory()
    async with factory() as session:
        batch = DiscographyBatch(
            scope_kind=DiscographyScopeKind.wanted_selected,
            scope_json='{"album_ids":[]}',
            scope_hash="0" * 64,
            state=DiscographyBatchState.cancelled,
        )
        cancelled = DiscographyBatchItem(
            release_identity="retry:cancelled",
            artist_name="Retry Artist",
            release_title="Cancelled Album",
            state=DiscographyBatchItemState.cancelled,
            reason_code="batch_cancelled",
        )
        retryable_skip = DiscographyBatchItem(
            release_identity="retry:hydration",
            artist_name="Retry Artist",
            release_title="Hydration Album",
            state=DiscographyBatchItemState.skipped,
            reason_code="hydration_failed",
        )
        ordinary_skip = DiscographyBatchItem(
            release_identity="retry:duplicate",
            artist_name="Retry Artist",
            release_title="Duplicate Album",
            state=DiscographyBatchItemState.skipped,
            reason_code="duplicate_catalog_album",
        )
        batch.items.extend([cancelled, retryable_skip, ordinary_skip])
        session.add(batch)
        await session.commit()
        batch_id = batch.id
        cancelled_id = cancelled.id
        retryable_skip_id = retryable_skip.id
        ordinary_skip_id = ordinary_skip.id

    page = await client.get(f"/discography-batches/{batch_id}")
    assert page.status_code == 200
    assert "Retry selected" in page.text
    assert f'name="item_ids" value="{cancelled_id}"' in page.text
    assert f'name="item_ids" value="{retryable_skip_id}"' in page.text
    assert f'name="item_ids" value="{ordinary_skip_id}"' not in page.text

    transport = client._transport
    assert isinstance(transport, ASGITransport)
    wake_count = 0

    class RunnerSpy:
        def wake(self) -> None:
            nonlocal wake_count
            wake_count += 1

    transport.app.state.discography_batch_runner = RunnerSpy()
    response = await client.post(
        f"/discography-batches/{batch_id}/retry",
        data={"item_ids": str(cancelled_id)},
        follow_redirects=False,
    )
    assert response.status_code == 303
    assert "notice=retried" in response.headers["location"]
    assert wake_count == 1
    async with factory() as session:
        item = await session.get(DiscographyBatchItem, cancelled_id)
        assert item is not None and item.state == DiscographyBatchItemState.pending


async def test_ineligible_retry_selection_is_truthful_and_does_not_wake(
    client: AsyncClient,
) -> None:
    factory = db_module.get_session_factory()
    async with factory() as session:
        batch = DiscographyBatch(
            scope_kind=DiscographyScopeKind.wanted_selected,
            scope_json='{"album_ids":[]}',
            scope_hash="0" * 64,
            state=DiscographyBatchState.completed_with_failures,
        )
        item = DiscographyBatchItem(
            release_identity="retry:ineligible",
            artist_name="Retry Artist",
            release_title="Already Complete",
            state=DiscographyBatchItemState.complete,
            reason_code="verified_complete",
        )
        batch.items.append(item)
        session.add(batch)
        await session.commit()
        batch_id, item_id = batch.id, item.id

    transport = client._transport
    assert isinstance(transport, ASGITransport)
    wake_count = 0

    class RunnerSpy:
        def wake(self) -> None:
            nonlocal wake_count
            wake_count += 1

    transport.app.state.discography_batch_runner = RunnerSpy()
    response = await client.post(
        f"/discography-batches/{batch_id}/retry",
        data={"item_ids": str(item_id)},
        follow_redirects=False,
    )
    assert response.status_code == 303
    assert "notice=no-eligible-retries" in response.headers["location"]
    assert "notice=retried" not in response.headers["location"]
    assert wake_count == 0
    page = await client.get(response.headers["location"])
    assert "No selected releases were eligible for retry." in page.text
