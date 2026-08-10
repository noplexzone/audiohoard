from __future__ import annotations

from httpx import AsyncClient


def test_interactive_search_adapters_cap_runtime_bulk_budget(monkeypatch) -> None:
    from app.config import Settings
    from app.routers import search as search_router

    observed: dict[str, float] = {}

    class FakeSlskdAdapter:
        def __init__(self, url: str, api_key: str, search_timeout_sec: float) -> None:
            observed["slskd"] = search_timeout_sec

    class FakeYouTubeAdapter:
        def __init__(self, cookies_file: str, search_timeout_sec: float) -> None:
            observed["youtube"] = search_timeout_sec

    monkeypatch.setattr(search_router, "SlskdAdapter", FakeSlskdAdapter)
    monkeypatch.setattr(search_router, "YouTubeAdapter", FakeYouTubeAdapter)

    settings = Settings(secret_key="test-secret")
    assert search_router._build_adapter("slskd", settings, 900) is not None
    assert search_router._build_adapter("youtube", settings, 900) is not None

    assert observed == {"slskd": 60.0, "youtube": 30.0}


async def test_search_returns_200_with_empty_sources(client: AsyncClient) -> None:
    resp = await client.post(
        "/search",
        json={"query": "Beethoven Symphony", "sources": []},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert "results" in data
    assert "source_states" in data


async def test_search_with_unknown_source_excluded(client: AsyncClient) -> None:
    resp = await client.post(
        "/search",
        json={"query": "test", "sources": ["nonexistent"]},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["results"] == []


async def test_explicit_tidal_search_reports_unconfigured_profile(
    client: AsyncClient, monkeypatch
) -> None:
    from app.sources import tidal as tidal_source

    monkeypatch.setattr(tidal_source, "_tidal_dl_version", lambda: "test-version")
    monkeypatch.setattr(tidal_source, "_tidal_dl_executable", lambda: "/test/tidal-dl")
    resp = await client.post("/search", json={"query": "test", "sources": ["tidal"]})
    assert resp.status_code == 200
    state = resp.json()["source_states"]["tidal"]
    assert state["available"] is False
    assert state["details"]["code"] == "profile_unconfigured"


async def test_search_unconfigured_sources_gracefully_degrade(client: AsyncClient) -> None:
    resp = await client.post(
        "/search",
        json={"query": "test query", "sources": ["slskd", "prowlarr"]},
    )
    assert resp.status_code == 200
    data = resp.json()
    for _name, state in data["source_states"].items():
        assert "available" in state


async def test_search_rejects_empty_query(client: AsyncClient) -> None:
    resp = await client.post("/search", json={"query": "", "sources": []})
    assert resp.status_code == 422


async def test_catalog_search_closes_database_transaction_before_provider_http(
    client: AsyncClient, monkeypatch
) -> None:
    from app.routers import search as search_router

    original_get_runtime_settings = search_router.get_runtime_settings
    request_db = None

    async def capturing_runtime(db):
        nonlocal request_db
        request_db = db
        return await original_get_runtime_settings(db)

    async def asserting_search(settings, query: str, providers: list[str]):
        del settings, query, providers
        assert request_db is not None
        assert not request_db.in_transaction()
        return []

    monkeypatch.setattr(search_router, "get_runtime_settings", capturing_runtime)
    monkeypatch.setattr(search_router, "search_catalog_artists", asserting_search)

    response = await client.get("/search?q=transaction+scope&provider=deezer")

    assert response.status_code == 200


async def test_naming_preview_endpoint(client: AsyncClient) -> None:
    resp = await client.post(
        "/naming/preview",
        json={
            "title": "Bohemian Rhapsody",
            "album_artist": "Queen",
            "album": "A Night at the Opera",
            "year": "1975",
            "track_no": 11,
            "ext": "flac",
        },
    )
    assert resp.status_code == 200
    data = resp.json()
    assert "rendered_path" in data
    assert "Bohemian Rhapsody" in data["rendered_path"]
    assert data["rendered_path"].endswith(".flac")


async def test_manual_search_requires_visible_source_selection(client: AsyncClient) -> None:
    page = await client.get(
        "/search?tab=advanced&artist=Massive+Attack&album=Mezzanine&track=Teardrop&expected_duration_sec=331&preferred_format=flac"
    )
    assert page.status_code == 200
    assert "<h1>Discover</h1>" in page.text
    assert "Manual search" in page.text
    assert 'value="Massive Attack"' in page.text
    csrf = client.cookies.get("csrf")
    response = await client.post(
        "/search/ui", data={"csrf_token": csrf, "artist": "Massive Attack"}
    )
    assert response.status_code == 200
    assert "Select at least one enabled source" in response.text
    assert 'value="Massive Attack"' in response.text


async def test_manual_search_marks_only_active_exact_rejections(client: AsyncClient) -> None:
    del client
    from datetime import UTC, datetime, timedelta

    import app.database as db_module
    from app.models.source_candidate_block import SourceCandidateBlock
    from app.routers.search import _mark_active_rejected_results
    from app.schemas.search import SearchResult

    factory = db_module.get_session_factory()
    async with factory() as session:
        block = SourceCandidateBlock(
            provider="slskd", peer="peer", filename="Artist/track.flac", reason="denied"
        )
        session.add(block)
        await session.commit()
        active = SearchResult(
            source="slskd",
            metadata={"username": "peer", "filename": "Artist/track.flac"},
        )
        await _mark_active_rejected_results(session, [active])
        assert active.metadata["blocked"] is True

        block.blocked_until = datetime.now(UTC) - timedelta(seconds=1)
        await session.commit()
        expired = SearchResult(
            source="slskd",
            metadata={"username": "peer", "filename": "Artist/track.flac"},
        )
        await _mark_active_rejected_results(session, [expired])
        assert "blocked" not in expired.metadata


async def test_manual_search_validation_error_renders_in_form(client: AsyncClient) -> None:
    response = await client.post(
        "/search/ui",
        data={"query": "x" * 501, "sources": ["slskd"]},
    )
    assert response.status_code == 422
    assert "at most 500 characters" in response.text


async def test_json_and_html_manual_search_apply_active_rejection_history(
    client: AsyncClient, monkeypatch
) -> None:
    from datetime import UTC, datetime, timedelta

    import app.database as db_module
    from app.models.source_candidate_block import SourceCandidateBlock
    from app.schemas.health import SourceStatus
    from app.schemas.search import SearchResult

    async def fake_source(name, settings, request, budget):
        del settings, request, budget
        return (
            name,
            [
                SearchResult(
                    source="slskd",
                    title="A blocked",
                    metadata={"username": "peer", "filename": "Artist/track.flac"},
                ),
                SearchResult(
                    source="slskd",
                    title="B usable",
                    metadata={"username": "other", "filename": "Artist/other.flac"},
                ),
            ],
            SourceStatus(available=True, details={}),
        )

    from dataclasses import replace

    import app.routers.search as search_router

    monkeypatch.setattr("app.routers.search._search_source", fake_source)
    factory = db_module.get_session_factory()
    async with factory() as session:
        runtime = await search_router.get_runtime_settings(session)
        runtime = replace(
            runtime,
            source_priority=[{"name": "slskd", "enabled": True}],
        )

        async def forced_runtime(db):
            del db
            return runtime

        monkeypatch.setattr(search_router, "get_runtime_settings", forced_runtime)
        block = SourceCandidateBlock(
            provider="slskd", peer="peer", filename="Artist/track.flac", reason="denied"
        )
        session.add(block)
        await session.commit()

        active = await client.post("/search", json={"query": "track", "sources": ["slskd"]})
        assert active.status_code == 200
        active_results = active.json()["results"]
        assert [result["title"] for result in active_results] == ["B usable", "A blocked"]
        assert active_results[1]["metadata"]["blocked"] is True

        html = await client.post("/search/ui", data={"query": "track", "sources": ["slskd"]})
        assert html.status_code == 200
        assert html.text.index("B usable") < html.text.index("A blocked")

        block.blocked_until = datetime.now(UTC) - timedelta(seconds=1)
        await session.commit()

    expired = await client.post("/search", json={"query": "track", "sources": ["slskd"]})
    assert expired.status_code == 200
    assert [result["title"] for result in expired.json()["results"]] == [
        "A blocked",
        "B usable",
    ]
    assert "blocked" not in expired.json()["results"][0]["metadata"]


async def test_manual_search_rejects_malformed_duration_inline(client: AsyncClient) -> None:
    response = await client.post(
        "/search/ui",
        data={"query": "track", "sources": ["slskd"], "expected_duration_sec": "not-a-number"},
    )
    assert response.status_code == 422
    assert "valid integer" in response.text
