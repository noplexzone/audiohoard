from __future__ import annotations

import app.settings_service as settings_service
from app.database import get_session_factory
from app.settings_service import get_runtime_settings


async def test_discovery_region_defaults_and_persists(client, monkeypatch) -> None:
    from app.routers import settings as settings_router

    invalidated: list[str] = []
    monkeypatch.setattr(settings_router.discovery_service, "invalidate_region", invalidated.append)
    async with get_session_factory()() as db:
        settings_service._cache = None
        assert (await get_runtime_settings(db)).discovery_region == "US"

    response = await client.post(
        "/settings",
        data={
            "section": "metadata",
            "metadata_order": ["musicbrainz", "deezer", "itunes"],
            "metadata_enabled": ["musicbrainz", "deezer", "itunes"],
            "primary_metadata_provider": "musicbrainz",
            "discovery_region": "JP",
        },
    )
    assert response.status_code == 303
    async with get_session_factory()() as db:
        assert (await get_runtime_settings(db)).discovery_region == "JP"
    assert invalidated == ["US", "JP"]


async def test_invalid_region_is_rejected_and_unrelated_save_preserves_region(client) -> None:
    valid = {
        "section": "metadata",
        "metadata_order": ["musicbrainz", "deezer", "itunes"],
        "metadata_enabled": ["musicbrainz", "deezer", "itunes"],
        "primary_metadata_provider": "musicbrainz",
        "discovery_region": "CA",
    }
    assert (await client.post("/settings", data=valid)).status_code == 303
    invalid = await client.post("/settings", data={**valid, "discovery_region": "XX"})
    assert invalid.status_code == 303
    assert "Unsupported+discovery+region" in invalid.headers["location"]

    unrelated = await client.post(
        "/settings",
        data={
            "section": "download-sources",
            "source_order": ["slskd", "prowlarr", "youtube", "tidal"],
        },
    )
    assert unrelated.status_code == 303
    async with get_session_factory()() as db:
        assert (await get_runtime_settings(db)).discovery_region == "CA"
