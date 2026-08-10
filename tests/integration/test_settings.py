from __future__ import annotations

import asyncio
from pathlib import Path

import pytest
from httpx import AsyncClient


@pytest.mark.asyncio
async def test_save_and_retrieve_plain_setting(
    client: AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    from app.sources.base import CapabilityState
    from app.sources.slskd import SlskdAdapter

    async def _ok(self: object) -> CapabilityState:
        return CapabilityState(available=True)

    monkeypatch.setattr(SlskdAdapter, "health", _ok)
    save = await client.post(
        "/api/settings/save",
        json={"slskd_url": "http://slskd-host:5030", "slskd_api_key": "key"},
    )
    assert save.status_code == 200

    get = await client.get("/api/settings")
    assert get.status_code == 200
    data = get.json()
    assert data["slskd_url"]["value"] == "http://slskd-host:5030"
    assert data["slskd_url"]["configured"] is True
    assert data["slskd_url"]["locked_by_environment"] is False


@pytest.mark.asyncio
async def test_save_secret_and_retrieve_masked(
    client: AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    from app.sources.base import CapabilityState
    from app.sources.slskd import SlskdAdapter

    async def _ok(self: object) -> CapabilityState:
        return CapabilityState(available=True)

    monkeypatch.setattr(SlskdAdapter, "health", _ok)
    save = await client.post(
        "/api/settings/save",
        json={"slskd_url": "http://slskd", "slskd_api_key": "super-secret-key"},
    )
    assert save.status_code == 200

    get = await client.get("/api/settings")
    data = get.json()
    assert data["slskd_api_key"]["value"] == "***"
    assert data["slskd_api_key"]["configured"] is True
    assert data["slskd_api_key"]["locked_by_environment"] is False


@pytest.mark.asyncio
async def test_blank_secret_keeps_existing_in_api(
    client: AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    from app.sources.base import CapabilityState
    from app.sources.slskd import SlskdAdapter

    async def _ok(self: object) -> CapabilityState:
        return CapabilityState(available=True)

    monkeypatch.setattr(SlskdAdapter, "health", _ok)
    await client.post(
        "/api/settings/save",
        json={"slskd_url": "http://slskd", "slskd_api_key": "initial-secret"},
    )
    await client.post(
        "/api/settings/save",
        json={"slskd_api_key": ""},
    )
    get = await client.get("/api/settings")
    data = get.json()
    assert data["slskd_api_key"]["configured"] is True
    assert data["slskd_api_key"]["value"] == "***"


@pytest.mark.asyncio
async def test_env_lock_reflected_in_api(
    client: AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    from app.config import Settings, override_settings

    override_settings(
        Settings(
            database_url="sqlite+aiosqlite:///:memory:",
            secret_key="test-secret",
            auth_cookie_secure=False,
            slskd_url="http://env-locked-slskd",
        )
    )
    get = await client.get("/api/settings")
    data = get.json()
    assert data["slskd_url"]["locked_by_environment"] is True
    assert data["slskd_url"]["value"] == "http://env-locked-slskd"


@pytest.mark.asyncio
async def test_test_endpoint_does_not_write(
    client: AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def _fake_health(self: object) -> object:
        from app.sources.base import CapabilityState

        return CapabilityState(available=True)

    from app.sources.slskd import SlskdAdapter

    monkeypatch.setattr(SlskdAdapter, "health", _fake_health)

    resp = await client.post(
        "/api/settings/test",
        json={"provider": "slskd", "slskd_url": "http://test", "slskd_api_key": "k"},
    )
    assert resp.status_code == 200
    assert resp.json()["available"] is True

    get = await client.get("/api/settings")
    data = get.json()
    assert data["slskd_url"]["configured"] is False


@pytest.mark.asyncio
async def test_test_endpoint_unknown_provider_returns_error(client: AsyncClient) -> None:
    resp = await client.post(
        "/api/settings/test",
        json={"provider": "unknown_provider"},
    )
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_tidal_settings_stored_and_returned(client: AsyncClient) -> None:
    save = await client.post(
        "/api/settings/save",
        json={"tidal_config_path": "/data/tidal/config", "tidal_quality": "HiFi"},
    )
    assert save.status_code == 200

    get = await client.get("/api/settings")
    data = get.json()
    assert data["tidal_config_path"]["value"] == "/data/tidal/config"
    assert data["tidal_quality"]["value"] == "HiFi"
    assert data["tidal_config_path"]["locked_by_environment"] is False


@pytest.mark.asyncio
async def test_settings_migration_model_persistence(db_session: object) -> None:
    from sqlalchemy import select
    from sqlalchemy.ext.asyncio import AsyncSession

    from app.models.settings import ProviderSetting

    assert isinstance(db_session, AsyncSession)
    row = ProviderSetting(key="slskd_url", value_plain="http://persist-test", value_encrypted=None)
    db_session.add(row)
    await db_session.flush()

    fetched = await db_session.scalar(
        select(ProviderSetting).where(ProviderSetting.key == "slskd_url")
    )
    assert fetched is not None
    assert fetched.value_plain == "http://persist-test"


@pytest.mark.asyncio
async def test_save_validate_only_flag_via_internal_api(db_session: object) -> None:
    from pathlib import Path

    from sqlalchemy import select
    from sqlalchemy.ext.asyncio import AsyncSession

    from app.config import Settings
    from app.models.settings import ProviderSetting
    from app.settings_service import save_settings

    assert isinstance(db_session, AsyncSession)
    settings = Settings(
        database_url="sqlite+aiosqlite:///:memory:",
        secret_key="test-secret",
        auth_cookie_secure=False,
        library_root=Path("/music"),
        staging_root=Path("/staging"),
    )
    await save_settings(
        db_session, {"slskd_url": "http://should-not-persist"}, settings, validate_only=True
    )
    await db_session.flush()
    row = await db_session.scalar(
        select(ProviderSetting).where(ProviderSetting.key == "slskd_url")
    )
    assert row is None


@pytest.mark.asyncio
async def test_save_backstop_blocks_when_provider_unreachable(
    client: AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    from app.sources.base import CapabilityState
    from app.sources.slskd import SlskdAdapter

    async def _fail_health(self: object) -> CapabilityState:
        return CapabilityState(available=False, reason="connection refused")

    monkeypatch.setattr(SlskdAdapter, "health", _fail_health)

    resp = await client.post(
        "/api/settings/save",
        json={"slskd_url": "http://new-url", "slskd_api_key": "some-key"},
    )
    assert resp.status_code == 422
    body = resp.json()
    assert "slskd" in body["detail"]["validation_errors"]

    # Nothing was written.
    get = await client.get("/api/settings")
    data = get.json()
    assert data["slskd_url"]["configured"] is False


@pytest.mark.asyncio
async def test_save_backstop_passes_when_provider_reachable(
    client: AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    from app.sources.base import CapabilityState
    from app.sources.slskd import SlskdAdapter

    async def _ok_health(self: object) -> CapabilityState:
        return CapabilityState(available=True)

    monkeypatch.setattr(SlskdAdapter, "health", _ok_health)

    resp = await client.post(
        "/api/settings/save",
        json={"slskd_url": "http://good-slskd", "slskd_api_key": "valid-key"},
    )
    assert resp.status_code == 200

    get = await client.get("/api/settings")
    data = get.json()
    assert data["slskd_url"]["value"] == "http://good-slskd"
    assert data["slskd_api_key"]["configured"] is True


@pytest.mark.asyncio
async def test_save_no_validation_when_no_credentials_changed(
    client: AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Saving only non-credential fields (e.g. library_root) must not trigger probe."""
    probe_called = False

    from app.sources.slskd import SlskdAdapter

    async def _should_not_be_called(self: object) -> object:
        nonlocal probe_called
        probe_called = True
        from app.sources.base import CapabilityState

        return CapabilityState(available=False, reason="should not have been called")

    monkeypatch.setattr(SlskdAdapter, "health", _should_not_be_called)

    resp = await client.post(
        "/api/settings/save",
        json={"naming_template": "{album_artist}/{year}/{title}.{ext}"},
    )
    assert resp.status_code == 200
    assert not probe_called


@pytest.mark.asyncio
async def test_test_endpoint_uses_stored_secret_as_fallback(
    client: AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Test endpoint should use the stored (decrypted) API key when none is supplied."""
    received_key: list[str] = []

    from app.sources.slskd import SlskdAdapter

    original_init = SlskdAdapter.__init__

    def _capturing_init(self: SlskdAdapter, url: str, api_key: str) -> None:
        received_key.append(api_key)
        original_init(self, url, api_key)

    from app.sources.base import CapabilityState

    async def _ok_health(self: object) -> CapabilityState:
        return CapabilityState(available=True)

    monkeypatch.setattr(SlskdAdapter, "__init__", _capturing_init)
    monkeypatch.setattr(SlskdAdapter, "health", _ok_health)

    # Store a secret key.
    await client.post(
        "/api/settings/save",
        json={"slskd_url": "http://stored-slskd", "slskd_api_key": "stored-secret-key"},
    )

    # Test with blank api_key — should fall back to the stored secret.
    resp = await client.post(
        "/api/settings/test",
        json={"provider": "slskd", "slskd_url": "http://stored-slskd", "slskd_api_key": ""},
    )
    assert resp.status_code == 200
    assert resp.json()["available"] is True
    assert received_key[-1] == "stored-secret-key"


@pytest.mark.asyncio
async def test_setup_with_provider_settings_persists_atomically(
    unauthenticated_client: AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Provider settings supplied during setup must be saved in the same transaction."""
    resp = await unauthenticated_client.post(
        "/api/auth/setup",
        json={
            "username": "owner",
            "password": "Owner-Password-Secure-42",
            "provider_settings": {
                "slskd_url": "http://setup-slskd",
                "slskd_api_key": "setup-secret",
                "musicbrainz_contact": "test@example.com",
            },
        },
    )
    assert resp.status_code == 201
    csrf = resp.json()["csrf_token"]

    unauthenticated_client.headers["X-CSRF-Token"] = csrf
    get = await unauthenticated_client.get("/api/settings")
    assert get.status_code == 200
    data = get.json()
    assert data["slskd_url"]["value"] == "http://setup-slskd"
    assert data["musicbrainz_contact"]["value"] == "test@example.com"


@pytest.mark.asyncio
async def test_setup_without_provider_settings_still_works(
    unauthenticated_client: AsyncClient,
) -> None:
    """Setup must succeed with only username/password (no provider_settings)."""
    resp = await unauthenticated_client.post(
        "/api/auth/setup",
        json={"username": "owner", "password": "Owner-Password-Secure-42"},
    )
    assert resp.status_code == 201


@pytest.mark.asyncio
async def test_partial_save_preserves_omitted_plain_settings(client: AsyncClient) -> None:
    first = await client.post(
        "/api/settings/save",
        json={"tidal_config_path": "/data/tidal", "tidal_quality": "HiFi"},
    )
    assert first.status_code == 200
    second = await client.post(
        "/api/settings/save",
        json={"musicbrainz_contact": "operator@example.com"},
    )
    assert second.status_code == 200
    data = (await client.get("/api/settings")).json()
    assert data["tidal_config_path"]["value"] == "/data/tidal"
    assert data["tidal_quality"]["value"] == "HiFi"


@pytest.mark.asyncio
async def test_incomplete_changed_provider_credentials_are_rejected(client: AsyncClient) -> None:
    response = await client.post(
        "/api/settings/save",
        json={"slskd_url": "http://slskd-without-key"},
    )
    assert response.status_code == 422
    assert response.json()["detail"]["validation_errors"]["slskd"] == (
        "URL and API key are required together"
    )
    data = (await client.get("/api/settings")).json()
    assert data["slskd_url"]["configured"] is False


@pytest.mark.asyncio
async def test_setup_rejects_incomplete_provider_pair_without_claiming_owner(
    unauthenticated_client: AsyncClient,
) -> None:
    rejected = await unauthenticated_client.post(
        "/api/auth/setup",
        json={
            "username": "owner",
            "password": "Owner-Password-Secure-42",
            "provider_settings": {"slskd_url": "http://missing-key"},
        },
    )
    assert rejected.status_code == 422
    assert rejected.json()["detail"]["validation_errors"]["slskd"] == (
        "URL and API key are required together"
    )
    retry = await unauthenticated_client.post(
        "/api/auth/setup",
        json={"username": "owner", "password": "Owner-Password-Secure-42"},
    )
    assert retry.status_code == 201


@pytest.mark.asyncio
async def test_tidal_quality_rejects_values_not_available_in_ui(client: AsyncClient) -> None:
    response = await client.post(
        "/api/settings/save",
        json={"tidal_quality": "HI_RES"},
    )
    assert response.status_code == 422


async def test_changelog_page_renders_markdown_links(client: object) -> None:
    from httpx import AsyncClient

    assert isinstance(client, AsyncClient)
    response = await client.get("/changelog")

    assert response.status_code == 200
    assert "0.4.1" in response.text
    assert "Keep a Changelog" in response.text
    assert "https://keepachangelog.com" in response.text


@pytest.mark.asyncio
async def test_html_connection_test_uses_entered_values_without_saving(
    client: AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    from app.sources.base import CapabilityState
    from app.sources.slskd import SlskdAdapter

    captured: dict[str, str] = {}

    async def healthy(self: SlskdAdapter) -> CapabilityState:
        captured["url"] = self._base_url
        captured["key"] = self._api_key
        return CapabilityState(available=True)

    monkeypatch.setattr(SlskdAdapter, "health", healthy)
    response = await client.post(
        "/settings/test",
        data={
            "provider": "slskd",
            "slskd_url": "http://entered-slskd:5030",
            "slskd_api_key": "entered-key",
        },
        follow_redirects=False,
    )

    assert response.status_code == 303
    assert captured == {"url": "http://entered-slskd:5030", "key": "entered-key"}
    settings = (await client.get("/api/settings")).json()
    assert settings["slskd_url"]["configured"] is False


@pytest.mark.asyncio
async def test_html_save_uses_provider_validation_and_writes_nothing_on_failure(
    client: AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    from app.sources.base import CapabilityState
    from app.sources.slskd import SlskdAdapter

    async def unavailable(self: SlskdAdapter) -> CapabilityState:
        return CapabilityState(available=False, reason="connection refused")

    monkeypatch.setattr(SlskdAdapter, "health", unavailable)
    response = await client.post(
        "/settings/save",
        data={
            "section": "download-clients",
            "slskd_url": "http://bad-slskd",
            "slskd_api_key": "bad-key",
        },
        follow_redirects=False,
    )

    assert response.status_code == 303
    assert "error=" in response.headers["location"]
    settings = (await client.get("/api/settings")).json()
    assert settings["slskd_url"]["configured"] is False


@pytest.mark.asyncio
async def test_source_priority_move_control_persists_order(client: AsyncClient) -> None:
    from app.database import get_session_factory
    from app.settings_service import get_runtime_settings

    response = await client.post(
        "/settings",
        data={
            "section": "download-sources",
            "source_order": ["slskd", "prowlarr", "youtube", "tidal"],
            "source_enabled": ["slskd", "prowlarr", "youtube", "tidal"],
            "move_source": "up:youtube",
        },
        follow_redirects=False,
    )

    assert response.status_code == 303
    factory = get_session_factory()
    async with factory() as db:
        runtime = await get_runtime_settings(db)
    assert [item["name"] for item in runtime.source_priority] == [
        "slskd",
        "youtube",
        "prowlarr",
        "tidal",
    ]
    assert all(item["enabled"] for item in runtime.source_priority)


@pytest.mark.asyncio
async def test_behavior_slskd_download_timeout_persists(client: AsyncClient) -> None:
    from app.database import get_session_factory
    from app.settings_service import get_runtime_settings

    async with get_session_factory()() as db:
        original_timeout = (await get_runtime_settings(db)).slskd_download_timeout_seconds

    try:
        response = await client.post(
            "/settings",
            data={"section": "behavior", "slskd_download_timeout_seconds": "360"},
            follow_redirects=False,
        )

        assert response.status_code == 303
        assert response.headers["location"] == "/settings/behavior?saved=1"
        async with get_session_factory()() as db:
            runtime = await get_runtime_settings(db)
        assert runtime.slskd_download_timeout_seconds == 360
    finally:
        await client.post(
            "/settings",
            data={
                "section": "behavior",
                "slskd_download_timeout_seconds": str(original_timeout),
            },
            follow_redirects=False,
        )


@pytest.mark.asyncio
async def test_invalid_behavior_form_redirects_with_error_instead_of_500(
    client: AsyncClient,
) -> None:
    response = await client.post(
        "/settings",
        data={"section": "behavior", "free_text_result_limit": "not-a-number"},
        follow_redirects=False,
    )

    assert response.status_code == 303
    assert response.headers["location"].startswith("/settings/behavior?error=")


@pytest.mark.asyncio
async def test_download_client_section_renders_disabled_source_states(client: AsyncClient) -> None:
    response = await client.post(
        "/settings",
        data={
            "section": "download-sources",
            "source_order": ["slskd", "prowlarr", "youtube", "tidal"],
            "source_enabled": ["slskd", "youtube"],
        },
        follow_redirects=False,
    )
    assert response.status_code == 303

    page = await client.get("/settings/download-clients")
    assert page.status_code == 200
    assert page.text.count("Disabled") >= 3


@pytest.mark.asyncio
async def test_saving_download_sources_preserves_auto_download_behavior(
    client: AsyncClient,
) -> None:
    enabled = await client.post(
        "/settings",
        data={
            "section": "behavior",
            "free_text_result_limit": "10",
            "discography_refresh_hours": "24",
            "source_search_budget_seconds": "15",
            "auto_download_wanted": "true",
        },
        follow_redirects=False,
    )
    assert enabled.status_code == 303

    saved_sources = await client.post(
        "/settings",
        data={
            "section": "download-sources",
            "source_order": ["slskd", "prowlarr", "youtube", "tidal"],
            "source_enabled": ["slskd", "prowlarr", "youtube"],
        },
        follow_redirects=False,
    )
    assert saved_sources.status_code == 303

    from app.database import get_session_factory
    from app.settings_service import get_runtime_settings

    factory = get_session_factory()
    async with factory() as db:
        runtime = await get_runtime_settings(db)
    assert runtime.auto_download_wanted is True


@pytest.mark.asyncio
async def test_behavior_parallel_acquisition_limit_renders_and_persists(
    client: AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    from app.jobs.dispatcher import job_dispatcher

    configured: list[int] = []

    async def capture_limit(value: int) -> None:
        configured.append(value)

    monkeypatch.setattr(job_dispatcher, "set_max_concurrent_jobs", capture_limit)
    page = await client.get("/settings/behavior")
    assert 'name="max_parallel_acquisitions"' in page.text

    response = await client.post(
        "/settings",
        data={"section": "behavior", "max_parallel_acquisitions": "7"},
        follow_redirects=False,
    )
    assert response.status_code == 303
    assert configured == [7]

    from app.database import get_session_factory
    from app.settings_service import get_runtime_settings

    async with get_session_factory()() as db:
        runtime = await get_runtime_settings(db)
    assert runtime.max_parallel_acquisitions == 7


async def test_concurrent_settings_saves_serialize_runtime_payload_and_live_limit(
    client: AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    from app.routers import settings as settings_router

    original_save = settings_router.save_runtime_settings
    active = 0
    peak = 0

    async def observed_save(*args: object, **kwargs: object) -> None:
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        await asyncio.sleep(0.05)
        try:
            await original_save(*args, **kwargs)  # type: ignore[arg-type]
        finally:
            active -= 1

    monkeypatch.setattr(settings_router, "save_runtime_settings", observed_save)
    responses = await asyncio.gather(
        client.post(
            "/settings",
            data={"section": "behavior", "max_parallel_acquisitions": "5"},
            follow_redirects=False,
        ),
        client.post(
            "/settings",
            data={"section": "behavior", "max_parallel_acquisitions": "9"},
            follow_redirects=False,
        ),
    )

    assert [response.status_code for response in responses] == [303, 303]
    assert peak == 1


@pytest.mark.asyncio
async def test_behavior_rejects_parallel_acquisition_limit_outside_range(
    client: AsyncClient,
) -> None:
    response = await client.post(
        "/settings",
        data={"section": "behavior", "max_parallel_acquisitions": "17"},
        follow_redirects=False,
    )
    assert response.status_code == 303
    assert response.headers["location"].startswith("/settings/behavior?error=")


@pytest.mark.asyncio
async def test_disabling_primary_metadata_provider_selects_an_enabled_replacement(
    client: AsyncClient,
) -> None:
    response = await client.post(
        "/settings",
        data={
            "section": "metadata",
            "metadata_order": ["musicbrainz", "deezer", "itunes"],
            "metadata_enabled": ["deezer", "itunes"],
            "primary_metadata_provider": "musicbrainz",
        },
        follow_redirects=False,
    )
    assert response.status_code == 303

    from app.database import get_session_factory
    from app.settings_service import get_runtime_settings

    factory = get_session_factory()
    async with factory() as db:
        runtime = await get_runtime_settings(db)
    assert runtime.primary_metadata_provider == "deezer"
    assert runtime.enabled_metadata_providers == ["deezer", "itunes"]


@pytest.mark.asyncio
async def test_invalid_provider_settings_form_redirects_with_error_instead_of_500(
    client: AsyncClient,
) -> None:
    response = await client.post(
        "/settings/save",
        data={
            "section": "download-clients",
            "tidal_quality": "lossless-plus",
        },
        follow_redirects=False,
    )
    assert response.status_code == 303
    assert response.headers["location"].startswith("/settings/download-clients?error=")


@pytest.mark.asyncio
async def test_quality_profile_renders_shipped_defaults_and_supported_controls(
    client: AsyncClient,
) -> None:
    response = await client.get("/settings/quality")
    assert response.status_code == 200
    body = response.text
    assert "Quality profile" in body
    for value in ("flac", "mp3", "m4a/aac", "ogg", "opus"):
        assert f'name="format_order" value="{value}"' in body
    assert '<option value="192"' in body
    assert '<option value="256"' in body
    assert '<option value="320" selected' in body
    assert 'name="allow_lower_quality_fallback"' in body
    assert "lyrics sidecars are always excluded" in body


@pytest.mark.asyncio
async def test_quality_profile_form_persists_order_bitrate_and_fallback(
    client: AsyncClient,
) -> None:
    response = await client.post(
        "/settings",
        data={
            "section": "quality",
            "format_order": ["mp3", "flac", "m4a/aac", "ogg", "opus"],
            "min_mp3_bitrate": "256",
            "allow_lower_quality_fallback": "true",
            "max_partial_attempts": "3",
        },
        follow_redirects=False,
    )
    assert response.status_code == 303
    assert response.headers["location"] == "/settings/quality?saved=1"

    page = await client.get("/settings/quality")
    assert page.text.index('value="mp3"') < page.text.index('value="flac"')
    assert '<option value="256" selected' in page.text
    assert 'name="allow_lower_quality_fallback" value="true" checked' in page.text


@pytest.mark.asyncio
async def test_quality_profile_rejects_invalid_bitrate_instead_of_clamping(
    client: AsyncClient,
) -> None:
    response = await client.post(
        "/settings",
        data={
            "section": "quality",
            "format_order": ["flac", "mp3", "m4a/aac", "ogg", "opus"],
            "min_mp3_bitrate": "255",
            "max_partial_attempts": "3",
        },
        follow_redirects=False,
    )
    assert response.status_code == 303
    assert response.headers["location"].startswith("/settings/quality?error=")

    page = await client.get("/settings/quality")
    assert 'value="255"' not in page.text
    assert all(f'<option value="{value}"' in page.text for value in (192, 256, 320))


@pytest.mark.asyncio
async def test_library_settings_naming_template_renders_default_placeholder(
    client: AsyncClient,
) -> None:
    response = await client.get("/settings/library")

    assert response.status_code == 200
    assert (
        'placeholder="{album_artist}/{album} ({year})/{disc_track} - {title}.{ext}"'
        in response.text
    )
    assert "<strong>Preview:</strong>" in response.text


@pytest.mark.asyncio
async def test_settings_root_renders_task_oriented_overview_without_live_probes(
    client: AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    from app.services.health_status import get_health_status_service

    async def forbidden_refresh(*args: object, **kwargs: object) -> object:
        raise AssertionError("overview GET must use cached status")

    monkeypatch.setattr(get_health_status_service(), "refresh_all", forbidden_refresh)
    monkeypatch.setattr(get_health_status_service(), "refresh_provider", forbidden_refresh)
    response = await client.get("/settings", follow_redirects=False)
    assert response.status_code == 200
    body = response.text
    for heading in (
        "Settings overview",
        "Acquisition",
        "Metadata & discovery",
        "Library & naming",
        "Automation",
        "Quality & verification",
        "Advanced & system",
    ):
        assert heading in body
    assert "Current source priority" in body
    assert "Current quality profile" in body
    assert "Environment-locked values" in body


@pytest.mark.asyncio
async def test_settings_overview_warning_links_target_rendered_anchors(
    client: AsyncClient,
) -> None:
    body = (await client.get("/settings")).text
    assert 'href="/settings/acquisition#source-priority"' in body
    assert 'href="/settings/library#library-root"' in body
    assert 'href="/settings/library#staging-root"' in body
    assert 'id="source-priority"' in (await client.get("/settings/acquisition")).text
    library = (await client.get("/settings/library")).text
    assert 'id="library-root"' in library and 'id="staging-root"' in library


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("legacy", "heading"),
    [
        ("download-sources", "Acquisition"),
        ("download-clients", "Acquisition"),
        ("behavior", "Advanced & system"),
        ("about", "Advanced & system"),
    ],
)
async def test_legacy_settings_sections_remain_as_aliases(
    client: AsyncClient, legacy: str, heading: str
) -> None:
    response = await client.get(f"/settings/{legacy}", follow_redirects=False)
    assert response.status_code == 200
    assert heading in response.text


@pytest.mark.asyncio
async def test_save_and_test_persists_and_refreshes_visible_status(
    client: AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    from app.sources.base import CapabilityState
    from app.sources.slskd import SlskdAdapter

    async def healthy(self: SlskdAdapter) -> CapabilityState:
        return CapabilityState(available=True)

    monkeypatch.setattr(SlskdAdapter, "health", healthy)
    response = await client.post(
        "/settings/save-and-test",
        data={
            "provider": "slskd",
            "section": "acquisition",
            "slskd_url": "http://saved-and-tested-slskd:5030",
            "slskd_api_key": "saved-and-tested-secret",
        },
        headers={"X-Requested-With": "fetch"},
    )
    assert response.status_code == 200
    result = response.json()
    assert result["saved"] is True and result["provider"] == "slskd"
    assert result["status"] == "Connected" and result["available"] is True
    stored = (await client.get("/api/settings")).json()
    assert stored["slskd_url"]["value"] == "http://saved-and-tested-slskd:5030"
    assert stored["slskd_api_key"]["value"] == "***"
    page = await client.get("/settings/acquisition")
    assert "Connected" in page.text and "saved-and-tested-secret" not in page.text


@pytest.mark.asyncio
async def test_save_and_test_ignores_fields_for_other_sections(
    client: AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    from app.sources.base import CapabilityState
    from app.sources.slskd import SlskdAdapter

    async def healthy(self: SlskdAdapter) -> CapabilityState:
        return CapabilityState(available=True)

    before = (await client.get("/api/settings")).json()["library_root"]["value"]
    monkeypatch.setattr(SlskdAdapter, "health", healthy)
    response = await client.post(
        "/settings/save-and-test",
        data={
            "provider": "slskd",
            "slskd_url": "http://slskd:5030",
            "slskd_api_key": "secret",
            "library_root": "/unrelated/crafted/path",
        },
        headers={"X-Requested-With": "fetch"},
    )
    assert response.status_code == 200
    after = (await client.get("/api/settings")).json()["library_root"]["value"]
    assert after == before


@pytest.mark.asyncio
async def test_provider_urls_reject_link_local_metadata_targets(client: AsyncClient) -> None:
    response = await client.post(
        "/api/settings/save",
        json={"slskd_url": "http://169.254.169.254/latest/meta-data"},
    )
    assert response.status_code == 422


@pytest.mark.asyncio
async def test_save_and_test_persists_configuration_when_connection_fails(
    client: AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    from app.sources.base import CapabilityState
    from app.sources.slskd import SlskdAdapter

    async def unavailable(self: SlskdAdapter) -> CapabilityState:
        return CapabilityState(available=False, reason="Authentication failed")

    monkeypatch.setattr(SlskdAdapter, "health", unavailable)
    response = await client.post(
        "/settings/save-and-test",
        data={
            "provider": "slskd",
            "section": "acquisition",
            "slskd_url": "http://slskd-needs-new-key:5030",
            "slskd_api_key": "wrong-but-safely-stored",
        },
        headers={"X-Requested-With": "fetch"},
    )
    assert response.status_code == 200
    assert response.json()["status"] == "Authentication failed"
    assert response.json()["saved"] is True
    stored = (await client.get("/api/settings")).json()
    assert stored["slskd_url"]["value"] == "http://slskd-needs-new-key:5030"


@pytest.mark.asyncio
async def test_path_test_uses_only_effective_configured_roots(
    client: AsyncClient, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    from app.config import Settings, override_settings
    from app.routers import settings as settings_router

    library, staging = tmp_path / "library", tmp_path / "staging"
    library.mkdir()
    staging.mkdir()
    override_settings(
        Settings(
            database_url="sqlite+aiosqlite:///:memory:",
            secret_key="test-secret",
            auth_cookie_secure=False,
            library_root=library,
            staging_root=staging,
        )
    )
    checked: list[str] = []
    original = settings_router._path_diagnostic

    def capture(path_value: str) -> str:
        checked.append(path_value)
        return original(path_value)

    monkeypatch.setattr(settings_router, "_path_diagnostic", capture)
    response = await client.post(
        "/settings/test-paths",
        data={"library_root": "/etc", "staging_root": "/"},
        follow_redirects=False,
    )
    assert response.status_code == 303
    assert checked == [str(library), str(staging)]
