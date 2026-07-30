from __future__ import annotations

from pathlib import Path

import pytest

from app.config import Settings
from app.crypto import encrypt_secret
from app.models.settings import ProviderSetting
from app.settings_service import (
    SECRET_KEYS,
    compute_effective_value,
    get_all_effective,
    save_settings,
)


def _make_settings(**kwargs: object) -> Settings:
    base = dict(
        database_url="sqlite+aiosqlite:///:memory:",
        secret_key="test-secret-key-xyz",
        auth_cookie_secure=False,
        library_root=Path("/music"),
        staging_root=Path("/staging"),
        slskd_url="",
        slskd_api_key="",
        prowlarr_url="",
        prowlarr_api_key="",
        sabnzbd_url="",
        sabnzbd_api_key="",
    )
    base.update(kwargs)
    return Settings(**base)  # type: ignore[arg-type]


# --- compute_effective_value ---


def test_env_plain_value_returned_when_set() -> None:
    settings = _make_settings(slskd_url="http://slskd:5030")
    ev = compute_effective_value("slskd_url", settings, {})
    assert ev.value == "http://slskd:5030"
    assert ev.configured is True
    assert ev.locked_by_environment is True


def test_db_plain_value_fills_when_env_blank() -> None:
    settings = _make_settings(slskd_url="")
    ev = compute_effective_value("slskd_url", settings, {"slskd_url": "http://db-slskd:5030"})
    assert ev.value == "http://db-slskd:5030"
    assert ev.configured is True
    assert ev.locked_by_environment is False


def test_env_plain_takes_precedence_over_db() -> None:
    settings = _make_settings(slskd_url="http://env-slskd")
    ev = compute_effective_value("slskd_url", settings, {"slskd_url": "http://db-slskd"})
    assert ev.value == "http://env-slskd"
    assert ev.locked_by_environment is True


def test_unconfigured_plain_field() -> None:
    settings = _make_settings(slskd_url="")
    ev = compute_effective_value("slskd_url", settings, {})
    assert ev.value == ""
    assert ev.configured is False
    assert ev.locked_by_environment is False


def test_secret_masked_when_env_set() -> None:
    settings = _make_settings(slskd_api_key="real-api-key")
    ev = compute_effective_value("slskd_api_key", settings, {})
    assert ev.value == "***"
    assert ev.configured is True
    assert ev.locked_by_environment is True


def test_secret_masked_when_db_set() -> None:
    settings = _make_settings(slskd_api_key="")
    ev = compute_effective_value("slskd_api_key", settings, {"slskd_api_key": "db-api-key"})
    assert ev.value == "***"
    assert ev.configured is True
    assert ev.locked_by_environment is False


def test_unconfigured_secret_returns_empty_string() -> None:
    settings = _make_settings(slskd_api_key="")
    ev = compute_effective_value("slskd_api_key", settings, {})
    assert ev.value == ""
    assert ev.configured is False


def test_secret_keys_set_contains_expected_members() -> None:
    assert "slskd_api_key" in SECRET_KEYS
    assert "prowlarr_api_key" in SECRET_KEYS
    assert "sabnzbd_api_key" in SECRET_KEYS
    assert "acoustid_api_key" in SECRET_KEYS
    assert "slskd_url" not in SECRET_KEYS


def test_model_default_library_root_can_be_overridden_by_database() -> None:
    settings = Settings(secret_key="test-secret-key-xyz")
    effective = compute_effective_value(
        "library_root", settings, {"library_root": "/custom/music"}
    )
    assert effective.value == "/custom/music"
    assert effective.locked_by_environment is False


def test_explicit_library_root_remains_environment_locked() -> None:
    settings = Settings(secret_key="test-secret-key-xyz", library_root=Path("/env/music"))
    effective = compute_effective_value("library_root", settings, {"library_root": "/db/music"})
    assert effective.value == "/env/music"
    assert effective.locked_by_environment is True


# --- get_all_effective ---


@pytest.mark.asyncio
async def test_get_all_effective_returns_all_keys(db_session: object) -> None:
    from sqlalchemy.ext.asyncio import AsyncSession

    assert isinstance(db_session, AsyncSession)
    settings = _make_settings()
    result = await get_all_effective(db_session, settings)
    expected_keys = {
        "slskd_url",
        "slskd_api_key",
        "prowlarr_url",
        "prowlarr_api_key",
        "sabnzbd_url",
        "sabnzbd_api_key",
        "ytdlp_cookies_file",
        "tidal_config_path",
        "tidal_session_path",
        "tidal_quality",
        "musicbrainz_contact",
        "acoustid_api_key",
        "library_root",
        "staging_root",
        "naming_template",
    }
    assert set(result.keys()) == expected_keys


# --- save_settings ---


@pytest.mark.asyncio
async def test_save_plain_setting_to_db(db_session: object) -> None:
    from sqlalchemy import select
    from sqlalchemy.ext.asyncio import AsyncSession

    assert isinstance(db_session, AsyncSession)
    settings = _make_settings()
    await save_settings(db_session, {"slskd_url": "http://new-slskd"}, settings)
    await db_session.flush()
    row = await db_session.scalar(
        select(ProviderSetting).where(ProviderSetting.key == "slskd_url")
    )
    assert row is not None
    assert row.value_plain == "http://new-slskd"
    assert row.value_encrypted is None


@pytest.mark.asyncio
async def test_save_secret_setting_encrypted(db_session: object) -> None:
    from sqlalchemy import select
    from sqlalchemy.ext.asyncio import AsyncSession

    assert isinstance(db_session, AsyncSession)
    settings = _make_settings()
    await save_settings(db_session, {"slskd_api_key": "my-api-key"}, settings)
    await db_session.flush()
    row = await db_session.scalar(
        select(ProviderSetting).where(ProviderSetting.key == "slskd_api_key")
    )
    assert row is not None
    assert row.value_plain is None
    assert row.value_encrypted is not None
    assert "my-api-key" not in row.value_encrypted


@pytest.mark.asyncio
async def test_blank_secret_keeps_existing(db_session: object) -> None:
    from sqlalchemy import select
    from sqlalchemy.ext.asyncio import AsyncSession

    assert isinstance(db_session, AsyncSession)
    settings = _make_settings()
    original_key = encrypt_secret("original-key", settings.secret_key)
    existing = ProviderSetting(
        key="slskd_api_key",
        value_encrypted=original_key,
        value_plain=None,
    )
    db_session.add(existing)
    await db_session.flush()

    await save_settings(db_session, {"slskd_api_key": ""}, settings)
    await db_session.flush()

    row = await db_session.scalar(
        select(ProviderSetting).where(ProviderSetting.key == "slskd_api_key")
    )
    assert row is not None
    assert row.value_encrypted == original_key


@pytest.mark.asyncio
async def test_locked_env_setting_not_overwritten(db_session: object) -> None:
    from sqlalchemy import select
    from sqlalchemy.ext.asyncio import AsyncSession

    assert isinstance(db_session, AsyncSession)
    settings = _make_settings(slskd_url="http://env-slskd")
    await save_settings(db_session, {"slskd_url": "http://attempt-override"}, settings)
    await db_session.flush()
    row = await db_session.scalar(
        select(ProviderSetting).where(ProviderSetting.key == "slskd_url")
    )
    assert row is None


@pytest.mark.asyncio
async def test_validate_only_does_not_write(db_session: object) -> None:
    from sqlalchemy import select
    from sqlalchemy.ext.asyncio import AsyncSession

    assert isinstance(db_session, AsyncSession)
    settings = _make_settings()
    await save_settings(db_session, {"slskd_url": "http://test-url"}, settings, validate_only=True)
    await db_session.flush()
    row = await db_session.scalar(
        select(ProviderSetting).where(ProviderSetting.key == "slskd_url")
    )
    assert row is None


@pytest.mark.asyncio
async def test_primary_metadata_provider_must_be_enabled(db_session: object) -> None:
    from sqlalchemy.ext.asyncio import AsyncSession

    from app.settings_service import save_runtime_settings

    assert isinstance(db_session, AsyncSession)
    with pytest.raises(ValueError, match="Primary metadata provider must be enabled"):
        await save_runtime_settings(
            db_session,
            [{"name": "slskd", "enabled": True}],
            10,
            metadata_providers=[
                {"name": "musicbrainz", "enabled": True},
                {"name": "deezer", "enabled": False},
            ],
            primary_metadata_provider="deezer",
        )


@pytest.mark.asyncio
async def test_runtime_settings_persist_monitoring_defaults_and_primary_provider(
    db_session: object,
) -> None:
    from sqlalchemy.ext.asyncio import AsyncSession

    from app.settings_service import get_runtime_settings, save_runtime_settings

    assert isinstance(db_session, AsyncSession)
    await save_runtime_settings(
        db_session,
        [{"name": "youtube", "enabled": True}, {"name": "slskd", "enabled": False}],
        7,
        metadata_providers=[
            {"name": "deezer", "enabled": True},
            {"name": "musicbrainz", "enabled": True},
        ],
        primary_metadata_provider="deezer",
        discography_refresh_hours=12,
        auto_download_wanted=True,
        source_search_budget_seconds=9,
        default_watchlist_release_albums=True,
        default_watchlist_release_singles=False,
        default_watchlist_release_eps=True,
        default_watchlist_monitor_upgrades=True,
    )

    runtime = await get_runtime_settings(db_session)

    assert runtime.enabled_sources[0] == "youtube"
    assert runtime.free_text_result_limit == 7
    assert runtime.primary_metadata_provider == "deezer"
    assert runtime.discography_refresh_hours == 12
    assert runtime.auto_download_wanted is True
    assert runtime.source_search_budget_seconds == 9
    assert runtime.default_watchlist_release_albums is True
    assert runtime.default_watchlist_release_singles is False
    assert runtime.default_watchlist_release_eps is True
    assert runtime.default_watchlist_monitor_upgrades is True
