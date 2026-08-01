from __future__ import annotations

import asyncio
import logging
import tempfile
from pathlib import Path
from typing import Annotated
from urllib.parse import quote_plus

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from pydantic import ValidationError
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth import require_admin, require_admin_read, require_mutation
from app.config import Settings, get_settings
from app.database import get_db
from app.jobs.dispatcher import job_dispatcher
from app.models.auth import AppUser
from app.naming.convention import render_path
from app.schemas.health import SourceStatus
from app.schemas.settings import SettingField, SettingsSaveRequest, SettingsTestRequest
from app.services.health_status import CachedProviderStatus, get_health_status_service
from app.settings_service import (
    ALLOWED_MIN_MP3_BITRATES,
    DEFAULT_FORMAT_PREFERENCE,
    DEFAULT_METADATA_PROVIDERS,
    DEFAULT_SOURCE_PRIORITY,
    QualityProfile,
    get_all_effective,
    get_runtime_settings,
    load_raw_db_values,
    resolve_for_probe,
    save_runtime_settings,
    save_settings,
)
from app.sources.prowlarr import ProwlarrAdapter
from app.sources.sabnzbd import SabnzbdAdapter
from app.sources.slskd import SlskdAdapter
from app.sources.tidal import TidalAdapter
from app.sources.youtube import YouTubeAdapter

router = APIRouter(tags=["settings"])
logger = logging.getLogger(__name__)


def _get_templates(request: Request) -> Jinja2Templates:
    return request.app.state.templates  # type: ignore[no-any-return]


async def _probe_provider(
    provider: str,
    url: str,
    key: str,
    cookies: str,
) -> SourceStatus:
    from app.sources.base import CapabilityState

    cap: CapabilityState
    if provider == "slskd":
        cap = await SlskdAdapter(url, key).health()
    elif provider == "prowlarr":
        cap = await ProwlarrAdapter(url, key).health()
    elif provider == "sabnzbd":
        cap = await SabnzbdAdapter(url, key).health()
    elif provider == "youtube":
        cap = await YouTubeAdapter(cookies).health()
    else:
        cap = CapabilityState(available=False, reason="Unknown provider")
    return SourceStatus(available=cap.available, reason=cap.reason, details=cap.extra)


SETTINGS_SECTIONS = {
    "download-sources": "Download sources",
    "download-clients": "Download clients",
    "metadata": "Metadata",
    "library": "Library",
    "behavior": "Behavior",
    "quality": "Quality",
    "about": "About",
}
PROVIDER_DESCRIPTIONS = {
    "musicbrainz": "MusicBrainz — canonical IDs, required",
    "deezer": "Deezer — artwork and streaming catalog IDs",
    "itunes": "iTunes — store catalog IDs and artwork fallback",
}


async def _client_statuses(db: AsyncSession, env: Settings) -> dict[str, SourceStatus]:
    raw_db = await load_raw_db_values(db, env.secret_key)

    def _r(key: str) -> str:
        return resolve_for_probe(key, "", env, raw_db)

    statuses: dict[str, SourceStatus] = {}
    for provider in ["slskd", "prowlarr", "sabnzbd"]:
        if not (_r(f"{provider}_url") and _r(f"{provider}_api_key")):
            statuses[provider] = SourceStatus(available=False, reason="Not configured", details={})
        else:
            statuses[provider] = await _probe_provider(
                provider, _r(f"{provider}_url"), _r(f"{provider}_api_key"), ""
            )
    statuses["youtube"] = await _probe_provider("youtube", "", "", _r("ytdlp_cookies_file"))
    cap = await TidalAdapter(
        _r("tidal_config_path"), _r("tidal_session_path"), _r("tidal_quality")
    ).health()
    statuses["tidal"] = SourceStatus(available=cap.available, reason=cap.reason, details=cap.extra)
    return statuses


def _naming_preview(template: str) -> str:
    try:
        return str(
            render_path(
                {
                    "album_artist": "Queen",
                    "artist": "Queen",
                    "album": "A Night at the Opera",
                    "year": "1975",
                    "disc": 1,
                    "disc_total": 1,
                    "track_no": 11,
                    "title": "Bohemian Rhapsody",
                    "ext": "flac",
                    "naming_template": template,
                },
                template=template,
            )
        )
    except Exception as exc:
        return f"Template error: {exc}"


def _path_diagnostic(path_value: str) -> str:
    path = Path(path_value)
    if not path.exists():
        return "Missing"
    if not path.is_dir():
        return "Not a directory"
    try:
        with tempfile.NamedTemporaryFile(prefix=".audiohoard-write-check-", dir=path):
            pass
    except OSError as exc:
        return f"Not writable: {exc.strerror or 'permission denied'}"
    return "Ready"


async def _validate_provider_updates(
    payload: SettingsSaveRequest, db: AsyncSession, env: Settings
) -> dict[str, str]:
    raw_db = await load_raw_db_values(db, env.secret_key)

    def current(key: str) -> str:
        return resolve_for_probe(key, "", env, raw_db)

    submitted_fields = payload.model_fields_set

    def changed(key: str, submitted: str | None, *, secret: bool = False) -> bool:
        if key not in submitted_fields:
            return False
        if secret and not submitted:
            return False
        return (submitted or "") != current(key)

    errors: dict[str, str] = {}
    for provider, url_key, key_key, url_value, key_value in (
        ("slskd", "slskd_url", "slskd_api_key", payload.slskd_url, payload.slskd_api_key),
        (
            "prowlarr",
            "prowlarr_url",
            "prowlarr_api_key",
            payload.prowlarr_url,
            payload.prowlarr_api_key,
        ),
        (
            "sabnzbd",
            "sabnzbd_url",
            "sabnzbd_api_key",
            payload.sabnzbd_url,
            payload.sabnzbd_api_key,
        ),
    ):
        if not (changed(url_key, url_value) or changed(key_key, key_value, secret=True)):
            continue
        effective_url = url_value if url_value is not None else current(url_key)
        effective_key = key_value or current(key_key)
        if not (effective_url and effective_key):
            errors[provider] = "URL and API key are required together"
            continue
        status = await _probe_provider(provider, url=effective_url, key=effective_key, cookies="")
        if not status.available:
            errors[provider] = status.reason or "Connection failed"
    return errors


@router.get("/settings", response_class=HTMLResponse, include_in_schema=False)
async def settings_page(
    request: Request,
    _admin: Annotated[AppUser, Depends(require_admin_read)],
) -> RedirectResponse:
    return RedirectResponse("/settings/download-sources", status_code=307)


@router.get("/settings/{section}", response_class=HTMLResponse, include_in_schema=False)
async def settings_section_page(
    section: str,
    request: Request,
    _admin: Annotated[AppUser, Depends(require_admin_read)],
    db: Annotated[AsyncSession, Depends(get_db)],
    env: Annotated[Settings, Depends(get_settings)],
) -> HTMLResponse:
    if section not in SETTINGS_SECTIONS:
        raise HTTPException(status_code=404, detail="Unknown settings section")
    effective = await get_all_effective(db, env)
    fields = {
        k: SettingField(
            value=v.value, configured=v.configured, locked_by_environment=v.locked_by_environment
        )
        for k, v in effective.items()
    }
    runtime = await get_runtime_settings(db)
    client_statuses = (
        get_health_status_service().snapshot() if section == "download-clients" else {}
    )
    if section == "download-clients":
        enabled_sources = {
            str(item["name"]) for item in runtime.source_priority if item.get("enabled") is True
        }
        for provider in ("slskd", "prowlarr", "youtube", "tidal"):
            if provider not in enabled_sources:
                client_statuses[provider] = CachedProviderStatus(
                    SourceStatus(available=False, reason="Disabled", details={})
                )
        if "prowlarr" not in enabled_sources:
            client_statuses["sabnzbd"] = CachedProviderStatus(
                SourceStatus(available=False, reason="Disabled", details={})
            )
    path_diagnostics: dict[str, str] = {}
    if section == "library":
        library_status, staging_status = await asyncio.gather(
            asyncio.to_thread(_path_diagnostic, fields["library_root"].value),
            asyncio.to_thread(_path_diagnostic, fields["staging_root"].value),
        )
        path_diagnostics = {
            "library_root": library_status,
            "staging_root": staging_status,
        }
    return _get_templates(request).TemplateResponse(
        request,
        "settings.html",
        {
            "settings": fields,
            "runtime": runtime,
            "default_sources": DEFAULT_SOURCE_PRIORITY,
            "default_metadata_providers": DEFAULT_METADATA_PROVIDERS,
            "saved": request.query_params.get("saved", ""),
            "test_result": request.query_params.get("test", ""),
            "validation_error": request.query_params.get("error", ""),
            "section": section,
            "sections": SETTINGS_SECTIONS,
            "client_statuses": client_statuses,
            "provider_descriptions": PROVIDER_DESCRIPTIONS,
            "naming_preview": _naming_preview(fields["naming_template"].value),
            "path_diagnostics": path_diagnostics,
            "app_version": request.app.version,
        },
    )


@router.post("/settings", response_class=HTMLResponse, include_in_schema=False)
async def save_runtime_settings_page(
    request: Request,
    db: Annotated[AsyncSession, Depends(get_db)],
    _user: Annotated[object, Depends(require_mutation)],
) -> RedirectResponse:
    form = await request.form()
    runtime = await get_runtime_settings(db)
    order = [str(v) for v in form.getlist("source_order")]
    move_source = str(form.get("move_source", ""))
    if ":" in move_source:
        direction, source_name = move_source.split(":", 1)
        if source_name in order:
            index = order.index(source_name)
            move_target_index = index - 1 if direction == "up" else index + 1
            if direction in {"up", "down"} and 0 <= move_target_index < len(order):
                order[index], order[move_target_index] = order[move_target_index], order[index]
    enabled = {str(v) for v in form.getlist("source_enabled")}
    source_priority = [{"name": name, "enabled": name in enabled} for name in order]
    metadata_order = [str(v) for v in form.getlist("metadata_order")]
    metadata_enabled = {str(v) for v in form.getlist("metadata_enabled")}
    if metadata_order and not metadata_enabled:
        return RedirectResponse(
            "/settings/metadata?error=At+least+one+metadata+provider+must+remain+enabled",
            status_code=303,
        )
    metadata_providers = (
        [{"name": name, "enabled": name in metadata_enabled} for name in metadata_order]
        if metadata_order
        else runtime.metadata_providers
    )
    try:
        limit = int(
            str(form.get("free_text_result_limit", runtime.free_text_result_limit)) or "10"
        )
        refresh_hours = int(
            str(form.get("discography_refresh_hours", runtime.discography_refresh_hours)) or "24"
        )
        library_scan_hours = int(
            str(form.get("library_scan_hours", runtime.library_scan_hours)) or "0"
        )
        duplicate_scan_hours = int(
            str(form.get("duplicate_scan_hours", runtime.duplicate_scan_hours)) or "0"
        )
        upgrade_check_hours = int(
            str(form.get("upgrade_check_hours", runtime.upgrade_check_hours)) or "0"
        )
        source_budget = int(
            str(form.get("source_search_budget_seconds", runtime.source_search_budget_seconds))
            or "300"
        )
        slskd_timeout = int(
            str(form.get("slskd_download_timeout_seconds", runtime.slskd_download_timeout_seconds))
            or "1800"
        )
        max_parallel = int(
            str(form.get("max_parallel_acquisitions", runtime.max_parallel_acquisitions)) or "3"
        )
        acoustid_threshold = float(
            str(form.get("acoustid_acceptance_threshold", runtime.acoustid_acceptance_threshold))
            or "0.90"
        )
    except ValueError:
        return RedirectResponse(
            "/settings/behavior?error=Behavior+values+must+be+whole+numbers", status_code=303
        )
    if not (
        1 <= limit <= 100
        and 1 <= refresh_hours <= 720
        and 0 <= library_scan_hours <= 720
        and 0 <= duplicate_scan_hours <= 720
        and 0 <= upgrade_check_hours <= 720
        and 3 <= source_budget <= 900
        and 10 <= slskd_timeout <= 86400
        and 1 <= max_parallel <= 16
        and 0.5 <= acoustid_threshold <= 0.9999
    ):
        return RedirectResponse(
            "/settings/behavior?error=Behavior+values+are+outside+the+allowed+range",
            status_code=303,
        )
    primary = str(form.get("primary_metadata_provider", runtime.primary_metadata_provider))
    if metadata_order and primary not in metadata_enabled:
        primary = next(name for name in metadata_order if name in metadata_enabled)
    section = str(form.get("section", ""))
    auto_download = (
        str(form.get("auto_download_wanted", "")).lower() in {"1", "true", "yes", "on"}
        if section == "behavior"
        else runtime.auto_download_wanted
    )
    duplicate_auto_clean = (
        str(form.get("duplicate_auto_clean", "")).lower() in {"1", "true", "yes", "on"}
        if section == "behavior"
        else runtime.duplicate_auto_clean
    )
    default_watchlist_release_albums = (
        str(form.get("default_watchlist_release_albums", "")).lower() in {"1", "true", "yes", "on"}
        if section == "behavior"
        else runtime.default_watchlist_release_albums
    )
    default_watchlist_release_singles = (
        str(form.get("default_watchlist_release_singles", "")).lower()
        in {"1", "true", "yes", "on"}
        if section == "behavior"
        else runtime.default_watchlist_release_singles
    )
    default_watchlist_release_eps = (
        str(form.get("default_watchlist_release_eps", "")).lower() in {"1", "true", "yes", "on"}
        if section == "behavior"
        else runtime.default_watchlist_release_eps
    )
    default_watchlist_monitor_upgrades = (
        str(form.get("default_watchlist_monitor_upgrades", "")).lower()
        in {"1", "true", "yes", "on"}
        if section == "behavior"
        else runtime.default_watchlist_monitor_upgrades
    )
    if section == "quality":
        fmt_order = [str(v) for v in form.getlist("format_order")]
        if len(fmt_order) != len(DEFAULT_FORMAT_PREFERENCE) or set(fmt_order) != set(
            DEFAULT_FORMAT_PREFERENCE
        ):
            return RedirectResponse(
                "/settings/quality?error=Format+order+must+contain+each+supported+format+once",
                status_code=303,
            )
        move_fmt = str(form.get("move_format", ""))
        if ":" in move_fmt:
            direction, fmt_name = move_fmt.split(":", 1)
            if fmt_name in fmt_order:
                idx = fmt_order.index(fmt_name)
                move_idx = idx - 1 if direction == "up" else idx + 1
                if direction in {"up", "down"} and 0 <= move_idx < len(fmt_order):
                    fmt_order[idx], fmt_order[move_idx] = fmt_order[move_idx], fmt_order[idx]
        try:
            min_mp3 = int(
                str(form.get("min_mp3_bitrate", runtime.quality_profile.min_mp3_bitrate))
            )
        except ValueError:
            return RedirectResponse("/settings/quality?error=Invalid+MP3+bitrate", status_code=303)
        if min_mp3 not in ALLOWED_MIN_MP3_BITRATES:
            return RedirectResponse(
                "/settings/quality?error=MP3+bitrate+must+be+192%2C+256%2C+or+320+kbps",
                status_code=303,
            )
        allow_fallback = str(form.get("allow_lower_quality_fallback", "")).lower() in {
            "1",
            "true",
            "yes",
            "on",
        }
        try:
            max_partial = int(str(form.get("max_partial_attempts", runtime.max_partial_attempts)))
        except ValueError:
            return RedirectResponse(
                "/settings/quality?error=Invalid+partial+attempt+limit", status_code=303
            )
        if not 1 <= max_partial <= 10:
            return RedirectResponse(
                "/settings/quality?error=Partial+attempt+limit+must+be+between+1+and+10",
                status_code=303,
            )
        quality_profile = QualityProfile(
            format_preference=fmt_order or list(DEFAULT_FORMAT_PREFERENCE),
            min_mp3_bitrate=min_mp3,
            allow_lower_quality_fallback=allow_fallback,
        )
        max_partial_val: int = max_partial
    else:
        quality_profile = runtime.quality_profile
        max_partial_val = runtime.max_partial_attempts
    settings_lock = getattr(request.app.state, "runtime_settings_write_lock", None)
    if settings_lock is None:
        settings_lock = asyncio.Lock()
        request.app.state.runtime_settings_write_lock = settings_lock
    async with settings_lock:
        current = await get_runtime_settings(db)
        await save_runtime_settings(
            db,
            (source_priority or current.source_priority)
            if section == "download-sources"
            else current.source_priority,
            limit if section == "behavior" else current.free_text_result_limit,
            metadata_providers if section == "metadata" else current.metadata_providers,
            primary if section == "metadata" else current.primary_metadata_provider,
            refresh_hours if section == "behavior" else current.discography_refresh_hours,
            library_scan_hours if section == "behavior" else current.library_scan_hours,
            duplicate_scan_hours if section == "behavior" else current.duplicate_scan_hours,
            duplicate_auto_clean if section == "behavior" else current.duplicate_auto_clean,
            upgrade_check_hours if section == "behavior" else current.upgrade_check_hours,
            auto_download if section == "behavior" else current.auto_download_wanted,
            source_search_budget_seconds=(
                source_budget if section == "behavior" else current.source_search_budget_seconds
            ),
            default_watchlist_release_albums=(
                default_watchlist_release_albums
                if section == "behavior"
                else current.default_watchlist_release_albums
            ),
            default_watchlist_release_singles=(
                default_watchlist_release_singles
                if section == "behavior"
                else current.default_watchlist_release_singles
            ),
            default_watchlist_release_eps=(
                default_watchlist_release_eps
                if section == "behavior"
                else current.default_watchlist_release_eps
            ),
            default_watchlist_monitor_upgrades=(
                default_watchlist_monitor_upgrades
                if section == "behavior"
                else current.default_watchlist_monitor_upgrades
            ),
            quality_profile=quality_profile if section == "quality" else current.quality_profile,
            max_partial_attempts=(
                max_partial_val if section == "quality" else current.max_partial_attempts
            ),
            acoustid_acceptance_threshold=(
                acoustid_threshold
                if section == "behavior"
                else current.acoustid_acceptance_threshold
            ),
            slskd_download_timeout_seconds=(
                slskd_timeout
                if section == "download-sources"
                else current.slskd_download_timeout_seconds
            ),
            max_parallel_acquisitions=(
                max_parallel if section == "behavior" else current.max_parallel_acquisitions
            ),
        )
        await db.commit()
        persisted = await get_runtime_settings(db)
        await job_dispatcher.set_max_concurrent_jobs(persisted.max_parallel_acquisitions)
    redirect_target = section if section in SETTINGS_SECTIONS else "download-sources"
    return RedirectResponse(f"/settings/{redirect_target}?saved=1", status_code=303)


@router.post("/settings/save", include_in_schema=False)
async def save_settings_page(
    request: Request,
    db: Annotated[AsyncSession, Depends(get_db)],
    env: Annotated[Settings, Depends(get_settings)],
    _user: Annotated[object, Depends(require_mutation)],
) -> RedirectResponse:
    form = await request.form()
    section = str(form.get("section", "download-clients"))
    allowed = set(SettingsSaveRequest.model_fields)
    updates = {str(key): str(value) for key, value in form.items() if str(key) in allowed}
    if "naming_template" in updates:
        preview = _naming_preview(updates["naming_template"])
        if preview.startswith("Template error:"):
            return RedirectResponse(
                f"/settings/library?error={quote_plus(preview)}", status_code=303
            )
    try:
        payload = SettingsSaveRequest.model_validate(updates)
    except ValidationError:
        target = section if section in SETTINGS_SECTIONS else "download-clients"
        return RedirectResponse(
            f"/settings/{target}?error=Invalid+settings+value", status_code=303
        )
    validation_errors = await _validate_provider_updates(payload, db, env)
    if validation_errors:
        detail = "; ".join(
            f"{provider}: {message}" for provider, message in validation_errors.items()
        )
        return RedirectResponse(
            f"/settings/download-clients?error={quote_plus(detail)}", status_code=303
        )
    await save_settings(db, updates, env)
    await db.commit()
    target = section if section in SETTINGS_SECTIONS else "download-clients"
    return RedirectResponse(f"/settings/{target}?saved=1", status_code=303)


@router.post("/settings/refresh", include_in_schema=False)
async def refresh_provider_status_page(
    request: Request,
    db: Annotated[AsyncSession, Depends(get_db)],
    env: Annotated[Settings, Depends(get_settings)],
    _user: Annotated[object, Depends(require_mutation)],
) -> RedirectResponse:
    form = await request.form()
    provider = str(form.get("provider", ""))
    section = str(form.get("section", "download-clients"))
    await get_health_status_service().refresh_provider(provider, db, env)
    target = section if section in SETTINGS_SECTIONS else "download-clients"
    return RedirectResponse(f"/settings/{target}?test={provider}:refreshed", status_code=303)


@router.post("/settings/test", include_in_schema=False)
async def test_provider_page(
    request: Request,
    db: Annotated[AsyncSession, Depends(get_db)],
    env: Annotated[Settings, Depends(get_settings)],
    _user: Annotated[object, Depends(require_mutation)],
) -> RedirectResponse:
    form = await request.form()
    provider = str(form.get("provider", ""))
    raw_db = await load_raw_db_values(db, env.secret_key)

    def _r(key: str) -> str:
        submitted = str(form.get(key, ""))
        return resolve_for_probe(key, submitted, env, raw_db)

    if provider == "tidal":
        cap = await TidalAdapter(
            _r("tidal_config_path"), _r("tidal_session_path"), _r("tidal_quality")
        ).health()
        status = SourceStatus(available=cap.available, reason=cap.reason, details=cap.extra)
    elif provider == "youtube":
        status = await _probe_provider("youtube", url="", key="", cookies=_r("ytdlp_cookies_file"))
    elif provider in {"slskd", "prowlarr", "sabnzbd"}:
        status = await _probe_provider(
            provider,
            url=_r(f"{provider}_url"),
            key=_r(f"{provider}_api_key"),
            cookies="",
        )
    else:
        status = SourceStatus(available=False, reason="Unknown provider", details={})
    result = "ok" if status.available else (status.reason or "failed")
    return RedirectResponse(
        f"/settings/download-clients?test={provider}:{result}", status_code=303
    )


@router.get("/api/settings", response_model=dict[str, SettingField])
async def get_settings_api(
    _admin: Annotated[AppUser, Depends(require_admin_read)],
    db: Annotated[AsyncSession, Depends(get_db)],
    env: Annotated[Settings, Depends(get_settings)],
) -> dict[str, SettingField]:
    effective = await get_all_effective(db, env)
    return {
        k: SettingField(
            value=v.value,
            configured=v.configured,
            locked_by_environment=v.locked_by_environment,
        )
        for k, v in effective.items()
    }


@router.post("/api/settings/test", response_model=SourceStatus)
async def test_provider(
    payload: SettingsTestRequest,
    _admin: Annotated[AppUser, Depends(require_admin)],
    db: Annotated[AsyncSession, Depends(get_db)],
    env: Annotated[Settings, Depends(get_settings)],
) -> SourceStatus:
    raw_db = await load_raw_db_values(db, env.secret_key)

    def _r(key: str, supplied: str) -> str:
        return resolve_for_probe(key, supplied, env, raw_db)

    if payload.provider == "slskd":
        return await _probe_provider(
            "slskd",
            url=_r("slskd_url", payload.slskd_url),
            key=_r("slskd_api_key", payload.slskd_api_key),
            cookies="",
        )
    if payload.provider == "prowlarr":
        return await _probe_provider(
            "prowlarr",
            url=_r("prowlarr_url", payload.prowlarr_url),
            key=_r("prowlarr_api_key", payload.prowlarr_api_key),
            cookies="",
        )
    if payload.provider == "sabnzbd":
        return await _probe_provider(
            "sabnzbd",
            url=_r("sabnzbd_url", payload.sabnzbd_url),
            key=_r("sabnzbd_api_key", payload.sabnzbd_api_key),
            cookies="",
        )
    if payload.provider == "youtube":
        return await _probe_provider(
            "youtube",
            url="",
            key="",
            cookies=_r("ytdlp_cookies_file", payload.ytdlp_cookies_file),
        )
    if payload.provider == "tidal":
        cap = await TidalAdapter(
            _r("tidal_config_path", payload.tidal_config_path),
            _r("tidal_session_path", payload.tidal_session_path),
            _r("tidal_quality", payload.tidal_quality),
        ).health()
        return SourceStatus(available=cap.available, reason=cap.reason, details=cap.extra)
    return SourceStatus(available=False, reason="Unknown provider", details={})  # pragma: no cover


@router.post("/api/settings/save")
async def save_settings_api(
    payload: SettingsSaveRequest,
    _admin: Annotated[AppUser, Depends(require_admin)],
    db: Annotated[AsyncSession, Depends(get_db)],
    env: Annotated[Settings, Depends(get_settings)],
) -> dict[str, str]:
    validation_errors = await _validate_provider_updates(payload, db, env)
    if validation_errors:
        raise HTTPException(
            status_code=422,
            detail={"validation_errors": validation_errors},
        )

    updates = {
        key: value
        for key, value in payload.model_dump(exclude_unset=True).items()
        if value is not None
    }
    await save_settings(db, updates, env)
    return {"status": "saved"}
