from __future__ import annotations

from ipaddress import ip_address
from typing import Literal
from urllib.parse import urlsplit

from pydantic import BaseModel, field_validator


class SettingField(BaseModel):
    value: str
    configured: bool
    locked_by_environment: bool


def validate_provider_url_value(value: str | None) -> str | None:
    if value is None or not value.strip():
        return value
    try:
        parsed = urlsplit(value)
        port = parsed.port
    except ValueError as exc:
        raise ValueError("provider URL is invalid") from exc
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        raise ValueError("provider URL must use http or https")
    if parsed.username or parsed.password or parsed.fragment:
        raise ValueError("provider URL cannot contain credentials or fragments")
    if port is not None and not 1 <= port <= 65535:
        raise ValueError("provider URL port is invalid")
    hostname = parsed.hostname.casefold().rstrip(".")
    if hostname in {"metadata.google.internal", "metadata.google.internal."}:
        raise ValueError("provider URL host is not allowed")
    try:
        address = ip_address(hostname)
    except ValueError:
        return value
    if (
        address.is_link_local
        or address.is_multicast
        or address.is_unspecified
        or address.is_reserved
    ):
        raise ValueError("provider URL address is not allowed")
    return value


class SettingsSaveRequest(BaseModel):
    slskd_url: str | None = None
    slskd_api_key: str | None = None
    prowlarr_url: str | None = None
    prowlarr_api_key: str | None = None
    sabnzbd_url: str | None = None
    sabnzbd_api_key: str | None = None
    ytdlp_cookies_file: str | None = None
    tidal_config_path: str | None = None
    tidal_session_path: str | None = None
    tidal_quality: Literal["", "Normal", "High", "HiFi", "Master"] | None = None
    musicbrainz_contact: str | None = None
    acoustid_api_key: str | None = None
    library_root: str | None = None
    staging_root: str | None = None
    naming_template: str | None = None

    @field_validator("slskd_url", "prowlarr_url", "sabnzbd_url")
    @classmethod
    def validate_provider_url(cls, value: str | None) -> str | None:
        return validate_provider_url_value(value)


_TESTABLE_PROVIDERS = Literal["slskd", "prowlarr", "sabnzbd", "youtube", "tidal"]


class SettingsTestRequest(BaseModel):
    provider: _TESTABLE_PROVIDERS
    slskd_url: str = ""
    slskd_api_key: str = ""
    prowlarr_url: str = ""
    prowlarr_api_key: str = ""
    sabnzbd_url: str = ""
    sabnzbd_api_key: str = ""
    ytdlp_cookies_file: str = ""
    tidal_config_path: str = ""
    tidal_session_path: str = ""
    tidal_quality: Literal["", "Normal", "High", "HiFi", "Master"] = ""

    @field_validator("slskd_url", "prowlarr_url", "sabnzbd_url")
    @classmethod
    def validate_provider_url(cls, value: str) -> str:
        return validate_provider_url_value(value) or ""
