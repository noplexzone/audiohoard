from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).parents[2]
TEMPLATES = ROOT / "app" / "templates"
STATIC = ROOT / "app" / "static"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def test_authenticated_shell_keeps_one_global_audio_outside_page_region() -> None:
    base = _read(TEMPLATES / "base.html")

    assert base.count("<audio") == 1
    assert "data-progressive-shell" in base
    assert "data-page-region" in base
    assert 'id="global-audio"' in base
    assert base.index("data-page-region") < base.index('id="global-player"')
    assert base.index('id="global-player"') < base.index('id="global-audio"')
    player_tag = base[
        base.index('<section class="global-player"') : base.index(
            ">", base.index('<section class="global-player"')
        )
    ]
    assert " hidden" in player_tag
    assert 'aria-hidden="true"' in player_tag
    assert "Nothing playing" not in base
    assert "Choose a track from your library" not in base
    assert "/static/js/player.js?v={{ app_version }}" in base
    assert "/static/js/navigation.js?v={{ app_version }}" in base
    assert not re.search(r"<script(?![^>]*\bsrc=)[^>]*>\s*\S", base, re.IGNORECASE)


def test_player_shell_exposes_complete_accessible_controls() -> None:
    base = _read(TEMPLATES / "base.html")

    for contract in (
        "data-player-previous",
        "data-player-toggle",
        "data-player-next",
        "data-player-seek",
        "data-player-volume",
        "data-player-mute",
        "data-player-title",
        "data-player-artist",
        "data-player-current-time",
        "data-player-duration",
        "data-player-status",
        'aria-live="polite"',
    ):
        assert contract in base


def test_server_rendered_track_hooks_are_explicit_and_skip_missing_catalog_tracks() -> None:
    library = _read(TEMPLATES / "library_tracks.html")
    artist = _read(TEMPLATES / "artist_detail.html")
    album = _read(TEMPLATES / "catalog_album.html")
    track = _read(TEMPLATES / "track.html")

    for template in (library, artist, track):
        assert 'data-play-url="/library/tracks/{{ track.id }}/audio"' in template
        assert 'data-track-id="{{ track.id }}"' in template
        assert 'data-track-title="{{ track.title' in template
        assert "data-track-artist=" in template
    assert "progress.library_track_id(track.id)" in album
    assert 'data-play-url="/library/tracks/{{ library_track_id }}/audio"' in album
    assert 'disabled aria-disabled="true"' in album


def test_navigation_source_preserves_native_fallback_and_page_lifecycle() -> None:
    source = _read(STATIC / "js" / "navigation.js")

    for contract in (
        "AbortController",
        "DOMParser",
        "popstate",
        "pushState",
        "content-type",
        "location.assign",
        "data-page-region",
        "audiohoard:page-dispose",
        "audiohoard:page-init",
        "aria-current",
        "focus",
    ):
        assert contract in source
    assert "event.metaKey" in source
    assert "event.ctrlKey" in source
    assert "anchor.download" in source
    assert "url.origin !== window.location.origin" in source
    assert "scrollRestoration" in source
    assert "scrollY" in source and "scrollX" in source
    assert "decodeURIComponent" in source and "url.hash" in source
    assert "AudiohoardNavigation" in source and "refresh" in source


def test_navigation_source_intercepts_download_forms_globally() -> None:
    source = _read(STATIC / "js" / "navigation.js")

    for contract in (
        "document.addEventListener('submit'",
        "form[data-download-form]",
        "event.preventDefault()",
        "X-Requested-With': 'fetch'",
        "new FormData(form)",
        "Queueing…",
        "Nothing to queue",
        "Download request failed",
    ):
        assert contract in source

    assert "event.defaultPrevented" in source


def test_player_source_handles_queue_transcode_media_and_keyboard_safely() -> None:
    source = _read(STATIC / "js" / "player.js")

    for contract in (
        "data-play-url",
        "data-track-id",
        "AbortController",
        "audiohoard:page-dispose",
        "?transcode=mp3",
        "mediaSession",
        "setActionHandler",
        "loadedmetadata",
        "timeupdate",
        "volumechange",
        "waiting",
        "playing",
        "error",
    ):
        assert contract in source
    assert "INPUT" in source and "TEXTAREA" in source and "SELECT" in source
    assert "isContentEditable" in source
    assert "player.hidden = false" in source
    assert "player-visible" in source


def test_import_review_audio_volume_defaults_to_max_and_persists_user_changes() -> None:
    source = _read(STATIC / "js" / "player.js")

    for contract in (
        "audiohoard.importReview.volume",
        "window.localStorage.getItem",
        "window.localStorage.setItem",
        "querySelectorAll('.review-audio')",
        "audiohoard:page-init",
        "data-review-volume-bound",
    ):
        assert contract in source

    assert "return 1;" in source
    assert "Math.min(1, Math.max(0, parsed))" in source


def test_player_layout_respects_touch_safe_areas_navigation_and_reduced_motion() -> None:
    css = _read(STATIC / "css" / "components.css") + _read(STATIC / "css" / "base.css")

    assert ".global-player" in css
    assert "env(safe-area-inset-bottom)" in css
    assert "var(--mobile-nav-height)" in css
    assert "min-height: 44px" in css
    assert "prefers-reduced-motion: reduce" in css
    assert ".mobile-nav {" in css
    assert "display: flex" in css
    assert "flex: 1 1 0" in css
    assert "grid-template-columns: repeat(5, 1fr)" not in css
    assert ".player-visible" in css
