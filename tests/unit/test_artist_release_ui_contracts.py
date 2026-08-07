from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).parents[2]
TEMPLATES = ROOT / "app" / "templates"
STATIC = ROOT / "app" / "static"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def test_artist_cards_report_truthful_complete_partial_unknown_and_local_counts() -> None:
    template = _read(TEMPLATES / "artists.html")

    assert "artist.complete_release_count" in template
    assert "primary-source releases complete" in template
    assert "total unknown" in template
    assert "artist.downloaded_file_count" in template


def test_catalog_and_legacy_artist_pages_share_discography_semantics() -> None:
    catalog = _read(TEMPLATES / "catalog_artist.html") + _read(
        TEMPLATES / "partials" / "_discography.html"
    )
    legacy = _read(TEMPLATES / "artist_detail.html")

    for template in (catalog, legacy):
        assert "artist-view-hero" in template
        assert "discography-section" in template
    assert "Albums" in catalog
    assert "Singles &amp; EPs" in catalog
    assert "Compilations" in catalog
    assert "primary_metadata_provider" in catalog
    assert "Primary source" in catalog


def test_catalog_artist_watchlist_settings_are_collapsed_and_immediate() -> None:
    catalog = _read(TEMPLATES / "catalog_artist.html")
    source = _read(STATIC / "js" / "artist-watchlist.js")
    css = _read(STATIC / "css" / "pages.css")

    assert "artist-watchlist-summary" in catalog
    assert "data-open-watchlist" in catalog
    assert "data-watchlist-dialog" in catalog
    assert "data-auto-submit-form" in catalog
    assert "Catalog source" in catalog
    assert "Changes apply immediately" in catalog
    assert "Auto-watchlist release types" not in catalog
    assert "New release policy" not in catalog
    assert "Save watchlist" not in catalog
    assert "Albums only" not in catalog
    assert "Singles/EPs off" not in catalog
    assert "data-open-watchlist" in source
    assert "Saving…" in source
    assert "watchlist-dialog" in css


def test_discography_uses_native_family_scoped_edition_controls() -> None:
    template = _read(TEMPLATES / "partials" / "_discography.html")
    catalog = _read(TEMPLATES / "catalog_artist.html")
    css = _read(STATIC / "css" / "pages.css")

    assert '<details class="edition-chooser">' in template
    assert 'name="edition"' in template
    assert 'name="action" value="defaults"' in template
    assert "Use defaults" in template
    assert "Explicit preferred" in template
    assert "Unknown only when Explicit is unavailable" in template
    assert "album_monitored" not in template
    assert "album_monitored" not in catalog
    assert "release-families/{{ family.anchor.id }}" in template
    assert ".edition-chooser" in css
    chooser_css = css[css.index(".edition-chooser") : css.index(".library-section")]
    assert "overflow-x" not in chooser_css


def test_release_page_has_compact_actions_autosave_and_safe_file_removal() -> None:
    template = _read(TEMPLATES / "catalog_album.html")
    source = _read(STATIC / "js" / "album.js")

    for label in (
        "Download missing",
        "Repair metadata",
        "Clean quality duplicates",
        "Monitor for upgrades",
        "Remove all downloaded files",
    ):
        assert label in template
    assert "<noscript>" in template
    assert "Save upgrade monitoring" in template
    assert 'data-autosave="monitor-upgrades"' in template
    assert 'action="/library/albums/{{ album.id }}/delete"' in template
    assert 'action="/library/tracks/{{ library_track_id }}/delete"' in template
    assert 'name="confirmation" value="delete"' in template
    assert "data-save-status" in template
    assert "Saving…" in source
    assert "Saved" in source
    assert "Could not save" in source
    assert "AudiohoardNavigation.refresh" in source
    assert "innerHTML" not in source


def test_imported_only_release_exposes_confirmation_gated_bulk_removal() -> None:
    template = _read(TEMPLATES / "artist_detail.html")

    assert 'action="/library/releases/delete"' in template
    assert "data-remove-release" in template
    assert 'name="confirmation" value="delete"' in template
    assert 'name="release_id"' in template
    assert 'name="artist_name"' in template
    assert 'name="album_title"' in template
    assert "Remove whole release" in template


def test_release_track_rows_expose_playback_details_and_unavailable_states() -> None:
    template = _read(TEMPLATES / "catalog_album.html")

    assert "progress.library_file(track.id)" in template
    assert "library_file.file_format" in template
    assert "library_file.file_size_bytes" in template
    assert "library_file.source" in template
    assert 'disabled aria-disabled="true"' in template
    assert "Audio unavailable" in template


def test_artist_release_layout_is_touch_safe_and_never_hover_dependent() -> None:
    css = _read(STATIC / "css" / "pages.css")

    assert ".release-action-toolbar" in css
    assert ".release-track-row" in css
    assert "min-height: 44px" in css
    assert "@media (max-width: 430px)" in css
    assert "overflow-wrap: anywhere" in css
    assert ".album-download" not in css or "opacity: 0" not in css
