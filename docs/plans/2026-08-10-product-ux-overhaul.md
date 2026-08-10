# Audiohoard product UX overhaul — implementation map

**Date:** 2026-08-10  
**Status:** Phases 1–2 implemented; Phases 3–6 are planning only.

This map anchors each phase to the current server-rendered application. It is not a claim that later-phase behavior has been built, and later phases must preserve authenticated mutations, CSRF protection, root-contained filesystem operations, transactional imports, and the existing provider/job recovery guarantees.

## Phase 1 — Trustworthy import review (**implemented in this slice**)

- `app/services/reference_audio.py` returns a typed `ReferenceAudio` only for an exact Deezer track ID or an exact Deezer album/disc/position. It rejects fuzzy search, iTunes fallback, legacy URL-only cache values, mismatched identities, ambiguous positions, and references that expire too soon; expired exact references are refreshed through the same exact identity.
- `app/services/catalog_metadata.py` persists Deezer preview URL, provider track/album IDs, match method, disc, and position as cache provenance and does not cache ambiguous positions.
- `app/templates/review.html` and `tests/integration/test_review.py` expose the exact-match method, cached state, remaining-item count, and a useful no-reference state without blocking manual review.
- `app/routers/staging.py` already performs denial as transactional quarantine: the staged file is moved aside before commit, restored on failure, and deleted only after commit succeeds; bounded reacquisition remains intact.
- Primary coverage: `tests/unit/test_reference_audio.py`, `tests/integration/test_review.py`, and the denial/quarantine cases in `tests/integration/test_staging_review.py`.

## Phase 2 — Navigation and Activity (**implemented**)

- `app/services/activity.py` provides one aggregate database round trip for Wanted releases, active downloads, failed/partial acquisitions, pending review decisions, and rejected sources.
- `app/routers/activity.py`, `app/templates/activity.html`, and `app/templates/partials/_activity_tabs.html` add `/activity` and shared secondary navigation while preserving `/wanted`, `/downloads`, `/review`, `/blocklist`, and `/search`.
- `app/templates/base.html` now exposes Home, Discover, Library, Activity, and Settings on desktop and Home, Discover, Library, and Activity on mobile. Only failed/partial acquisitions and pending review decisions contribute to the attention badge.
- Coverage: `tests/unit/test_activity.py` and `tests/integration/test_activity_hub.py`.

## Phase 3 — Library browsing and playback (**not implemented**)

Current-code anchors: `app/templates/index.html`, `app/templates/library_tracks.html`, `app/templates/track.html`, `app/static/js/player.js`, `app/routers/tracks.py`, and the library query services.

Plan artwork-first browsing, clear availability/quality state, predictable filtering, and continuous local playback without changing file-authorization or library-root safety contracts. No Phase 3 production changes are included in this slice.

## Phase 4 — Artist and release workflows (**not implemented**)

Current-code anchors: `app/templates/artists.html`, `app/templates/artist_detail.html`, `app/templates/catalog_artist.html`, `app/templates/catalog_album.html`, `app/templates/partials/_artist_card.html`, `app/templates/partials/_release_card.html`, `app/static/js/album.js`, and `app/routers/catalog.py`.

Plan consistent artist/release hierarchy, edition-aware monitoring, progress, acquisition, and maintenance actions while retaining provider-scoped identities. No Phase 4 production changes are included in this slice.

## Phase 5 — Search, discovery, and wanted flow (**not implemented**)

Current-code anchors: `app/templates/search.html`, `app/templates/discover_list.html`, `app/templates/wanted.html`, `app/static/js/discovery.js`, `app/static/js/wanted.js`, `app/static/js/artist-watchlist.js`, `app/routers/search.py`, and `app/routers/catalog.py`.

Plan continuity from discovery/search identity to watchlist, wanted, and acquisition actions, with explicit provider identity and useful empty/error states. No Phase 5 production changes are included in this slice.

## Phase 6 — Settings, operations, and acceptance gates (**not implemented**)

Current-code anchors: `app/templates/settings.html`, `app/templates/maintenance.html`, `app/templates/blocklist.html`, `app/static/js/settings.js`, `app/routers/settings.py`, `app/routers/maintenance.py`, `app/routers/blocklist.py`, shared navigation/styles, and the integration suite.

Plan task-oriented settings and diagnostics, confirmation for destructive actions, responsive/accessibility review, and end-to-end regression/performance gates. No Phase 6 production changes are included in this slice.
