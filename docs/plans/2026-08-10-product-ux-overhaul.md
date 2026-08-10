# Audiohoard product UX overhaul — implementation map

**Date:** 2026-08-10  
**Status:** Phases 1–5 implemented; Phase 6 pending.

This map anchors the brief to the current server-rendered application. All slices preserve authenticated mutations, CSRF, environment locks, root-contained filesystem operations, transactional imports, and persistent job recovery.

## Phase 1 — Import-review correctness (**implemented**)

- `app/services/reference_audio.py`: typed `ReferenceAudio`; exact Deezer provider identity only; no iTunes, fuzzy search, unproven `Track.deezer_id`, or legacy URL-only cache.
- `app/services/catalog_metadata.py`: exact album/disc/position cache provenance and ambiguity rejection.
- `app/templates/review.html`: exact-match provenance, cached state, remaining queue count, and explicit no-reference guidance.
- `app/routers/staging.py`: denial uses transactional same-directory quarantine, rollback restoration, and deletion only after commit.
- Coverage: `tests/unit/test_reference_audio.py`, `tests/integration/test_review.py`, and denial cases in `tests/integration/test_staging_review.py`.

## Phase 2 — Navigation and Activity (**implemented**)

- `app/services/activity.py`: one aggregate round trip for Wanted, active downloads, failed/partial acquisitions, pending review, and rejected sources.
- `app/routers/activity.py`, `app/templates/activity.html`, and `_activity_tabs.html`: `/activity` overview and shared workflow tabs.
- `app/templates/base.html`: desktop Home/Discover/Library/Activity/Settings and mobile Home/Discover/Library/Activity; only actionable acquisition/review failures contribute to the badge.
- Preserved routes: `/wanted`, `/downloads`, `/review`, `/blocklist`, and `/search`.
- Coverage: `tests/unit/test_activity.py` and `tests/integration/test_activity_hub.py`.

## Phase 3 — Settings (**implemented**)

- `app/routers/settings.py`: cached overview context, actionable warning links, bounded Save and test, safe effective-root path diagnostics, legacy section compatibility, and existing environment-lock/secret behavior.
- `app/templates/settings/`: overview plus Acquisition, Metadata & discovery, Library & naming, Automation, Quality & verification, and Advanced & system sections with reusable fields, status, connection-card, and save-bar partials.
- `app/static/js/settings.js`: progressive unsaved-change and inline Save-and-test feedback with native form fallback.
- Packaging includes nested settings templates.
- Coverage: `tests/integration/test_settings.py`, `tests/security/test_settings_security.py`, settings service/runtime unit tests, and packaging tests.

## Phase 4 — Search and Discover (**implemented**)

- `app/routers/search.py` and `app/templates/search.html`: Discover naming, supported artist catalog search, monitored-artist context, contextual Manual search fields, and visible enabled-source selection.
- `app/services/manual_search.py`: documented deterministic evidence scoring and grouping only by stable artifact namespace/ID.
- `app/schemas/search.py`: bounded duration, preferred-format, expected-count, and catalog-ID context fields.
- Release, track, Wanted, and failed-download views link into prefilled Manual search while preserving legacy `/search` and existing POST contracts.
- Coverage: `tests/unit/test_manual_search_scoring.py` and `tests/integration/test_search.py`.

## Phase 5 — Wanted and Rejected Sources (**implemented**)

- Wanted now exposes durable acquisition state, latest attempt/failure/provider context, review/rejected-source links, and supported server-side state filters without per-row queries.
- The bulk control is truthfully bounded and labeled **Queue this page**; existing queue/authentication/CSRF contracts remain intact.
- `/blocklist` remains compatible while the UI is named Rejected Sources and adds pagination, exact artifact context, filters, allow, and allow-plus-retry actions.
- Migration `0030` adds reversible `blocked_until`, `retry_count`, and `last_failure_at` fields. Known transient timeouts are backfilled into cooldowns; explicit denial and identity failures remain permanent.
- Active Activity counts and acquisition exclusion logic ignore expired temporary rejections.

## Phase 6 — Supporting cleanup and acceptance (**pending**)

Current anchors: `pyproject.toml`, README, Docker/release metadata, changelog, Alembic, project validation scripts, and browser-test configuration.

Reconcile version sources, finish documentation/accessibility/security review, add deterministic browser smoke coverage, verify migration upgrade/downgrade, run the full validation suite, review the final diff, and publish only after independent approval.
