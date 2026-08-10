# Audiohoard product UX overhaul — implementation map

**Date:** 2026-08-10  
**Status:** Phases 1–3 implemented; Phases 4–6 pending.

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

## Phase 4 — Search and Discover (**pending**)

Current anchors: `app/routers/search.py`, `app/services/discovery.py`, `app/templates/search.html`, `app/templates/discover_list.html`, contextual release/track pages, and source adapters.

Implement grouped supported entities without fuzzy identity merging, personalized sections backed by existing data, contextual Manual search, visible source controls, deterministic candidate scoring, and high-confidence candidate grouping.

## Phase 5 — Wanted and Rejected Sources (**pending**)

Current anchors: Wanted query/POST paths in `app/routers/catalog.py`, `app/templates/wanted.html`, `app/routers/blocklist.py`, `app/models/source_candidate_block.py`, job/acquisition models, and Alembic migrations.

Implement server-side work-queue state/filters, truthful queue-all semantics, paginated contextual Rejected Sources, permanent/temporary classification and cooldown, restore/retry actions, and cross-links.

## Phase 6 — Supporting cleanup and acceptance (**pending**)

Current anchors: `pyproject.toml`, README, Docker/release metadata, changelog, Alembic, project validation scripts, and browser-test configuration.

Reconcile version sources, finish documentation/accessibility/security review, add deterministic browser smoke coverage, verify migration upgrade/downgrade, run the full validation suite, review the final diff, and publish only after independent approval.
