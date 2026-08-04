# Import Review Deck Implementation Plan

> **For Hermes:** Use Claude Code task-by-task; Jarvis owns review, verification, merge, tagging, and artifact proof.

**Goal:** Replace the Downloads import-review rail with an authenticated `/review` one-card deck comparing staged audio/tags against catalog truth and provider reference previews.

**Architecture:** Add iTunes preview parsing and a fail-open Deezer-first resolver. Project pending review rows at request time using existing relationships, path validation, tag reading, model properties, staged audio, and approve/deny routes. Render the oldest item with CSP-safe static JS and token-based CSS.

**Tech Stack:** FastAPI, SQLAlchemy, Jinja2, existing Mutagen metadata service, vanilla JS/CSS, pytest, Ruff, mypy, uv, GitHub Actions.

## Global Constraints

- MINOR release from v0.17.3; target v0.18.0.
- Do not delegate to L or Light. No schema or migrations. No inline `style=` or `on*` handlers.
- Reuse `read_audio_file_metadata`, `_validate_audio_path`, staged audio serving, and approve/deny routes.
- Acquisition source is existing `item.source_label` and appears only in details. Reference source is `deezer`/`itunes` from the resolver and appears only on the reference player badge. Never wire that badge to `source_label`.
- Reuse `item.source_label` and `item.original_filename` without recomputing them.
- CSRF forms work without JS. Downloads becomes full-width with only a nonzero `/review` queue link.
- Push the feature branch regardless. Tag/publish only if green modulo the accepted pre-existing `test_library_import` failure. Never push Docker locally.

### Task 1: Reference preview resolution

Modify `app/metadata/itunes.py`; create `app/services/reference_audio.py`; add `tests/unit/test_itunes.py`. Add `preview_url` to the common iTunes track projection. Return `{url, source}` with Deezer preferred and iTunes fallback, preferring stored previews over live lookup and failing open. Write/run the requested parsing test red, implement, run green.

### Task 2: Review view-model

Modify `app/services/staging.py`; create `tests/unit/test_staging_review.py`. Expose `as_tagged`, `should_be`, per-field normalized mismatch flags, reference, existing source/filename properties, and verification data. Validate before reading tags; a read error yields all-None tags. Write/run requested tests red, implement, run green.

### Task 3: `/review` route and deck

Create/modify a router and registration, `app/templates/review.html`, `app/templates/base.html`, `app/static/css/pages.css`, `app/static/js/review-deck.js`, and `tests/integration/test_review.py`. Render oldest pending item; include staged/reference players, reference-only provider badge, midpoint control, tag table, mismatch classes, verification/details with acquisition source and original filename, CSRF forms, and `All caught up.`. Implement Right/Left, Space/R, focus guard, independent playback, and double-submit prevention. Add/run the three requested integration tests.

### Task 4: Retire Downloads rail

Modify `app/templates/downloads.html`, reduce/remove `app/templates/partials/_import_review.html`, update CSS and `tests/integration/test_downloads_ui.py`. Make queue full-width and show `N tracks awaiting review →` only when nonzero. Confirm the deck retains Source and Original filename first.

### Task 5: Review, verify, and release

Run the exact requested focused tests, Ruff lint, Ruff format check, mypy, and full pytest. Inspect the full diff and run an independent read-only review not using L/Light. Add changelog notes and push `feature/import-review-deck` regardless. If green, move notes to `0.18.0` dated 2026-08-04, add fresh Unreleased, set `0.18.0` in `pyproject.toml` and `docker/Dockerfile`, commit `release: v0.18.0`, merge to clean `origin/main` without squash, push main, annotate/push `v0.18.0`, monitor CI/release, and verify Docker Hub artifacts.
