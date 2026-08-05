# Review Provenance and Mobile Swipe Implementation Plan

> **For Hermes:** Implement directly task-by-task using TDD; freeze the final commit for independent review.

**Goal:** Show slskd peer/folder provenance in import review and let mobile reviewers swipe right to approve or left to deny with the existing deletion confirmation.

**Architecture:** Derive display-only peer and remote-folder fields from existing `Track.acquisition_provenance_json`; do not add schema. Add pointer-event gesture handling to the existing review card and submit existing CSRF-protected forms, preserving confirmation and keyboard controls.

**Tech Stack:** FastAPI/Jinja, SQLAlchemy model properties, vanilla JavaScript Pointer Events, CSS, pytest/httpx.

## Global Constraints

- Swipe right approves.
- Swipe left opens the existing deny/delete confirmation.
- Do not intercept vertical scrolling or gestures beginning on interactive controls.
- Keep keyboard and button behavior unchanged.
- Display provider, slskd username, remote folder, and original filename.
- Use persisted provenance only; missing historical fields render as `—`.
- No database schema or migration.
- No inline style or event handlers; preserve CSP.
- Stable release would be `0.19.0`; tagging and production restart are separate approval gates.

---

### Task 1: Project slskd provenance safely

**Objective:** Expose provider username and remote folder from existing acquisition provenance.

**Files:**
- Modify: `app/models/staging_review.py`
- Modify: `app/services/staging.py`
- Modify: `app/templates/review.html`
- Test: `tests/unit/test_staging_review_model.py`
- Test: `tests/unit/test_staging_review.py`
- Test: `tests/integration/test_review.py`

**Interfaces:**
- Produces: `StagingReviewItem.source_username`, `StagingReviewItem.source_folder`; view keys `source_username`, `source_folder`.
- Folder parsing accepts `/` and `\\`, strips filename, and never substitutes a local staging path for remote provenance.

**Steps:**
1. Add failing property and rendered-output tests for `allbren` and `downloads\\LISA\\Alter Ego`.
2. Run focused tests and confirm expected failures.
3. Implement minimal provenance parsing and template fields.
4. Rerun focused tests and confirm pass.

### Task 2: Add touch/pointer swipe actions

**Objective:** Add deliberate horizontal gestures using existing approve/deny forms.

**Files:**
- Modify: `app/static/js/review-deck.js`
- Modify: `app/static/css/app.css`
- Modify: `app/templates/review.html`
- Test: `tests/integration/test_review.py`
- Test: `tests/unit/test_packaging_assets.py`

**Interfaces:**
- Consumes: existing `submit(button)` and form confirmation listener.
- Produces: pointer-down/move/up/cancel handling, horizontal-intent threshold, CSS state classes, and visible mobile guidance.

**Steps:**
1. Add failing rendered/static-contract tests for gesture hooks and guidance.
2. Confirm RED.
3. Implement pointer state: ignore interactive descendants; cancel for vertical intent; require horizontal threshold; right submits approve; left submits deny; reset on cancel.
4. Add CSS feedback without inline attributes; use classes and `element.style.setProperty` only for live displacement.
5. Confirm focused tests and manually verify mobile behavior and denial confirmation.

### Task 3: Verify and publish acceptance artifact

**Objective:** Produce a reviewed feature branch without touching production.

**Files:**
- Modify: `CHANGELOG.md` under `[Unreleased]`.

**Steps:**
1. Run focused tests, full pytest, Ruff lint, Ruff format, mypy, CSP scan, and package build.
2. Perform authenticated desktop/mobile browser QA: provenance, vertical scroll, controls, right approve, left deny confirmation/cancel.
3. Commit with Conventional Commits and freeze the range for independent review.
4. Resolve blocking findings and rerun gates.
5. Push branch and open a protected-main PR.
6. Do not tag or restart production without a separate release/deployment decision. If current CI cannot publish `develop` without a stable tag, report that constraint instead of mis-tagging.
