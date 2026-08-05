# Review Lock and Mobile Tag Layout Implementation Plan

> **For Hermes:** Execute directly with TDD because the transaction and UI fixes are a tightly coupled incident patch.

**Goal:** Prevent transient SQLite writer contention from turning an approved import review into HTTP 500, and make tag comparisons readable on mobile without horizontal scrolling inside the swipe surface.

**Architecture:** Harden `execute_release_import` so every failed flush is rolled back before ORM state is inspected or rewritten, then rebuild failure state from stable row IDs in a clean transaction. Replace the review tag table with semantic stacked comparison rows that fit the card width; desktop may use a three-column grid while mobile uses one-column field cards.

**Tech Stack:** FastAPI, SQLAlchemy async, SQLite WAL, Jinja2, CSS, pytest, Playwright.

## Global Constraints

- Preserve approval/deny semantics, authentication, CSRF, CSP, Turbo/navigation lifecycle, and destructive-action confirmation.
- Swipe right approves; swipe left invokes existing deny confirmation.
- No horizontal overflow or horizontal-scroll interaction inside the mobile swipe card.
- Do not repeat filesystem import work when recovering from a transient lock.
- No schema changes.
- Production container must not be restarted or changed during development/publication.
- Target patch version: `0.19.2`; publish `0.19.2` and `develop`, not `latest`.

---

### Task 1: Recover import execution failures in a clean transaction

**Objective:** Ensure a SQLite lock or any execution exception cannot leave the session pending rollback or produce a secondary `PendingRollbackError`.

**Files:**
- Modify: `app/services/library_import.py`
- Test: `tests/unit/test_library_import.py`

**Interfaces:**
- Consumes: `execute_release_import(db, release, ...)` and existing filesystem rollback lists.
- Produces: the same `ImportExecutionError`, but only after `await db.rollback()` and clean persisted failure-state writes using captured release/plan IDs.

**Steps:**
1. Add a test that injects `OperationalError('database is locked')` at the importing-state autoflush and asserts `ImportExecutionError`, a usable session, `release=rolled_back`, plan=`failed`, and no copied destination.
2. Run the exact test and require RED from the current `PendingRollbackError`.
3. Capture stable IDs before execution, rollback the SQLAlchemy transaction after filesystem rollback, reload rows, and write failure state in the clean transaction.
4. Run focused library-import and staging-review tests.

### Task 2: Replace horizontal tag table with responsive comparison rows

**Objective:** Let users compare tags without horizontal scrolling or gesture conflict.

**Files:**
- Modify: `app/templates/review.html`
- Modify: `app/static/css/pages.css`
- Test: `tests/integration/test_review.py`
- Test: `tests/unit/test_packaging_assets.py`

**Interfaces:**
- Consumes: existing `tag_fields`, `review.as_tagged`, `review.should_be`, and `review.diff` mappings.
- Produces: `.tag-comparison-list`, `.tag-comparison-row`, `.tag-comparison-value` markup with explicit “As tagged” and “Catalog” labels.

**Steps:**
1. Change contracts to reject `.table-wrap`/`.tag-diff-table` and require stacked comparison markup and mobile no-overflow CSS.
2. Run focused tests and require RED.
3. Implement semantic list/description markup and responsive CSS: three columns at desktop, one column at mobile, wrapping anywhere.
4. Run authenticated 393×852 and 320×568 browser checks with details expanded; assert no document/card/details overflow and that deliberate card swipes still work.

### Task 3: Patch release and final gates

**Objective:** Publish the exact tested correction as `v0.19.2` without touching production.

**Files:**
- Modify: `CHANGELOG.md`, `pyproject.toml`, `uv.lock`, `docker/Dockerfile`, version tests.

**Steps:**
1. Record both fixes under `0.19.2` and bump package/image/version contracts.
2. Run Ruff, format, mypy, JS syntax, focused tests, full suite, build, and final diff checks.
3. Commit/push, open PR, wait for CI, and obtain independent read-only review.
4. Merge through protected main, wait for main CI, tag `v0.19.2`, verify tag/release workflows, GitHub Release, Docker `0.19.2`/`develop` digest and labels, and isolated runtime smoke.
5. Report the exact pull line. Production remains unchanged.
