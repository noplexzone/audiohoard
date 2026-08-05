# Mobile Review and PWA Shell Correction Plan

> **For Hermes:** Execute directly with TDD, then freeze the final commit for independent review.

**Goal:** Make AudioHoard import review usable in an installed iPhone PWA by fixing swipe decisions, removing unused controls, fitting navigation into one row, hiding the empty global player, and putting the complete decision surface above collapsed secondary details.

**Architecture:** Preserve existing authenticated routes, forms, CSRF, confirmation, and native audio elements. Add an iOS Touch Events gesture path alongside pen Pointer Events, make feedback truthful and visible, change the shell to reveal the global player only after a library track is selected, and turn long tag/provenance sections into collapsed secondary details below immediate decision controls.

**Tech Stack:** FastAPI/Jinja, static JavaScript, CSS, pytest, isolated authenticated browser QA.

## Global Constraints

- Repository: `/mnt/user/appdata/dev/audiohoard`.
- Worktree: `/mnt/user/appdata/dev/_scratch/audiohoard-mobile-review-pwa`.
- Production baseline: `v0.19.0`, commit `45f76db469b77e1ad743df14b55213c4d0078758`.
- Patch target: `v0.19.1` after acceptance verification.
- Do not change database schema or migrations.
- Do not change approve/deny backend actions, CSRF, or denial confirmation.
- Swipe right approves; swipe left opens denial confirmation.
- Vertical scrolling and native audio controls must remain usable.
- Remove “Jump downloaded file to midpoint” completely.
- Global Now Playing is hidden until a library track is selected.
- Mobile navigation remains one row and every rendered destination remains at least 44 CSS pixels wide at 393px.
- The primary review decision surface—identity, both audio players, approve/deny controls, swipe hint, and secondary-detail summary—must fit in a 393×852 viewport without scrolling.
- Production may be inspected read-only but must not be restarted, recreated, or otherwise modified without separate permission.

## Task 1: Shell and review markup contracts

**Files:**
- Modify: `tests/unit/test_player_navigation_contracts.py`
- Modify: `tests/integration/test_review.py`
- Modify: `app/templates/base.html`
- Modify: `app/templates/review.html`

**RED:** Add assertions that the global player starts hidden, the midpoint control is absent, action forms precede secondary detail, long tag/provenance content is inside one collapsed `<details>`, and the mobile nav does not hard-code fewer columns than rendered destinations.

**GREEN:** Update Jinja markup only. Keep form actions and confirmation text unchanged. Put decision actions immediately after audio comparison and move tag comparison plus verification into a collapsed “Tags & file details” disclosure.

**Proof:** Focused template/integration tests and HTML index assertions pass.

## Task 2: Reliable iOS swipe and truthful feedback

**Files:**
- Modify: `tests/unit/test_packaging_assets.py`
- Modify: `app/static/js/review-deck.js`
- Modify: `app/static/css/pages.css`

**RED:** Require touchstart/touchmove/touchend handling, vertical-intent cancellation, interactive-target exclusion, direction feedback visibility, and removal of midpoint behavior.

**GREEN:** Use Touch Events for touch screens and Pointer Events for pen only. Track one touch identifier, cancel on vertical intent, call `preventDefault()` only after horizontal intent, keep denial confirmation through the existing form, and reveal directional feedback with `visibility: visible`. Use fixed class-based movement to remain CSP-safe.

**Proof:** Node syntax check, focused tests, and browser event probes prove right approval, left confirmation, vertical cancellation, and ignored audio/button starts.

## Task 3: Compact 393×852 mobile shell

**Files:**
- Modify: `tests/unit/test_player_navigation_contracts.py`
- Modify: `tests/integration/test_review.py`
- Modify: `app/static/js/player.js`
- Modify: `app/static/css/base.css`
- Modify: `app/static/css/components.css`
- Modify: `app/static/css/pages.css`
- Modify: `CHANGELOG.md`

**RED:** Add contracts for one-row flex navigation, hidden idle player, dynamic player-spacing class, two-column decision buttons, compact mobile artwork/audio panels, and mobile-hidden page header/reference explanation.

**GREEN:** Render the player hidden and reveal it only after `playAt()` selects a real item; toggle a shell class to reserve player space only while visible. Convert nav to a one-row flex layout. At mobile width, reduce card chrome, clamp title copy, use 56px artwork, flatten audio panels, keep approve/deny side by side, and hide redundant prose.

**Proof:** At 393×852: no root horizontal overflow; every nav target ≥44px; nav is one row; idle player has zero rendered height; decision summary bottom is within viewport; fixed nav does not overlap reachable content.

## Task 4: Verification and publication

- Run focused tests, then full pytest, Ruff lint/format, mypy, Node syntax, package build, and CSP scan.
- Run isolated authenticated browser QA with representative review data and capture mobile screenshots/probes.
- Freeze commit and request independent review focused on destructive swipe safety and mobile layout.
- Push through protected-main PR workflow.
- Publish and verify `noplexzone/audiohoard:develop` and the policy-required patch release only after all gates pass.
- Do not deploy or restart production without separate permission.
