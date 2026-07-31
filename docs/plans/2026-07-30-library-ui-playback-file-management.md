# Library UI, Playback, and File Management Implementation Plan

> **Pipeline:** Light implementation -> L specification/quality/security review -> Light remediation -> Jarvis verification/publication.

**Goal:** Deliver truthful release ownership, uninterrupted authenticated playback, a visual artist/release UI, and safe immediate file removal/reconciliation.

**Architecture:** Persist imported-file presence; add secure shared range/transcode services and journaled deletion; keep the global player outside a History API content-swap region.

**Stack:** FastAPI, async SQLAlchemy/SQLite, Alembic, Jinja, vanilla JS/CSS, watchfiles, ffmpeg, pytest.

## Global constraints

- Work only on `feature/ui-overhaul` at `/mnt/user/appdata/dev/_scratch/audiohoard-ui-overhaul`, based on origin/main v0.9.3.
- Never use production DB/library data or mutate/restart the live container.
- Permanent deletion is confirmation-gated, root-contained, no-symlink, rollback/crash safe, and never affects unrelated files.
- Any authenticated user may play/remove; existing CSRF protects mutations; no admin role.
- Keep catalog metadata and make absent tracks reacquirable.
- Playback remains uninterrupted across internal navigation.
- Page reads stay database-backed and bounded.
- Follow `CLAUDE.md`, `docs/VERSIONING.md`, TDD, Conventional Commits, and Unreleased changelog. No stable tag/`latest` without separate approval.

## File map

- State/schema: `app/models/import_plan.py`, new deletion-operation model, Alembic migration, migration/schema tests.
- Filesystem: new `app/services/library_files.py`, new watcher service, `app/main.py`.
- Media: new `app/services/media_streaming.py`, new library media router, staging helper reuse.
- Projection/routes: `app/services/catalog.py`, `app/routers/catalog.py`.
- Player/navigation: `app/templates/base.html`, new `app/static/js/player.js` and `navigation.js`, lifecycle-safe page scripts.
- UI: artist/release templates plus component/page CSS and integration tests.
- Packaging: `pyproject.toml`, `uv.lock`, `CHANGELOG.md`.

## Task 1 — Persist library-file state

**Produce:** `LibraryFileState` (`unknown/present/missing/removed`), check/removal metadata on import plans, and a durable grouped deletion-operation journal.

**RED:** migration/default/schema parity; legacy imported rows become unknown; journal FK/index/state contracts. Upgrade only a disposable DB.

**GREEN:** implement model/migration and focused tests. Do not claim legacy paths present without filesystem verification.

**Commit:** `feat(library): persist imported file state`

## Task 2 — Secure media path and range service

**Produce:** library-root-contained path resolver and strict single-range response helper; reuse from staging where behavior remains compatible.

**RED:** relative/traversal/outside-root/symlink/nonregular/unsupported/empty rejection; valid full/prefix/open/suffix range; invalid/multi-range 416; HEAD; cancellation and descriptor closure.

**GREEN:** implement generic errors and correct MIME/range headers.

**Commit:** `feat(media): add secure range streaming primitives`

## Task 3 — Library streaming and cached transcoding

**Produce:** `GET|HEAD /library/tracks/{id}/audio`, optional `transcode=mp3`, stat-keyed cache, per-key lock, global semaphore, bounded LRU cleanup/invalidation.

**RED:** auth; nonpresent plan; direct range/HEAD; one ffmpeg call for concurrent same-key requests; concurrency bound; timeout/failure/cancel cleanup; source-change invalidation; cache byte/count bounds.

**GREEN:** generalize existing staging MP3-preview pattern. Use no-stdin ffmpeg, timeout/output bounds, atomic cache publish, sanitized errors.

**Commit:** `feat(media): stream imported library tracks`

## Task 4 — Single and bulk permanent removal

**Produce:** `POST /library/tracks/{id}/remove` and `POST /albums/{id}/remove-files`, journaled service, cache invalidation, catalog recomputation, JSON enhanced response plus 303 fallback.

**RED:** auth/CSRF; all paths validated before bulk mutation; symlink/outside-root rejection; DB failure restores all renamed files; postcommit unlink failure remains retryable; idempotence; unrelated files untouched; immediate count/progress change; catalog preserved/reacquirable.

**GREEN:** prepared journal -> same-directory rename/fsync -> DB state/ownership commit -> unlink/finalize, with bounded DB retry and locks.

**Commit:** `feat(library): add safe permanent file removal`

## Task 5 — Recovery, watcher, and bounded reconciliation

**Produce:** startup journal recovery, `watchfiles` lifecycle service, bounded startup/periodic reconciliation with durable progress.

**RED:** prepared/moved/committed crash states recover idempotently; invalid low-ID rows do not starve later valid rows across runs; batches commit durably; external delete/move-out becomes missing; bursts debounce; in-flight temp paths ignored; duplicate events coalesce; cancellation propagates; watcher errors do not kill app; sweep repairs missed event.

**GREEN:** add dependency/lockfile, start after effective settings and stop in lifespan finally; keep transactions short.

**Commit:** `feat(library): reconcile external file removals`

## Task 6 — Truthful release ownership

**Produce:** artist complete/partial/local/total projections and per-track present/missing state.

**RED:** complete, partial, zero-present, duplicate attempts, same-title distinct IDs, unknown denominator, removed/missing exclusion, deterministic pagination, no unbounded page-time scan.

**GREEN:** bounded grouped SQL from catalog manifests and latest present plans.

**Commit:** `feat(catalog): report truthful release ownership`

## Task 7 — Global player and uninterrupted shell

**Produce:** player outside main, play/queue API, lifecycle events, progressive same-origin content swapping, back/forward/title/nav/focus/live-region handling and hard fallback.

**RED/browser contracts:** navigation does not replace audio; currentTime advances across library -> artist -> album -> back; superseded fetch aborts; login/malformed response hard-navigates; modifier/new-tab/external/download/media links are untouched; initializer runs once; queue skips missing; media error retries transcode.

**GREEN:** idempotent delegated listeners and optional Media Session integration without weakening CSP or native fallback.

**Commit:** `feat(ui): add persistent library player`

## Task 8 — Artist and release UI overhaul

**Produce:** artwork-first artist cards; unified catalog/legacy artist system; cover-led album/single/EP hero; compact action toolbar; autosave switch; grouped maintenance; direct track play/details/remove; bulk remove.

**RED/templates:** exact complete/total plus separate partial copy; honest unknown copy; no monitoring Save button; pending/saved/error feedback; confirmations; missing reacquire action; escaped metadata; legacy parity; 360px semantics.

**GREEN:** responsive CSS and lifecycle-safe enhancement with native POST fallback.

**Commit:** `feat(ui): overhaul artist and release pages`

## Task 9 — Review and quality gates

Update `CHANGELOG.md` under Unreleased. Run:

- `uv run pytest`
- `uv run ruff check .`
- `uv run ruff format --check .`
- `uv run mypy app`
- `uv build`
- `git diff --check`

L reviews a fixed commit range for spec compliance, then security/data-loss/races/accessibility. Critical/important findings block. Light remediates one consolidated set; L re-reviews fixes plus open findings. Jarvis inspects the exact diff and reruns gates.

## Task 10 — Disposable runtime/browser acceptance

Build an exact local review image. Run a distinct disposable container with fresh writable app data and fixture-only library/staging mounts. Seed direct-play, transcode-required, complete, partial, missing, and unknown-denominator cases.

Verify auth/range/transcode/seek, one cached transcode, uninterrupted navigation/back/forward, console/network, desktop/360px overflow/focus/player clearance, single/bulk removal and immediate state, preserved reacquisition, external watcher update, restart reconciliation, and crash-journal recovery. Remove only session-created QA artifacts after reference checks.

## Task 11 — Branch and acceptance artifact

Re-read `docs/VERSIONING.md`. Push the feature branch, open a PR, and wait for CI. Merge only after approval. Publish `noplexzone/audiohoard:develop` through the repository CI/release mechanism—never an untracked local push. Verify the remote commit and Docker manifest digest. Stable semver and `latest` require separate approval.

Final handoff must include: `Pull noplexzone/audiohoard:develop (sha256:...)` and label it acceptance-ready, not release-ready.
