# Acquisition Throughput and Provider Ownership Implementation Plan

> **For Hermes:** Use Claude Code task-by-task; Jarvis verifies every diff, test, PR, and artifact.

**Goal:** Make large Audiohoard acquisition batches drain quickly without losing provider-transfer ownership, duplicating searches, or weakening track/release identity verification.

**Architecture:** Preserve the durable job/attempt model, but split local concurrency into an acquisition lease that can be yielded while slskd reports an external queue wait. Give durable slskd search its own adaptive timeout rather than deriving it from the download timeout. Reconcile terminal-job provider obligations through exact immutable attempt ownership, coalesce only identical in-flight searches while applying each consumer's own candidate validation, and prefer one coherent release-folder acquisition before per-track continuation.

**Tech Stack:** Python 3.12, asyncio, FastAPI, SQLAlchemy async/SQLite, httpx, pytest, Ruff, mypy, Docker.

## Global Constraints

- Work only in `/mnt/user/appdata/dev/_scratch/audiohoard-acquisition-throughput` on branch `perf/acquisition-throughput` based on `9f077f8b85c7e6e32875c4435038f2196a29846f`.
- Production remains read-only. Do not restart/update containers, edit `/mnt/user/appdata/audiohoard/audiohoard.db`, cancel live slskd transfers, or remove files.
- No cleanup may delete by peer/path or a provisional ID. Provider mutation requires exact canonical UUID plus persisted peer/path agreement and a fresh provider snapshot.
- Search/result sharing must never share an acceptance decision. Every waiting catalog target independently applies collaborator, remix/version, edition, duration, quality, denial-block, and catalog-identity guards.
- Verified tracks import immediately. Release completeness drives continuation/closure, not per-track import eligibility.
- Existing `max_parallel_acquisitions` remains runtime-configurable from 1–16. Lowering does not cancel active work; raising wakes waiters.
- No stable version, semver tag, GitHub Release, `latest`, or production deployment. The eventual acceptance artifact is only `noplexzone/audiohoard:develop` after merge and CI.
- Follow TDD: discriminating RED, minimal GREEN, focused/broad verification, coherent Conventional Commits.

## Acceptance Criteria

1. A terminal job with queued/enqueued/downloading slskd attempts is not treated as clean; the periodic reconciler adopts or cleans only exact, unambiguous UUID-bound obligations and otherwise retains visible blocked/retryable debt.
2. A valid in-process or durable provider wait cannot become `dispatch_lost` merely because no job-envelope write occurred during an allowed search/download wait.
3. Durable slskd search timing uses the configured search budget independently of `slskd_download_timeout_seconds`; adaptive polling may end early only after a provider terminal state or an acceptable high-confidence result path.
4. A job waiting in slskd's external queue yields its local acquisition permit and reacquires before active supervision, completion/import, non-cancellation failure persistence, or cleanup. Cancellation while yielded cannot deadlock.
5. Simultaneous identical normalized slskd searches issue one provider POST and share raw results/errors. Cache entries are in-flight/short-lived only, isolated by endpoint plus credential fingerprint, evict failures, and never share target acceptance decisions.
6. Catalog release acquisition searches/scorers evaluate coherent album folders before per-track fallback. Per-track continuation searches only unresolved catalog IDs and never re-enqueues verified tracks.
7. Existing tests plus new production-shaped concurrency, restart, provider-race, duplicate-query, and album-folder tests pass; Ruff check/format, mypy, package build, Docker build, and fresh-container smoke pass.

---

### Task 1: Durable provider ownership and watchdog closure

**Objective:** Prevent terminal job envelopes from abandoning live provider work and prevent legitimate long waits from being declared lost.

**Files:** `app/jobs/dispatcher.py`, `app/jobs/runner.py`, `app/services/acquisition_cleanup.py`, `app/sources/slskd.py` if required, corresponding dispatcher/cleanup/recovery tests.

**Interfaces:** Produce an explicit durable job heartbeat/checkpoint operation safe outside provider I/O. Reuse centralized exact-attempt provider cleanup; do not add a peer/path deletion path.

**Steps:**
1. Add failing tests for a long live provider wait, a recovered task without an in-memory task handle, terminal jobs with unresolved attempts, ambiguous/replacement UUIDs, and exact idempotent cleanup.
2. Prove RED against `9f077f8`.
3. Implement durable heartbeat/ownership rechecks and terminal-obligation reconciliation using short transactions and provider I/O outside them.
4. Run focused dispatcher/runner/cleanup/recovery tests; commit `fix(acquisition): preserve provider ownership across long waits`.

### Task 2: Search budget separation and external-queue permit yielding

**Objective:** Prevent ten-minute search waits from consuming the complete acquisition concurrency limit and allow later work to start while slskd transfers are externally queued.

**Files:** `app/jobs/dispatcher.py`, `app/jobs/runner.py`, `app/sources/slskd.py`, settings/config if needed, and focused dispatcher/slskd/runner/settings tests.

**Interfaces:** Produce a task-local resizable acquisition lease with yield/reacquire semantics. `_slskd_search_timeout_seconds(runtime)` must not read download timeout as its ordinary search budget.

**Steps:**
1. Add RED tests for configured 30/90/900-second search budgets, queued→later-job-start, queued→active reacquisition, queued→error reacquisition, cancellation while yielded, and runtime resizing.
2. Implement the smallest task-local lease and state callbacks at the existing slskd poll seam.
3. Keep provider polling alive while yielded; reacquire before stateful post-poll work.
4. Run focused tests; commit `perf(acquisition): yield local slots during provider queue waits`.

### Task 3: Exact in-flight search coalescing

**Objective:** Eliminate duplicate provider POST/poll sequences without weakening consumer-specific identity checks.

**Files:** `app/sources/slskd.py`, runner only if required, `tests/unit/test_slskd.py`, relevant identity tests.

**Interfaces:** Produce a bounded in-flight/small-TTL search snapshot keyed by normalized endpoint, credential fingerprint, normalized exact query, and provider mode/limits. Return raw results/errors; scoring remains caller-local.

**Steps:**
1. Add RED barrier tests proving concurrent identical searches make one POST, isolation by credentials/limits, failure eviction, cancellation shielding, and caller-local guards.
2. Implement shielded single-flight search execution with bounded TTL/eviction.
3. Run focused suites; commit `perf(slskd): coalesce identical in-flight searches`.

### Task 4: Release-folder-first acquisition and per-track continuation

**Objective:** Search and score one coherent release folder before creating individual missing-track searches.

**Files:** catalog/batch expansion service selected after inspection, existing slskd album scoring service, `app/services/discography_batch_runner.py` only at established interfaces, batch/slskd/integration tests.

**Interfaces:** Produce selected folder provenance, matched catalog-track IDs, unresolved IDs, and created/observed job IDs. Existing per-track jobs remain fallback envelopes.

**Steps:**
1. Add a production-shaped RED fixture with a complete album folder, sidecars, alternate formats, duplicate editions, and continuation.
2. Implement grouping/scoring before row caps and dispatch a coherent folder once.
3. Verify exact catalog identities and unresolved-only continuation.
4. Commit `perf(acquisition): acquire coherent release folders before track fallback`.

### Task 5: Integration, reporting, and acceptance artifact

**Objective:** Prove the complete branch, expose truthful unresolved transfer debt, and publish an acceptance image without touching production.

**Steps:**
1. Update `CHANGELOG.md` under `Unreleased` and add diagnostics only if needed.
2. Run focused suites, full non-browser and browser tests, Ruff check/format, mypy, package build, `git diff --check`, Docker build, and a fresh writable-database smoke test.
3. L reviews the fixed commit range for specification compliance, cleanup safety, concurrency races, and regressions; Light remediates and L re-reviews.
4. Jarvis inspects the diff and reruns release gates.
5. Push branch, open PR, wait for CI, merge only after approval and green checks, then verify the main-branch workflow publishes `noplexzone/audiohoard:develop`.
6. Verify Docker Hub index/amd64 digest and report the literal pull line. Do not deploy/restart production without Caleb's separate approval.
