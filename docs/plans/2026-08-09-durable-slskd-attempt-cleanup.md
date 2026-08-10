# Durable slskd Attempt Ownership and Cleanup Plan

> Implement task-by-task with RED/GREEN tests. Work only in `/mnt/user/appdata/dev/_scratch/audiohoard-slskd-cleanup` on `fix/durable-slskd-attempt-cleanup` from `b04b92c`. Do not touch production databases, queues, settings, folders, containers, credentials, or running services.

## Goal and architecture

Prevent duplicate provider work for an already-owned track from the exact same catalog release, and persist every slskd candidate as an independent cleanup-owned attempt. Track fields remain a selected/current compatibility projection; normalized attempt rows become cleanup truth. Cleanup uses exact immutable provider UUIDs and content-bound file ownership. Legacy unmatched material is report-only until Caleb separately approves destructive cleanup.

## User-owned invariants

- Caleb permits duplicate songs across different releases. Suppression requires the exact non-null `(catalog_album_id, catalog_track_id)` and a physically present committed imported destination for that exact release/track.
- Never delete/overwrite library files. Retain review and ready-to-import artifacts.
- Provider deletion requires exact immutable slskd UUID; peer/path fallback is not safe. Ambiguous legacy rows remain unresolved.
- Commit durable terminal/artifact/import state before provider HTTP or filesystem mutation. No external I/O in SQLite write transactions.
- Retry/reassignment must honor cleanup claims and cannot let cleanup delete replacement work.
- Complete cleanup uses stable metadata plus SHA-256 and existing hardened quarantine semantics.
- Incomplete cleanup removes only exactly owned partials after exact transfer removal, then empty ancestors. The grace sweeper skips any live-transfer file/tree.
- Legacy cleanup has no apply/delete mode in this change. Add only a read-only report.
- Update `CHANGELOG.md` under Unreleased. Do not bump version, tag, push, publish, merge, or restart containers.

## Acceptance criteria

1. Exact release/track with a present imported destination closes/skips redundant work before search/enqueue; missing destination still acquires; a different release still acquires.
2. Concurrent equivalent runners perform provider work at most once.
3. Every slskd candidate has a durable attempt with job/track/catalog identity, peer/path, UUID once known, provider/artifact/outcome states, staged/partial paths, cleanup states/claims, sanitized errors, and timestamps. Candidate N+1 never overwrites N.
4. Terminal cleanup claims an attempt, deletes exact UUID, verifies absence via fresh provider read, and finalizes idempotently. Ambiguous/no-UUID attempts are neither deleted nor marked complete.
5. A marker-write failure after DELETE cannot delete replacement work on retry.
6. Imported or safe terminal superseded artifacts use exact content-bound cleanup; review/ready artifacts remain; empty complete ancestors are pruned.
7. Exact owned partials and old empty incomplete directories are pruned only after live-transfer cross-check and grace threshold.
8. Startup/periodic reconciliation consume attempt rows; unresolved legacy debt stays visible.
9. A container-native read-only command reports unmatched terminal transfers, unreferenced complete files, cleanup debt, and empty incomplete trees without mutation or credential leakage.
10. Migration/schema parity, focused/full pytest, Ruff lint/format, mypy, package build, migration roundtrip, and disposable container smoke pass.

## Task 1 — Persistence

Create `app/models/acquisition_attempt.py`, migration `0027_acquisition_attempts.py`, relationships/exports, schema parity, and model tests. Define explicit provider, artifact, outcome, provider-cleanup, and file-cleanup states; indexed exact catalog/provider identities; claim token/version; error/timestamps. Prove multiple attempts coexist for one Track and retry is idempotent.

## Task 2 — Exact same-release suppression

Add one shared ownership predicate requiring the same exact catalog IDs, an imported plan, a non-empty committed destination, containment, regular file, and present file state. Add a short SQLite claim/uniqueness fence at dispatch/run boundary. Tests: owned exact release makes zero search/enqueue calls; missing file acquires; different release acquires; two concurrent runners submit once.

## Task 3 — Persist every slskd candidate

Modify `app/jobs/runner.py` and `app/sources/slskd.py`, preferably through `app/services/acquisition_attempts.py`. Create attempt before enqueue; checkpoint returned/discovered canonical UUID immediately; persist provider terminal state, artifact path/identity/hash, selected/rejected/superseded outcome, and sanitized errors. Track remains only current projection. Tests cover sequential candidates, retry adoption, restart by UUID, and secret-free errors.

## Task 4 — Exact provider cleanup

Centralize cleanup in `app/services/acquisition_cleanup.py` and lifecycle wiring. Claim/commit before HTTP; remove exact UUID; refresh bypassing/invalidation of cache; absence is idempotent success; re-read and finalize only the claim. Safe migration from Track obligations only when UUID is genuinely known. Tests cover ordering, crash after DELETE, 404, ambiguous fallback, concurrent replacement with same peer/path, and no DELETE of replacement UUID.

## Task 5 — Exact file and directory cleanup

Reuse existing content-bound quarantine and post-quarantine revalidation. Persist successful outcome separately from attempted timestamp. Prune empty complete ancestors after exact cleanup. Clean failed/superseded artifacts only when no review/recovery obligation remains. Add bounded incomplete-root sweeper with grace period and fresh live-transfer snapshot. File-backed tests cover review retention, replacement/hash mismatch, symlink/outside-root rejection, active transfer, non-empty ancestors, grace, cancellation, and restart.

## Task 6 — Report-only legacy reconciliation

Create `app/maintenance/slskd_cleanup_report.py`, console entry point, README docs, and tests. Use SQLite URI `mode=ro`; classify durable obligations, ambiguous/no-UUID attempts, unmatched terminal transfers, review/ready files, imported cleanup debt, unreferenced complete files, and empty incomplete trees. No delete/apply flag. Redact credentials and bound output.

## Task 7 — Gates

Update changelog/docs. Run focused tests, `uv run pytest`, `uv run ruff check .`, `uv run ruff format --check .`, `uv run mypy app`, `uv build`, fresh migration upgrade/downgrade/upgrade, Docker build/start/health, and report command on synthetic read-only state. Make coherent Conventional Commits only; Jarvis owns final review/push/CI/publication.
