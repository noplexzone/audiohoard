# Audiohoard — Project Constraints for Claude

## What this project is

A private, self-hosted FastAPI application that coordinates music acquisition, metadata enrichment, fingerprinting, reviewed library imports, and monitored artist catalogs.

## Hard constraints

### Sources
- Supported acquisition paths are slskd, Prowlarr with SABnzbd, YouTube through yt-dlp, and direct TIDAL track URLs through an operator-authenticated tidal-dl profile.
- Never stub, synthesize, or hide provider results. Adapters implement `SourceAdapter` and report truthful capability and failure states.
- External downloads do not complete until the client reaches a terminal success state and a usable staged artifact exists.

### Metadata and identity
- MusicBrainz is canonical for MBIDs. Tracks without a recording identity remain explicitly unresolved.
- Deezer and iTunes provide supplemental metadata and artwork. AcoustID lookup is optional.
- External API calls are bounded, rate-limited where required, and retried only when safe.

### Naming and file operations
- The default naming template is `{album_artist}/{year} - {album}/{disc_track} - {title}.{ext}`. One-disc releases use `TT`; multi-disc releases use `D-TT`.
- Library writes occur only through a persisted, reviewed import plan.
- Import execution must enforce staging/library containment, collision policy, metadata write/readback verification, atomic replacement, and filesystem rollback.
- The library mount must be writable when imports are enabled. Never delete or reorganize unrelated library files.

### Data and jobs
- Use SQLAlchemy 2.x async with SQLite via `aiosqlite`.
- All schema changes go through Alembic; never use `create_all()` in production paths.
- The SQLite-backed in-process dispatcher owns job execution, duplicate suppression, startup recovery, retry, and cancellation. No external broker is required.

### Health contracts
- `/health/live` is a cheap public process-liveness check.
- `/health/ready` checks database readiness and returns HTTP 503 when unavailable.
- Provider diagnostics use cached state and require authentication; public health requests must not trigger provider network probes or expose configuration details.

### Testing and security
- Write discriminating regression tests for behavior changes. Unit tests mock external HTTP; disposable integration instances may use real services.
- No secrets in source control. Environment and database-backed secrets remain masked in UI/API responses.
- Validate naming tokens, paths, selected-result payloads, and redirects. Prevent traversal, open redirects, CSRF, and SQL interpolation.

### Docker
- The application and dependencies run under Docker Compose.
- The image includes `fpcalc`; absence at runtime degrades fingerprinting without blocking acquisition.
- Persist application data and TIDAL profiles. Mount staging and library paths according to configured import behavior.

## Style
- Python 3.12+. Use `from __future__ import annotations`.
- Prefer async I/O and move unavoidable filesystem probes to a worker thread.
- Type-annotate public functions. Use Ruff, Ruff format, mypy, and pytest.
- The web UI is server-rendered Jinja; small progressive-enhancement scripts are acceptable when native behavior remains usable.
- No commented-out code or unresolved TODOs on release branches.
