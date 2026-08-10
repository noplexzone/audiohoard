# Audiohoard

**Private, self-hosted music acquisition and library management — v0.7.1**

A FastAPI application that coordinates multiple acquisition sources, enriches tracks with metadata, fingerprints audio, and enforces strict library naming conventions. Application state remains self-hosted; configured metadata and acquisition providers receive the requests required to perform their work.

## Acquisition Sources

| Source | Protocol | Notes |
|---|---|---|
| slskd | Soulseek P2P | Primary peer-to-peer source |
| Prowlarr + SABnzbd | Usenet NZB | Indexer-managed newsgroup downloads |
| YouTube | HTTP stream | yt-dlp extraction |
| TIDAL | tidal-dl | Direct HTTPS track URLs; authenticated local profile required |

### TIDAL setup

Audiohoard uses `tidal-dl` for direct TIDAL **track** URLs. It does not simulate catalog search and does not accept album or playlist URLs.

1. Build the image and create the persistent profile directory:
   ```bash
   docker compose build
   mkdir -p data/tidal
   ```
2. Perform the one-time interactive tidal-dl authentication outside the web app:
   ```bash
   docker compose run --rm --entrypoint tidal-dl -e HOME=/app/data/tidal app
   ```
3. In Settings (or `.env`), configure `/app/data/tidal/.tidal-dl.json` and `/app/data/tidal/.tidal-dl.token.json`. Both files must use those exact names, share the same directory, and remain on the persistent data volume.
4. Choose `Normal`, `High`, `HiFi`, or `Master`, then use a direct URL such as `https://tidal.com/browse/track/123456` when creating a TIDAL job.

The web app never starts an interactive login. Missing or expired authentication is reported as unavailable/failure instead of hanging a worker. Each acquisition uses a private copy of the profile so tidal-dl cannot persist transient output paths or mutate the operator's credential files.

## Metadata & Fingerprinting

- **MusicBrainz** — canonical track/release identity (MBIDs)
- **Deezer** — supplementary metadata (BPM, gain, preview)
- **AcoustID** — acoustic fingerprinting via `fpcalc` (optional; degrades gracefully when binary absent)
- **AcoustID Lookup** — matches fingerprint against the AcoustID database when a key is configured

## Navigation and activity

Audiohoard follows **Discover → Monitor → Acquire → Verify → Library**. The primary navigation is Home, Discover, Library, Activity, and Settings; mobile keeps Settings in the header. Activity unifies Wanted, Downloads, Review, and Rejected Sources without breaking their existing URLs, and its badge counts only failed/partial acquisitions and review decisions that need attention.

## Automated acquisition and import

Album searches group slskd results by peer, folder, and format before selecting a coherent release according to the quality profile in Settings. Incomplete albums automatically schedule bounded, track-specific continuation searches without redownloading catalog tracks already acquired.

Downloaded files are fingerprinted and compared with the expected MusicBrainz recording through AcoustID. Complete releases import transactionally without a manual Import step only when every catalog track is verified or explicitly approved. Mismatches and unavailable/ambiguous fingerprints remain staged under **Downloads → Pending review**, where authenticated users can listen and approve or deny them. Import review offers comparison audio only when an exact Deezer track ID or exact Deezer album/disc/position proves the reference identity; fuzzy title/artist matches, iTunes previews, and legacy URL-only cache entries are not used as verification evidence. Denial schedules bounded reacquisition and moves the staged file into a same-directory quarantine before the database transaction commits; the file is restored if the transaction fails and deleted only after a successful commit.

## Naming Convention

Files are renamed according to a strict, configurable template:

```
<AlbumArtist>/<Year> - <Album>/<DiscTrack> - <Title>.<ext>
```

Path previews and staging/import workflow state are persisted for review. Import execution writes verified metadata, atomically moves files into `LIBRARY_ROOT`, and rolls filesystem changes back if the transaction fails. Staging paths live under `STAGING_ROOT` and must not escape it.

Extension tokens are sanitized with the same filesystem safety rules as other naming tokens, then capped at 32 characters. The final filename component is capped at 200 characters while preserving a dot plus the bounded sanitized extension.

## Stack

- **Backend** — Python 3.12+, FastAPI, SQLAlchemy 2.x (async), SQLite
- **Templates** — Jinja2 (server-side HTML for admin UI)
- **Task Queue** — restart-recoverable in-process dispatcher backed by persistent SQLite job and acquisition/import workflow records; no external broker
- **Containerisation** — Docker + Docker Compose

## Requirements

- Docker + Docker Compose v2
- `fpcalc` binary (Chromaprint) available in container for fingerprinting
- slskd instance reachable on the local network
- Prowlarr + SABnzbd instances reachable on the local network
- Valid AcoustID API key for cloud fingerprint lookup (optional); Deezer's public metadata API requires no key

## Quick Start

```bash
git clone https://github.com/noplexzone/audiohoard.git
cd audiohoard
cp .env.example .env
# fill in .env values, including a non-empty SECRET_KEY
docker compose up -d
```

The admin UI is served at `http://localhost:8000`. Acquisition records are shown under Downloads, and runtime source priority/result-cap settings are under Settings.

Jobs persist in SQLite and are recovered after restart. Downloads support cancellation and retry; slskd and SABnzbd jobs remain active until their external transfer reaches a terminal state and an artifact is verified.

## Health endpoints

| Endpoint | Authentication | Meaning |
|---|---|---|
| `/health/live` | Public | Cheap process-liveness response |
| `/health/ready` | Public | Database readiness; returns HTTP 503 when unavailable |
| `/health/sources` | Required | Cached provider diagnostics; does not probe providers during the GET |

For this direct LAN HTTP setup, keep `AUTH_COOKIE_SECURE=false` as shown in
`.env.example`; otherwise browsers will not return the session and CSRF cookies over
HTTP. Set `AUTH_COOKIE_SECURE=true` whenever Audiohoard is served behind HTTPS.
`SESSION_TTL_SECONDS` controls session lifetime and defaults to 43,200 seconds
(12 hours).

## Container image

The release workflow publishes tagged builds to `noplexzone/audiohoard` on Docker Hub after the quality gate passes. Pull v0.7.1 with:

```bash
docker pull noplexzone/audiohoard:0.7.1
```

## Continuous integration

Pull requests and pushes to `main` run pytest, Ruff lint and formatting checks, mypy, Python package build, and a Docker image build. Version tags run the same quality gate before publishing the Docker image.


## Database maintenance

Audiohoard normally maintains review-row integrity automatically. To audit historical
orphaned staging-review rows, run the maintenance command in dry-run mode:

```bash
uv run audiohoard-repair-reviews ./data/audiohoard.db
```

For a Docker Compose installation, audit the live database without changing it:

```bash
docker compose exec app audiohoard-repair-reviews /app/data/audiohoard.db
```

Applying the repair is deliberately gated. Stop Audiohoard, run the maintenance entrypoint
against the mounted database, then restart and verify readiness:

```bash
docker compose stop app
docker compose run --rm --entrypoint audiohoard-repair-reviews app \
  /app/data/audiohoard.db --apply --confirm-stopped
docker compose start app
docker compose exec app audiohoard-repair-reviews /app/data/audiohoard.db
```

The command creates and verifies a timestamped backup, obtains an exclusive SQLite write
lock, aborts if the database changed while the backup was captured, removes only review
rows whose track or release no longer exists, and verifies database/FK integrity before
committing. Never run `--apply` while the Audiohoard container is active.

### slskd ownership reconciliation report

Audit durable slskd attempt ownership, live queue UUIDs, exact staged artifacts, and old
empty download directories with the report-only reconciler:

```bash
docker compose exec app python -m app.maintenance.slskd_reconcile
```

The command opens SQLite read-only with `PRAGMA query_only`, makes one read-only slskd GET,
and only inspects filesystem metadata/content. It has no apply or delete mode and never
invokes cleanup. Output is bounded JSON and omits provider credentials and raw errors.

Empty-directory inspection and periodic sweeping remain disabled unless
`SLSKD_COMPLETE_ROOT` and/or `SLSKD_INCOMPLETE_ROOT` are explicitly configured as exact
**container-visible mounted paths**. Audiohoard never infers these roots from a remote
filename and never assumes an unmounted host path is equivalent. Sweeping is bottom-up,
removes directories only (never files or roots), does not follow symlinks, honors
`SLSKD_DIRECTORY_SWEEP_MIN_AGE_SECONDS` (default 24 hours), and skips the entire run when a
fresh slskd transfer snapshot is unavailable. Add matching read/write volume mounts to your
Compose override before enabling either root.
