# Audiohoard

**Private, self-hosted music acquisition and library management — v0.6.0**

A FastAPI application that coordinates multiple acquisition sources, enriches tracks with metadata, fingerprints audio, and enforces strict library naming conventions. Designed to run entirely on-premises; no data leaves the host.

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

## Naming Convention

Files are renamed according to a strict, configurable template:

```
<AlbumArtist>/<Year> - <Album>/<DiscTrack> - <Title>.<ext>
```

Path previews and safe staging/import workflow state are computed and stored. Actual library mutation remains isolated behind future verified import execution; staging paths live under `STAGING_ROOT` and must not escape it.

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
- Valid API keys for AcoustID (optional) and Deezer

## Quick Start

```bash
git clone https://github.com/noplexzone/audiohoard.git
cd audiohoard
cp .env.example .env
# fill in .env values, including a non-empty SECRET_KEY
docker compose up -d
```

The admin UI is served at `http://localhost:8000`. Acquisition records are shown under Downloads, and runtime source priority/result-cap settings are under Settings.

For this direct LAN HTTP setup, keep `AUTH_COOKIE_SECURE=false` as shown in
`.env.example`; otherwise browsers will not return the session and CSRF cookies over
HTTP. Set `AUTH_COOKIE_SECURE=true` whenever Audiohoard is served behind HTTPS.
`SESSION_TTL_SECONDS` controls session lifetime and defaults to 43,200 seconds
(12 hours).

## Container image

The release workflow publishes tagged builds to `noplexzone/audiohoard` on Docker Hub after the quality gate passes. Pull v0.6.0 with:

```bash
docker pull noplexzone/audiohoard:0.6.0
```

## Continuous integration

Pull requests and pushes to `main` run pytest, Ruff lint and formatting checks, mypy, Python package build, and a Docker image build. Version tags run the same quality gate before publishing the Docker image.

## Version

v0.6.0 — monitored artist catalogs, Wanted acquisition, server-rendered management UI, and durable acquisition/import workflows
