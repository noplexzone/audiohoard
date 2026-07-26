# Changelog

All notable changes to Audiohoard are documented here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

## [0.6.1] - 2026-07-26

### Added
- Added restart-safe in-process job dispatch with startup recovery, retry, cancellation, and monitored-album scheduling.
- Added terminal-state polling and artifact verification for slskd and SABnzbd acquisitions.
- Added download queue details, live refresh, actionable errors, and browser retry/cancel controls.
- Added visible account logout controls and direct Artists/Imports navigation on mobile.
- Added a dispatcher watchdog that recovers orphaned active jobs once and then fails recurring losses with a structured `dispatch_lost` reason.
- Added filterable dashboard and download status links, source-priority reordering controls, and library/staging path diagnostics.

### Changed
- Prowlarr album search results are handled as alternative release candidates instead of individual tracks.
- Audio discovery and import now share one verified format contract for FLAC, MP3, M4A/MP4, Ogg Vorbis, and Opus.
- Browser settings validation now tests entered provider credentials before saving and reports Ready, Degraded, and Disabled states consistently.
- Search and settings screens now use clearer labels, table semantics, contrast, and accessible target sizes.
- The release workflow can publish the moving `develop` image on demand without creating a version tag.
- Job state and per-result progress now commit in short transactions so active downloads expose elapsed time, last activity, transfer source, and track-level progress while work is running.

### Fixed
- Prevented external acquisitions from completing before a usable staged artifact exists.
- Removed arbitrary positional catalog-track assignment and album-level MBID promotion to resolved track identity.
- Successful complete album imports now update catalog ownership; incomplete and rolled-back imports remain Wanted.
- Monitoring refresh failures are isolated per artist and no longer terminate the scheduler.
- Public liveness/readiness checks no longer probe or expose provider diagnostics, and failed readiness returns HTTP 503.
- Expired browser sessions are deleted and HTML requests redirect to login while API clients retain JSON 401 responses.
- Import execution now requires a persisted, reviewed ready plan instead of silently planning and importing in one action.
- Dashboard status counts include partial jobs and remain responsive with all six terminal/active states.
- Runtime version labels, README examples, Compose image tags, and health checks now match v0.6.0.
- Login redirects now preserve safe deep links, show the signed-in username, and reject open redirects.
- Import and settings form failures now redirect with browser-readable errors instead of returning server errors.
- Invalid behavior, metadata, and naming values are rejected without partially saving settings.
- Guarded job startup and dispatcher exception observation prevent provider or settings failures from leaving jobs invisibly pending.
- Artist enrichment and startup repair merge duplicate provider identities without losing monitoring state or albums, including legacy rows where only one normalized-name match has an MBID.
- Manual and background enrichment failures now render concise sanitized banners and redirect cleanly instead of exposing SQL details or returning a raw HTTP 500.
- Main-branch protection now requires the full `Quality checks` and Docker build checks; tag publication remains dependent on the release workflow’s full quality job.

## [0.6.0] - 2026-07-22

### Added
- Rebuilt catalog artist pages with artwork hero headers, identity chips, grouped discography cards, filter chips, compact monitor controls, and native form actions.
- Added catalog album duplicate reconciliation for legacy punctuation-normalization collisions.

### Changed
- Cached PBKDF2-derived Fernet settings keys per process, removing repeated 200,000-iteration derivation from request-time secret decrypts.
- Regenerated favicons with border-connected background keying to transparent alpha while keeping launcher icons opaque.
- Enrichment fills missing artist artwork from Deezer, backfills album track counts from providers without overwriting opened tracklists, and schedules first-open background enrichment.

### Fixed
- Scoped global form-control CSS so checkboxes and radios no longer render as full-width 42px controls.
- Normalized Unicode apostrophes, quotes, and dashes during album matching so curly/straight punctuation variants dedupe correctly.

## [0.5.0] - 2026-07-22

### Changed
- Project renamed to Audiohoard; all visible strings, branding, and packaging updated.
- Added `display_name()` helper and Jinja2 filter/global for provider and source labels.
- Generated branding assets (favicon, apple-touch-icon, PWA icons, webmanifest).
- MusicBrainz default app name and version updated to `audiohoard`/`0.5.0`.
- Docker image, container name, database path, and staging root updated to audiohoard.

## [0.4.1] - 2026-07-22

### Fixed
- Fixed changelog page rendering for markdown links.

## [0.4.0] - 2026-07-22

### Added
- Artist monitoring, wanted-album views, per-album monitor controls, and in-app discography refresh settings.
- Cross-provider artist enrichment with conservative matching, provenance, provider badges, and manual enrichment.
- Primary metadata provider selection and catalog search defaulting to the primary provider.
- Sectioned settings pages and an About changelog page.

### Changed
- Catalog and free-text downloads use shared source-priority fallback and record attempted-to-served provenance.

## [0.3.0] - 2026-07-21

### Added
- Metadata catalog search, catalog artist pages, catalog albums, and catalog-driven acquisition entry points.

## [0.2.1] - 2026-07-20

### Fixed
- Restored native form submission behavior in the v0.2 UI.

## [0.2.0] - 2026-07-20

### Added
- Dashboard and server-rendered v0.2 application shell with library, artist, downloads, imports, and settings navigation.

## [0.1.3] - 2026-07-19

### Added
- TIDAL acquisition support and provider settings improvements.

## [0.1.2] - 2026-07-19

### Fixed
- Database URL handling during migrations.

## [0.1.1] - 2026-07-18

### Fixed
- Initial release hardening fixes after v0.1.0.

## [0.1.0] - 2026-07-18

### Added
- Initial self-hosted music acquisition workflow, jobs, source adapters, settings, and Docker packaging.
