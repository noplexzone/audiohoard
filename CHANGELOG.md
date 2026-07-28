# Changelog

All notable changes to Audiohoard are documented here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

### Changed
- Library release cards and album details now show downloaded-versus-wanted track progress, replacing duplicate downloaded-file and wanted-release sections.
- Downloads now group all acquisition attempts for the same release and automatically clear completed or timed-out work from Audiohoard and slskd queues after durable state is saved.
- Verified tracks now import immediately without waiting for the rest of an album, while missing tracks remain available for targeted continuation searches.
- The default album directory format is now `{album} ({year})`, while track filenames retain their disc and track prefixes.

### Fixed
- Download queue grouping now retains completed release progress without re-exposing hidden attempts, bounds relationship loading to selected groups, and records successful slskd cleanup so startup does not repeat historical transfer deletions.
- Successful imports now remove empty acquisition directories below the configured staging root while preserving non-empty directories and the staging root itself.
- Matching expected recording MBIDs above the configured AcoustID threshold now auto-verify even when the fingerprint result lists alternate recordings, and stale review rows are cleared.
- Startup reconciliation removes terminal acquisition rows that no longer have a staged or imported file, preventing old jobs from affecting later acquisitions.
- Partial imports preserve committed plans and process only newly eligible tracks instead of replanning or rolling back the entire release.

## [0.7.0] - 2026-07-27

### Added
- Added configurable AcoustID auto-acceptance and slskd timeout controls, plus a durable timed-out candidate blocklist and alternate-source retries.
- Added source-independent quality profiles, coherent slskd album-folder scoring, bounded missing-track continuation, AcoustID verification, and authenticated staged-audio review controls.
- Added non-destructive queue controls to hide individual terminal downloads, clear failed downloads, or clear all finished downloads while preserving library metadata.
- Added provider-native artist identities and discography snapshots, with per-artist watchlist-provider selection and MusicBrainz, Deezer, and iTunes discography switching.
- Added an Alembic-to-model schema parity gate and mocked slskd-to-staging integration coverage for verified auto-import and review outcomes.
- Added persisted import-review reasons with non-destructive Dismiss controls and bounded Re-acquire handling for missing staged sources.

### Changed
- Replaced the manual Import tab with automatic transactional import after catalog completeness and fingerprint verification gates pass.
- Replaced every application icon and favicon with the new Audiohoard artwork.
- Consolidated Artists into one filterable watchlist with artwork and release-type counts.
- Library views now include only successfully downloaded files and display their paths and metadata.
- Replaced user-facing monitoring terminology with watchlist terminology and restored a conventional settings gear icon.
- Exposed the ordered Quality profile in Settings and enforced format-family, minimum MP3 bitrate, and lower-quality fallback rules during slskd selection.

### Fixed
- Album downloads now hydrate catalog track manifests before dispatch, report incomplete slskd folders as partial with targeted continuations, and release SQLite write locks before provider waits.
- Approved legacy albums now reconcile catalog tracks and resume transactional import; committed imports remove staging inputs and completed slskd transfers.
- Library and dashboard projections now exclude staging-only downloads and require committed import destinations.
- Album acquisitions no longer flatten coherent slskd folders into a ten-file cap, and normalized catalog matching now reports only genuinely missing tracks.
- Reconciled slskd transfers by peer and filename when enqueue responses omit the provider UUID, and restored compatibility with the legacy staging mount when the new default path is absent.
- Provider-specific release kinds no longer contaminate one another, and Albums-only views and watchlist policies now exclude Singles and EPs, including iTunes `- Single` and `- EP` releases.
- Corrected slskd download enqueue payloads to use the required request array, preserved validation details, and handled nested transfer responses.
- Made pull-request, main, and tag quality checks authoritative and uniquely named so release checks cannot satisfy branch protection accidentally.
- Persisted and rendered AcoustID mismatch, verification-unavailable, track-count, planning, missing-source, and execution failure reasons without deleting retained source or audit records on dismissal.

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
- Runtime version labels, README examples, Compose image tags, and health checks now match v0.6.1.
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
