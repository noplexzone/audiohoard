# Review Audio Alignment Implementation Plan

> **For Hermes:** Execute directly or with Claude Code task-by-task; Jarvis owns integration, verification, release, and artifact proof.

**Goal:** Match a Deezer 30-second reference preview to the equivalent timestamp in a staged downloaded file and provide truthful matched-section/A-B controls in `/review`.

**Architecture:** Add a bounded, schema-free alignment service that transiently downloads only Deezer preview audio, extracts raw Chromaprint fingerprints with the container's existing `fpcalc`, and finds the best offset by normalized Hamming distance. An authenticated review endpoint returns exact/high-confidence alignment or an explicitly estimated centered-preview start. The review JavaScript seeks and A/B-switches between equivalent moments without playing both sources simultaneously.

**Tech Stack:** FastAPI, httpx, `fpcalc`/Chromaprint, server-rendered Jinja, CSP-safe vanilla JavaScript, pytest.

## Global Constraints

- Work from `origin/main` v0.19.2 in `/mnt/user/appdata/dev/_scratch/audiohoard-review-audio-alignment`.
- No schema or migration.
- Reuse `_validate_audio_path`; do not create a second staging-path validator.
- Deezer is eligible for acoustic alignment. iTunes remains streamed independently and must not be downloaded, cached, or synchronized.
- Reference media is transient: delete temporary preview files and persist/cache only derived offset metadata in process memory.
- Bound remote size, timeout, redirects, subprocess duration, and concurrent alignments.
- Never label a heuristic as exact. Responses distinguish `matched`, `estimated`, and `unavailable`.
- Centered fallback start is `(downloaded_duration - reference_duration) / 2`, not the downloaded midpoint.
- Players do not play simultaneously. A/B switching pauses the current player, translates time using the offset, then starts the other.
- Preserve keyboard, swipe, volume persistence, approve/deny, and progressive-navigation behavior.
- Update `CHANGELOG.md` under `[Unreleased]`; if fully green, release as v0.20.0 under `docs/VERSIONING.md`.

## Tasks

1. **Alignment scoring contract:** add `app/services/audio_alignment.py` and `tests/unit/test_audio_alignment.py`; RED tests cover exact/noisy/ambiguous windows and centered fallback; implement normalized Hamming scoring and confidence.
2. **Bounded endpoint:** add authenticated `GET /staging/review/{item_id}/alignment`; reuse `_validate_audio_path`; transiently fetch only Deezer with size/timeout/redirect limits; run bounded `fpcalc`; return matched/estimated/unavailable without paths or provider errors; integration-test auth, provider policy, failure, and cleanup.
3. **Review controls:** add Match section, A/B, status region, and nudge controls to `review.html`/`review-deck.js`/`pages.css`; fetch on explicit action; never play both sources; translate time by offset; preserve keyboard/swipe/forms/navigation; test template and JS contracts.
4. **Verification and release:** update changelog; run generated-media probe and full pytest/ruff/format/mypy gate; independently review SSRF, containment, temp cleanup, subprocess bounds, confidence, CSP, and races; if green release v0.20.0, verify CI, Docker tags/digest, and disposable runtime without touching production.
