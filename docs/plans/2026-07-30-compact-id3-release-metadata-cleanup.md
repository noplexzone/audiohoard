# Compact ID3 Release Metadata Cleanup Implementation Plan

**Goal:** Prevent compact ID3 release tags from splitting imported or repaired MP3 tracks in Navidrome.

**Architecture:** Normalize TXXX descriptions to alphanumeric keys and compare them against Audiohoard's managed metadata set. Normal import and Repair Metadata share the writer.

## Constraints
- Preserve unrelated metadata such as ReplayGain, lyrics, and AcoustID.
- Do not modify the production library, databases, or running containers.
- Publish through tagged release CI only; no local Docker push or latest tag.

## Task 1 — TDD cleanup fix
1. Add a failing regression with the exact compact descriptors observed in production.
2. Normalize managed TXXX descriptions and clear separator variants.
3. Verify canonical replacement tags remain and unrelated tags survive.
4. Run focused and full gates; update CHANGELOG.

## Task 2 — Release 0.9.2
1. Re-read docs/VERSIONING.md and update version metadata.
2. Merge without squash, tag v0.9.2, push main and tag.
3. Verify release CI and noplexzone/audiohoard:develop digest.
