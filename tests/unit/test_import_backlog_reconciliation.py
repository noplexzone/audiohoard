from __future__ import annotations

import asyncio
import json
from pathlib import Path

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.catalog_entities import CatalogAlbum, CatalogAlbumTrack, CatalogArtist
from app.models.import_plan import CollisionState, ImportPlan, LibraryFileState
from app.models.job import Job, JobStatus
from app.models.release import Release
from app.models.staging_review import StagingReviewItem
from app.models.track import Track
from app.models.workflow import (
    AcoustIDVerificationState,
    AcquisitionState,
    ImportWorkflowState,
)
from app.services.import_backlog_reconciliation import reconcile_import_backlog


async def _fixture(db: AsyncSession, tmp_path: Path) -> dict[str, object]:
    artist = CatalogArtist(name="Artist")
    album = CatalogAlbum(artist=artist, title="Album", track_count=4)
    catalog = [
        CatalogAlbumTrack(
            position=index,
            disc=1,
            title=f"Song {index}",
            duration_sec=180 + index,
            recording_mbid=f"00000000-0000-0000-0000-00000000000{index}",
        )
        for index in range(1, 5)
    ]
    album.tracks.extend(catalog)
    job = Job(source="slskd", query="Artist Album", status=JobStatus.done, catalog_album=album)
    release = Release(job=job, source="slskd", title="Album", album_artist="Artist")
    stale_release = Release(job=job, source="slskd", title="Old duplicate")
    db.add_all([artist, album, job, release, stale_release])
    await db.flush()

    source = tmp_path / "candidate.flac"
    source.write_bytes(b"candidate")
    identity_track = Track(
        job=job,
        release=release,
        source="slskd",
        title="Song 1 (feat. Guest)",
        staging_path=str(source),
        catalog_album_id=album.id,
        catalog_track_id=catalog[0].id,
        acquisition_state=AcquisitionState.downloaded,
        acoustid_verification_state=AcoustIDVerificationState.unavailable,
    )
    ambiguous_source = tmp_path / "ambiguous.flac"
    ambiguous_source.write_bytes(b"ambiguous")
    ambiguous_track = Track(
        job=job,
        release=release,
        source="slskd",
        title="Song 2",
        staging_path=str(ambiguous_source),
        catalog_album_id=album.id,
        catalog_track_id=catalog[1].id,
        acquisition_state=AcquisitionState.downloaded,
        acoustid_verification_state=AcoustIDVerificationState.unavailable,
    )
    db.add_all([identity_track, ambiguous_track])
    await db.flush()
    identity_review = StagingReviewItem(
        track_id=identity_track.id,
        release_id=release.id,
        expected_title="Song 1",
        observed_acoustid_mbids_json=json.dumps([catalog[0].recording_mbid]),
        fingerprint_duration_sec=catalog[0].duration_sec,
        acoustid_score=0.99,
        verification_reason="no_expected_mbid",
    )
    ambiguous_review = StagingReviewItem(
        track_id=ambiguous_track.id,
        release_id=release.id,
        expected_title="Song 2",
        observed_acoustid_mbids_json=json.dumps(
            [catalog[1].recording_mbid, catalog[2].recording_mbid]
        ),
        fingerprint_duration_sec=catalog[1].duration_sec,
        acoustid_score=1.0,
        verification_reason="no_expected_mbid",
    )

    destination = tmp_path / "music" / "Song 3.flac"
    destination.parent.mkdir()
    destination.write_bytes(b"owned")
    owner_track = Track(
        job=job,
        release=release,
        source="slskd",
        title="Song 3 owner",
        catalog_album_id=album.id,
        catalog_track_id=catalog[2].id,
        import_state=ImportWorkflowState.imported,
    )
    candidate_source = tmp_path / "projection.flac"
    candidate_source.write_bytes(b"owned")
    projection_track = Track(
        job=job,
        release=release,
        source="slskd",
        title="Song 3 projection",
        staging_path=str(candidate_source),
        catalog_album_id=album.id,
        catalog_track_id=catalog[2].id,
        acquisition_state=AcquisitionState.downloaded,
        import_state=ImportWorkflowState.needs_review,
    )
    wrong_track = Track(
        job=job,
        release=release,
        source="slskd",
        title="Wrong identity",
        staging_path=str(candidate_source),
        catalog_album_id=album.id,
        catalog_track_id=catalog[3].id,
        acquisition_state=AcquisitionState.downloaded,
        import_state=ImportWorkflowState.needs_review,
    )
    stale_track = Track(
        job=job,
        release=stale_release,
        source="slskd",
        title="Stale projection",
        catalog_album_id=album.id,
        catalog_track_id=catalog[3].id,
        acquisition_state=AcquisitionState.downloaded,
        import_state=ImportWorkflowState.needs_review,
    )
    db.add_all([owner_track, projection_track, wrong_track, stale_track])
    await db.flush()
    owner_plan = ImportPlan(
        release=release,
        track=owner_track,
        source_path=str(destination),
        destination_path=str(destination),
        status=ImportWorkflowState.imported,
        file_state=LibraryFileState.present,
    )
    projection_plan = ImportPlan(
        release=release,
        track=projection_track,
        source_path=str(candidate_source),
        staging_path=str(candidate_source),
        destination_path=str(destination),
        status=ImportWorkflowState.needs_review,
        collision_state=CollisionState.conflict,
        error_detail="destination already exists with different bytes",
    )
    wrong_plan = ImportPlan(
        release=release,
        track=wrong_track,
        source_path=str(candidate_source),
        staging_path=str(candidate_source),
        destination_path=str(destination),
        status=ImportWorkflowState.needs_review,
        collision_state=CollisionState.conflict,
    )
    stale_plan = ImportPlan(
        release=stale_release,
        track=stale_track,
        source_path=str(tmp_path / "stale.flac"),
        destination_path=str(tmp_path / "old.flac"),
        status=ImportWorkflowState.rolled_back,
        collision_state=CollisionState.duplicate,
        rollback_detail="duplicate destination already imported",
    )
    no_plan_track = Track(
        job=job,
        release=release,
        source="slskd",
        title="Missing no-plan",
        acquisition_state=AcquisitionState.downloaded,
        content_sha256="shared-hash",
        catalog_track_id=catalog[0].id,
    )
    conflicting_hash_track = Track(
        job=job,
        release=release,
        source="slskd",
        title="Conflicting hash",
        acquisition_state=AcquisitionState.downloaded,
        content_sha256="shared-hash",
        catalog_track_id=catalog[1].id,
    )
    artifact_track = Track(
        job=job,
        release=release,
        source="slskd",
        title="Artifact missing",
        acquisition_state=AcquisitionState.downloaded,
        import_state=ImportWorkflowState.needs_review,
    )
    db.add_all(
        [
            owner_plan,
            projection_plan,
            wrong_plan,
            stale_plan,
            no_plan_track,
            conflicting_hash_track,
            artifact_track,
        ]
    )
    await db.flush()
    hash_plan = ImportPlan(
        release=release,
        track=conflicting_hash_track,
        source_path=str(tmp_path / "hash.flac"),
        destination_path=str(tmp_path / "hash-destination.flac"),
        status=ImportWorkflowState.needs_review,
        collision_state=CollisionState.duplicate,
    )
    artifact_plan = ImportPlan(
        release=release,
        track=artifact_track,
        source_path=str(tmp_path / "missing.flac"),
        destination_path=str(tmp_path / "artifact-destination.flac"),
        status=ImportWorkflowState.needs_review,
        collision_state=CollisionState.needs_review,
    )
    db.add_all(
        [
            identity_review,
            ambiguous_review,
            hash_plan,
            artifact_plan,
        ]
    )
    await db.flush()
    return {
        "identity_track": identity_track,
        "identity_review": identity_review,
        "ambiguous_review": ambiguous_review,
        "projection_track": projection_track,
        "projection_plan": projection_plan,
        "wrong_plan": wrong_plan,
        "stale_track": stale_track,
        "stale_plan": stale_plan,
        "no_plan_track": no_plan_track,
        "hash_plan": hash_plan,
        "artifact_plan": artifact_plan,
        "stale_release": stale_release,
    }


async def test_reconciliation_dry_run_is_pure_and_reports_only_safe_candidates(
    db_session: AsyncSession, tmp_path: Path
) -> None:
    rows = await _fixture(db_session, tmp_path)

    report = await reconcile_import_backlog(db_session, acceptance_threshold=0.90, apply=False)

    assert report.dry_run is True
    assert report.identity_candidates == (rows["identity_review"].id,)
    assert report.destination_candidates == (rows["projection_plan"].id,)
    assert report.stale_projection_candidates == (rows["stale_plan"].id,)
    assert report.no_plan_tracks == (rows["no_plan_track"].id,)
    assert report.artifact_missing_plans == (rows["artifact_plan"].id,)
    assert report.cross_track_hash_conflict_plans == (rows["hash_plan"].id,)
    assert rows["identity_track"].mbid is None
    assert rows["projection_plan"].status == ImportWorkflowState.needs_review
    assert rows["stale_track"].import_state == ImportWorkflowState.needs_review


async def test_reconciliation_apply_is_idempotent_and_leaves_ambiguous_rows(
    db_session: AsyncSession, tmp_path: Path
) -> None:
    rows = await _fixture(db_session, tmp_path)

    first = await reconcile_import_backlog(db_session, acceptance_threshold=0.90, apply=True)
    await db_session.commit()

    assert first.identity_repaired == 1
    assert first.destinations_closed == 1
    assert first.stale_projections_normalized == 1
    assert rows["identity_track"].acoustid_verification_state == AcoustIDVerificationState.verified
    assert await db_session.get(StagingReviewItem, rows["identity_review"].id) is None
    assert await db_session.get(StagingReviewItem, rows["ambiguous_review"].id) is not None
    assert rows["projection_plan"].status == ImportWorkflowState.rolled_back
    assert rows["projection_track"].import_state == ImportWorkflowState.rolled_back
    assert rows["wrong_plan"].status == ImportWorkflowState.needs_review
    assert rows["stale_track"].import_state == ImportWorkflowState.rolled_back
    assert rows["stale_release"].review_dismissed_at is not None

    second = await reconcile_import_backlog(db_session, acceptance_threshold=0.90, apply=True)
    assert second.identity_repaired == 0
    assert second.destinations_closed == 0
    assert second.stale_projections_normalized == 0
    remaining = list((await db_session.scalars(select(StagingReviewItem))).all())
    assert remaining == [rows["ambiguous_review"]]


async def test_destination_identity_without_byte_equality_remains_for_review(
    db_session: AsyncSession, tmp_path: Path
) -> None:
    rows = await _fixture(db_session, tmp_path)
    plan = rows["projection_plan"]
    assert isinstance(plan, ImportPlan)
    assert plan.staging_path is not None
    await asyncio.to_thread(Path(plan.staging_path).write_bytes, b"different bytes")

    report = await reconcile_import_backlog(db_session, acceptance_threshold=0.90, apply=True)

    assert report.destination_candidates == ()
    assert report.destinations_closed == 0
    assert plan.status == ImportWorkflowState.needs_review
