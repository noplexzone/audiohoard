from __future__ import annotations

import json

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.job import Job, JobStatus
from app.models.release import Release
from app.models.release_candidate import MatchReviewState
from app.models.track import Track
from app.models.workflow import ImportWorkflowState
from app.services.edition_matching import EditionEvidence, resolve_release_match


async def _release_with_track(db_session: AsyncSession) -> tuple[Release, Track]:
    job = Job(source="slskd", query="artist album", status=JobStatus.pending)
    db_session.add(job)
    await db_session.flush()
    release = Release(
        job_id=job.id,
        source="slskd",
        title="Album",
        album_artist="Artist",
        year="1999",
        release_mbid="release-original",
        country="US",
        label="Label",
        catalog_number="CAT-001",
        barcode="123456789012",
        track_count=10,
        import_state=ImportWorkflowState.matching,
    )
    db_session.add(release)
    await db_session.flush()
    track = Track(
        job_id=job.id,
        release_id=release.id,
        title="Song",
        artist="Artist",
        album_artist="Artist",
        album="Album",
        year="1999",
        disc=1,
        disc_total=1,
        track_no=1,
        duration_sec=180,
        mbid="recording-1",
        acoustid="fingerprint-only-id",
        source="slskd",
        import_state=ImportWorkflowState.matching,
    )
    db_session.add(track)
    await db_session.flush()
    return release, track


async def test_unattended_match_selects_release_when_recording_and_release_agree(
    db_session: AsyncSession,
) -> None:
    release, track = await _release_with_track(db_session)

    result = await resolve_release_match(
        db_session,
        release,
        track,
        [
            EditionEvidence(
                recording_mbid="recording-1",
                release_mbid="release-original",
                medium_position=1,
                track_position=1,
                duration_sec=192,
                track_count=10,
                year="1999",
                country="US",
                label="Label",
                catalog_number="CAT-001",
                barcode="123456789012",
                source="musicbrainz",
            )
        ],
    )
    await db_session.refresh(release, attribute_names=["candidates"])

    assert result.state == ImportWorkflowState.ready
    assert result.selected_candidate is not None
    assert result.selected_candidate.selected is True
    assert result.selected_candidate.review_state == MatchReviewState.auto_selected
    assert release.import_state == ImportWorkflowState.ready
    assert track.import_state == ImportWorkflowState.ready
    reasons = result.selected_candidate.match_reasons
    assert any("release MBID agrees" in reason for reason in reasons)
    assert any("duration within drift tolerance" in reason for reason in reasons)
    persisted_evidence = json.loads(result.selected_candidate.evidence_json or "{}")
    assert persisted_evidence["recording_mbid"] == "recording-1"


async def test_multidisc_position_and_compilation_artist_can_match(
    db_session: AsyncSession,
) -> None:
    release, track = await _release_with_track(db_session)
    release.album_artist = "Various Artists"
    track.album_artist = "Various Artists"
    track.artist = "Featured Artist"
    track.disc = 2
    track.disc_total = 2

    result = await resolve_release_match(
        db_session,
        release,
        track,
        [
            EditionEvidence(
                recording_mbid="recording-1",
                release_mbid="release-original",
                artist="Featured Artist",
                album_artist="Various Artists",
                medium_position=2,
                track_position=1,
                duration_sec=180,
                track_count=10,
                source="musicbrainz",
            )
        ],
    )

    assert result.state == ImportWorkflowState.ready
    assert result.selected_candidate is not None
    reasons = result.selected_candidate.match_reasons
    assert any("medium position agrees" in reason for reason in reasons)
    assert any("compilation artist evidence accepted" in reason for reason in reasons)


@pytest.mark.parametrize(
    "candidate",
    [
        EditionEvidence(recording_mbid="recording-1", source="acoustid"),
        EditionEvidence(
            recording_mbid="recording-1",
            release_mbid=None,
            medium_position=1,
            track_position=1,
            source="musicbrainz",
        ),
        EditionEvidence(
            recording_mbid="recording-1",
            release_mbid="release-original",
            medium_position=1,
            track_position=1,
            barcode="DIFFERENT",
            catalog_number="OTHER-CAT",
            source="musicbrainz",
        ),
    ],
)
async def test_incomplete_acoustid_or_contradictory_evidence_needs_review(
    db_session: AsyncSession, candidate: EditionEvidence
) -> None:
    release, track = await _release_with_track(db_session)

    result = await resolve_release_match(db_session, release, track, [candidate])

    assert result.state == ImportWorkflowState.needs_review
    assert result.selected_candidate is None
    assert release.import_state == ImportWorkflowState.needs_review
    assert release.error_detail == "release-match review: no unambiguous edition candidate"
    assert track.import_state == ImportWorkflowState.needs_review
    assert result.candidates[0].review_state == MatchReviewState.needs_review
    assert result.candidates[0].selected is False


async def test_tied_candidates_need_review_and_do_not_select_edition(
    db_session: AsyncSession,
) -> None:
    release, track = await _release_with_track(db_session)

    result = await resolve_release_match(
        db_session,
        release,
        track,
        [
            EditionEvidence(
                recording_mbid="recording-1",
                release_mbid="release-original",
                medium_position=1,
                track_position=1,
                duration_sec=180,
                track_count=10,
                year="1999",
                source="musicbrainz",
            ),
            EditionEvidence(
                recording_mbid="recording-1",
                release_mbid="release-original",
                medium_position=1,
                track_position=1,
                duration_sec=180,
                track_count=10,
                year="1999",
                source="musicbrainz",
            ),
        ],
    )

    assert result.state == ImportWorkflowState.needs_review
    assert result.selected_candidate is None
    assert all(candidate.selected is False for candidate in result.candidates)
    assert all(
        candidate.review_state == MatchReviewState.needs_review for candidate in result.candidates
    )


async def test_manual_selection_is_auditable_and_sets_ready_state(
    db_session: AsyncSession,
) -> None:
    release, track = await _release_with_track(db_session)
    result = await resolve_release_match(
        db_session,
        release,
        track,
        [
            EditionEvidence(
                recording_mbid="recording-1",
                release_mbid="release-original",
                medium_position=1,
                track_position=1,
                source="musicbrainz",
            )
        ],
    )
    candidate = result.candidates[0]

    manual = await resolve_release_match(
        db_session,
        release,
        track,
        [],
        manual_candidate_id=candidate.id,
        reviewer="operator",
        review_note="matched liner notes barcode",
    )

    assert manual.state == ImportWorkflowState.ready
    assert manual.selected_candidate is not None
    assert manual.selected_candidate.id == candidate.id
    assert manual.selected_candidate.review_state == MatchReviewState.manual_selected
    audit = json.loads(manual.selected_candidate.review_audit_json or "{}")
    assert audit == {"reviewer": "operator", "note": "matched liner notes barcode"}


async def test_contradictory_release_attributes_force_review_even_with_mbid_agreement(
    db_session: AsyncSession,
) -> None:
    release, track = await _release_with_track(db_session)

    result = await resolve_release_match(
        db_session,
        release,
        track,
        [
            EditionEvidence(
                recording_mbid="recording-1",
                release_mbid="release-original",
                medium_position=1,
                track_position=1,
                duration_sec=180,
                track_count=9,
                year="2001",
                country="GB",
                label="Different Label",
                catalog_number="CAT-001",
                barcode="123456789012",
                source="musicbrainz",
            )
        ],
    )

    assert result.state == ImportWorkflowState.needs_review
    assert result.selected_candidate is None
    reasons = result.candidates[0].match_reasons
    assert any("track count contradicts release metadata" in reason for reason in reasons)
    assert any("year contradicts release metadata" in reason for reason in reasons)
    assert any("country contradicts release metadata" in reason for reason in reasons)
    assert any("label contradicts release metadata" in reason for reason in reasons)


async def test_manual_selection_rejects_candidate_for_another_track(
    db_session: AsyncSession,
) -> None:
    release, track = await _release_with_track(db_session)
    other_track = Track(
        job_id=track.job_id,
        release_id=release.id,
        title="Other Song",
        source="slskd",
        import_state=ImportWorkflowState.matching,
    )
    db_session.add(other_track)
    await db_session.flush()
    other_result = await resolve_release_match(
        db_session,
        release,
        other_track,
        [EditionEvidence(recording_mbid="other", release_mbid="release-original")],
    )

    with pytest.raises(ValueError, match="candidate does not belong"):
        await resolve_release_match(
            db_session,
            release,
            track,
            [],
            manual_candidate_id=other_result.candidates[0].id,
            reviewer="operator",
        )
