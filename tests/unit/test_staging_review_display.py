from __future__ import annotations

import json

from app.models.staging_review import StagingReviewItem
from app.models.track import Track


def _item(track: Track) -> StagingReviewItem:
    return StagingReviewItem(track=track)


def test_review_source_details_use_slskd_remote_filename() -> None:
    item = _item(
        Track(
            job_id=1,
            source="slskd",
            source_path="/staging/renamed.flac",
            acquisition_provenance_json=json.dumps(
                {
                    "source": "slskd",
                    "username": "peer",
                    "filename": r"Remote Album\07 Original Track.flac",
                }
            ),
        )
    )

    assert item.source_label == "Soulseek (slskd)"
    assert item.source_username == "peer"
    assert item.source_folder == "Remote Album"
    assert item.original_filename == "07 Original Track.flac"


def test_review_source_details_fall_back_to_source_path_for_legacy_rows() -> None:
    item = _item(
        Track(
            job_id=1,
            source="prowlarr",
            source_path="/staging/downloaded/Original Usenet Name.m4a",
            acquisition_provenance_json="not-json",
        )
    )

    assert item.source_label == "Prowlarr / SABnzbd"
    assert item.source_username is None
    assert item.source_folder is None
    assert item.original_filename == "Original Usenet Name.m4a"


def test_review_source_details_are_optional_for_unknown_legacy_rows() -> None:
    item = _item(Track(job_id=1, source="", source_path=None, staging_path=None))

    assert item.source_label is None
    assert item.source_username is None
    assert item.source_folder is None
    assert item.original_filename is None


def test_review_source_folder_handles_slskd_backslash_paths() -> None:
    item = _item(
        Track(
            job_id=1,
            source="slskd",
            acquisition_provenance_json=json.dumps(
                {
                    "source": "slskd",
                    "username": "allbren",
                    "filename": r"downloads\LISA\Alter Ego\01 Rockstar.flac",
                }
            ),
        )
    )

    assert item.source_username == "allbren"
    assert item.source_folder == r"downloads\LISA\Alter Ego"
    assert item.original_filename == "01 Rockstar.flac"
