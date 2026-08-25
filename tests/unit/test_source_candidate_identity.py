from __future__ import annotations

from app.services.source_candidate_identity import normalize_source_candidate_identity


def test_slskd_identity_normalizes_remote_path_without_case_or_title_overblocking() -> None:
    expected = ("slskd", "StarCaller", "music/done/Album/01 Song.flac")
    assert (
        normalize_source_candidate_identity(
            " slskd ",
            " StarCaller ",
            r"  \\music\\done\.\Album\Disc 1\..\01 Song.flac  ",
        )
        == expected
    )

    assert (
        normalize_source_candidate_identity(
            "slskd", "starcaller", "/music/done/Album/01 Song.flac"
        )
        != expected
    )
    assert (
        normalize_source_candidate_identity("slskd", "StarCaller", "/other/01 Song.flac")
        != expected
    )


def test_slskd_identity_requires_peer_and_full_remote_path() -> None:
    assert normalize_source_candidate_identity("slskd", "", "/Album/01.flac") is None
    assert normalize_source_candidate_identity("slskd", "peer", "  ") is None
