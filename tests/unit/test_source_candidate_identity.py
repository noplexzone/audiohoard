from __future__ import annotations

from app.services.source_candidate_identity import normalize_source_candidate_identity


def test_slskd_identity_normalizes_remote_path_without_case_or_title_overblocking() -> None:
    expected = ("slskd", "StarCaller", "//music/done/Album/01 Song.flac")
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


def test_slskd_identity_preserves_relative_root_drive_and_unc_namespaces() -> None:
    peer = "peer"
    relative = normalize_source_candidate_identity("slskd", peer, "folder/../song.flac")
    posix = normalize_source_candidate_identity("slskd", peer, "/folder/../song.flac")
    drive = normalize_source_candidate_identity("slskd", peer, r"C:\folder\..\song.flac")
    unc = normalize_source_candidate_identity("slskd", peer, r"\\server\share\folder\..\song.flac")

    assert relative == ("slskd", peer, "song.flac")
    assert posix == ("slskd", peer, "/song.flac")
    assert drive == ("slskd", peer, "C:/song.flac")
    assert unc == ("slskd", peer, "//server/share/song.flac")
    assert len({relative, posix, drive, unc}) == 4


def test_slskd_identity_dotdot_cannot_escape_absolute_namespace_anchor() -> None:
    peer = "peer"
    assert normalize_source_candidate_identity("slskd", peer, "/../../song.flac") == (
        "slskd",
        peer,
        "/song.flac",
    )
    assert normalize_source_candidate_identity("slskd", peer, "C:/../../song.flac") == (
        "slskd",
        peer,
        "C:/song.flac",
    )
    assert normalize_source_candidate_identity(
        "slskd", peer, "//server/share/../../song.flac"
    ) == ("slskd", peer, "//server/share/song.flac")


def test_slskd_identity_preserves_drive_relative_traversal_namespace() -> None:
    peer = "peer"
    drive_relative_parent = normalize_source_candidate_identity("slskd", peer, "C:../song.flac")
    drive_relative = normalize_source_candidate_identity("slskd", peer, "C:song.flac")
    drive_absolute = normalize_source_candidate_identity("slskd", peer, "C:/../song.flac")
    ordinary_relative = normalize_source_candidate_identity("slskd", peer, "../song.flac")

    assert drive_relative_parent == ("slskd", peer, "C:../song.flac")
    assert drive_relative == ("slskd", peer, "C:song.flac")
    assert drive_absolute == ("slskd", peer, "C:/song.flac")
    assert ordinary_relative == ("slskd", peer, "../song.flac")
    assert len({drive_relative_parent, drive_relative, drive_absolute, ordinary_relative}) == 4


def test_slskd_identity_rejects_malformed_unc_without_server_and_share() -> None:
    assert normalize_source_candidate_identity("slskd", "peer", "//server") is None
    assert normalize_source_candidate_identity("slskd", "peer", "//server/") is None
    assert normalize_source_candidate_identity("slskd", "peer", "//../share/song.flac") is None
