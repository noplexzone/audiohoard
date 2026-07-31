from app.metadata.filename_parse import (
    compose_search_query,
    parse_filename,
    strip_non_identity_descriptors,
)


def test_parse_artist_album_title_with_track_and_codec() -> None:
    guess = parse_filename("The Cure - Disintegration - 01 - Plainsong [FLAC].flac")
    assert guess.artist == "The Cure"
    assert guess.album == "Disintegration"
    assert guess.title == "Plainsong"
    assert guess.confidence >= 0.8


def test_parse_artist_title_and_keep_remaster_hint_out_of_title() -> None:
    guess = parse_filename("01. Radiohead - Paranoid Android (2019 Remaster).mp3")
    assert guess.artist == "Radiohead"
    assert guess.title == "Paranoid Android"
    assert "2019 Remaster" in guess.hints


def test_fielded_query_composition() -> None:
    assert (
        compose_search_query("", "Nirvana", "Nevermind", "Lithium") == "Nirvana Nevermind Lithium"
    )


def test_featured_artist_annotation_is_identity_preserving() -> None:
    assert (
        strip_non_identity_descriptors("Miami (feat. Lil Wayne & Rick Ross)")
        == "Miami (feat. Lil Wayne & Rick Ross)"
    )
    assert (
        strip_non_identity_descriptors("Heartless (Feat Julia Michaels)")
        == "Heartless (Feat Julia Michaels)"
    )
