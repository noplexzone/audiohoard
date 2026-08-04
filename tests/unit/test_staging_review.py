from types import SimpleNamespace

from app.services.staging import build_review_item, build_tag_diff


def test_tag_diff_flags_title_but_normalizes_album() -> None:
    as_tagged = {
        "title": "Wrong title",
        "artist": None,
        "album": "  THE ALBUM  ",
        "album_artist": None,
        "track_number": None,
        "disc_number": None,
        "year": None,
        "genre": None,
    }
    should_be = {**as_tagged, "title": "Right title", "album": "the album"}

    diff = build_tag_diff(as_tagged, should_be)

    assert diff["title"] is True
    assert diff["album"] is False


async def test_review_item_keeps_missing_reference_as_none(test_settings) -> None:
    artist = SimpleNamespace(name="Catalog Artist")
    album = SimpleNamespace(
        title="Catalog Album",
        year="2026",
        artist=artist,
        artwork_url=None,
        track_count=1,
    )
    catalog_track = SimpleNamespace(
        title="Catalog Track",
        position=1,
        disc=1,
        duration_sec=180,
        album=album,
    )
    track = SimpleNamespace(
        staging_path=None,
        catalog_track=catalog_track,
        catalog_album=album,
        title="Catalog Track",
        artist="Catalog Artist",
        album="Catalog Album",
        album_artist="Catalog Artist",
        track_no=1,
        disc=1,
        year="2026",
        deezer_id=None,
    )
    item = SimpleNamespace(
        id=9,
        track=track,
        release=SimpleNamespace(title="Catalog Album", album_artist="Catalog Artist"),
        expected_title="Catalog Track",
        expected_recording_mbid=None,
        observed_acoustid_mbids=[],
        acoustid_score=None,
        fingerprint_duration_sec=None,
        source_label="Soulseek (slskd)",
        original_filename="original.flac",
    )

    view = await build_review_item(item, test_settings, resolve_reference=False)

    assert view["reference"] is None
