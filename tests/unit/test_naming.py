from __future__ import annotations

from pathlib import Path

import pytest

from app.naming.convention import NamingError, _sanitize_segment, render_path

DEFAULT = "{album_artist}/{album} ({year})/{disc_track} - {title}.{ext}"


class TestSanitizeSegment:
    def test_strips_unsafe_chars(self) -> None:
        assert "/" not in _sanitize_segment("AC/DC")
        assert "*" not in _sanitize_segment("he*llo")
        assert "?" not in _sanitize_segment("what?")
        assert ":" not in _sanitize_segment("foo:bar")

    def test_replaces_dot_sequences(self) -> None:
        result = _sanitize_segment("foo..bar")
        assert ".." not in result

    def test_preserves_unicode(self) -> None:
        assert "ü" in _sanitize_segment("Günterscheid")
        assert "é" in _sanitize_segment("Beyoncé")
        assert "中" in _sanitize_segment("中文")

    def test_truncates_long_segment(self) -> None:
        long = "A" * 300
        assert len(_sanitize_segment(long)) <= 200

    def test_windows_reserved_names(self) -> None:
        for name in ("CON", "PRN", "AUX", "NUL", "COM1", "LPT9"):
            result = _sanitize_segment(name)
            assert result.upper() != name

    def test_empty_becomes_underscore(self) -> None:
        result = _sanitize_segment("")
        assert result == "_"

    def test_control_chars_removed(self) -> None:
        result = _sanitize_segment("foo\x00bar\x1f")
        assert "\x00" not in result
        assert "\x1f" not in result

    def test_repeated_underscores_are_collapsed_after_replacement(self) -> None:
        assert _sanitize_segment("AC//DC::*Live") == "AC_DC_Live"


class TestRenderPath:
    def test_nominal_single_disc(self) -> None:
        path = render_path(
            title="Bohemian Rhapsody",
            album_artist="Queen",
            album="A Night at the Opera",
            year="1975",
            track_no=11,
            ext="flac",
            template=DEFAULT,
        )
        assert path == "Queen/A Night at the Opera (1975)/11 - Bohemian Rhapsody.flac"

    def test_nominal_multi_disc(self) -> None:
        path = render_path(
            title="Comfortably Numb",
            album_artist="Pink Floyd",
            album="The Wall",
            year="1979",
            disc=2,
            disc_total=2,
            track_no=6,
            ext="flac",
            template=DEFAULT,
        )
        assert path == "Pink Floyd/The Wall (1979)/2-06 - Comfortably Numb.flac"

    def test_track_zero_padded_two_digits(self) -> None:
        path = render_path(
            title="Track One",
            album_artist="Artist",
            album="Album",
            year="2020",
            track_no=1,
            ext="mp3",
            template=DEFAULT,
        )
        assert "/01 - Track One.mp3" in path

    def test_missing_title_raises(self) -> None:
        with pytest.raises(NamingError, match="title"):
            render_path(title="", ext="flac", template=DEFAULT)

    def test_missing_ext_raises(self) -> None:
        with pytest.raises(NamingError, match="ext"):
            render_path(title="Song", ext="", template=DEFAULT)

    def test_path_traversal_neutralised(self) -> None:
        path = render_path(
            title="Song",
            album_artist="AC/DC",
            album="Album",
            year="2020",
            track_no=1,
            ext="flac",
            template=DEFAULT,
        )
        assert "../" not in path
        assert path.split("/")[0] == "AC_DC"

    def test_path_containment_check(self, tmp_path: Path) -> None:
        root = tmp_path / "library"
        root.mkdir()
        path = render_path(
            title="Legit Song",
            album_artist="Artist",
            album="Album",
            year="2020",
            track_no=1,
            ext="flac",
            template=DEFAULT,
            library_root=root,
        )
        full = (root / path).resolve()
        assert str(full).startswith(str(root.resolve()))

    def test_path_traversal_neutralised_by_sanitization(self, tmp_path: Path) -> None:
        root = tmp_path / "library"
        root.mkdir()
        evil = "../../../etc/passwd"
        rendered = render_path(
            title=evil,
            album_artist=evil,
            album=evil,
            year=evil,
            ext="flac",
            template=DEFAULT,
            library_root=root,
        )
        assert "../" not in rendered
        full = (root / rendered).resolve()
        assert str(full).startswith(str(root.resolve()))

    def test_traversal_in_template_segments_raises(self, tmp_path: Path) -> None:
        root = tmp_path / "library"
        root.mkdir()
        with pytest.raises(NamingError, match="escapes library root"):
            render_path(
                title="song",
                ext="flac",
                template="../../outside/{title}.{ext}",
                library_root=root,
            )

    def test_accepts_dict_tokens(self) -> None:
        path = render_path(
            {
                "title": "Song",
                "album_artist": "Artist",
                "album": "Album",
                "year": "2020",
                "track_no": 3,
                "ext": "flac",
            },
            template=DEFAULT,
        )
        assert path == "Artist/Album (2020)/03 - Song.flac"

    def test_accepts_track_like_object(self) -> None:
        class TrackLike:
            title = "Song"
            artist = "Artist"
            album_artist = "Artist"
            album = "Album"
            year = "2020"
            disc = 2
            disc_total = 2
            track_no = 3
            source_path = "/downloads/song.mp3"

        assert render_path(TrackLike(), template=DEFAULT) == "Artist/Album (2020)/2-03 - Song.mp3"

    def test_unknown_template_token_raises(self) -> None:
        with pytest.raises(NamingError, match="token"):
            render_path(
                title="Song",
                ext="flac",
                template="{unknown_token}/{title}.{ext}",
            )

    def test_unicode_preserved_in_path(self) -> None:
        path = render_path(
            title="Für Elise",
            album_artist="Beethoven",
            album="Bagatelles",
            year="1867",
            track_no=1,
            ext="flac",
            template=DEFAULT,
        )
        assert "Für Elise" in path

    def test_default_fallbacks_for_missing_optional_tokens(self) -> None:
        path = render_path(title="A Track", ext="mp3", template=DEFAULT)
        assert "Unknown Artist" in path or "Unknown Album" in path
        assert path.endswith(".mp3")

    def test_long_title_preserves_filename_extension_and_segment_limit(self) -> None:
        path = render_path(
            title="T" * 400,
            album_artist="Artist",
            album="Album",
            year="2026",
            track_no=1,
            ext="flac",
            template=DEFAULT,
        )

        filename = path.split("/")[-1]
        assert len(filename) <= 200
        assert filename.startswith("01 - ")
        assert filename.endswith(".flac")

    def test_long_title_and_extension_preserve_bounded_extension(self) -> None:
        path = render_path(
            title="タイトル" * 100,
            album_artist="Artist",
            album="Album",
            year="2026",
            track_no=1,
            ext="拡張子" * 100,
            template=DEFAULT,
        )

        filename = path.split("/")[-1]
        expected_ext = "拡張子" * 10 + "拡張"
        assert len(expected_ext) == 32
        assert len(filename) <= 200
        assert "." in filename
        assert filename.endswith(f".{expected_ext}")

    def test_sanitized_extension_policy_is_bounded_and_preserves_dot(self) -> None:
        path = render_path(
            title="T" * 400,
            album_artist="Artist",
            album="Album",
            year="2026",
            track_no=1,
            ext=("bad/ext?." * 50),
            template=DEFAULT,
        )

        filename = path.split("/")[-1]
        expected_ext = "bad_ext_.bad_ext_.bad_ext_.bad_e"
        assert len(expected_ext) == 32
        assert len(filename) <= 200
        assert filename.endswith(f".{expected_ext}")
