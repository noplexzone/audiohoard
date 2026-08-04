from app.metadata.itunes import _parse_track


def test_itunes_track_parses_optional_preview_url() -> None:
    with_preview = _parse_track(
        {
            "trackId": 42,
            "trackNumber": 3,
            "trackName": "Previewed",
            "previewUrl": "https://audio-ssl.itunes.apple.com/preview.m4a",
        }
    )
    without_preview = _parse_track({"trackId": 43, "trackNumber": 4, "trackName": "Silent"})

    assert with_preview.preview_url == "https://audio-ssl.itunes.apple.com/preview.m4a"
    assert without_preview.preview_url is None
