from __future__ import annotations

from app.services.catalog import track_meets_quality
from app.settings_service import QualityProfile


def test_mp3_is_upgrade_eligible_when_flac_is_preferred() -> None:
    profile = QualityProfile(
        format_preference=["flac", "m4a/aac", "mp3", "opus"],
        min_mp3_bitrate=320,
        allow_lower_quality_fallback=True,
    )

    assert not track_meets_quality("mp3", profile)
    assert not track_meets_quality("mp3 320kbps", profile)
    assert track_meets_quality("flac", profile)


def test_top_ranked_mp3_uses_minimum_bitrate_threshold() -> None:
    profile = QualityProfile(
        format_preference=["mp3", "flac", "m4a/aac", "opus"],
        min_mp3_bitrate=320,
        allow_lower_quality_fallback=True,
    )

    assert not track_meets_quality("mp3 192kbps", profile)
    assert track_meets_quality("mp3 320kbps", profile)
    assert track_meets_quality("mp3", profile)
