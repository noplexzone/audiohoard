from __future__ import annotations

CONTENT_RATING_EXPLICIT = "explicit"
CONTENT_RATING_CLEAN = "clean"
CONTENT_RATING_NOT_EXPLICIT = "not_explicit"
CONTENT_RATING_UNKNOWN = "unknown"
VALID_CONTENT_RATINGS = {
    CONTENT_RATING_EXPLICIT,
    CONTENT_RATING_CLEAN,
    CONTENT_RATING_NOT_EXPLICIT,
    CONTENT_RATING_UNKNOWN,
}


def normalize_content_rating(value: object) -> str:
    text = str(value or "").strip().casefold().replace("-", "_").replace(" ", "_")
    if text in {"explicit", "e"}:
        return CONTENT_RATING_EXPLICIT
    if text in {"clean", "cleaned"}:
        return CONTENT_RATING_CLEAN
    if text in {"not_explicit", "notexplicit", "not_explicitly_rated", "none", "false"}:
        return CONTENT_RATING_NOT_EXPLICIT
    return CONTENT_RATING_UNKNOWN


def deezer_content_rating(data: dict[str, object]) -> str:
    explicit = data.get("explicit_lyrics")
    lyrics = data.get("explicit_content_lyrics")
    cover = data.get("explicit_content_cover")
    if explicit is True or str(lyrics) == "1" or str(cover) == "1":
        return CONTENT_RATING_EXPLICIT
    if explicit is False and str(lyrics) in {"2", "3", "6"}:
        return CONTENT_RATING_CLEAN
    return CONTENT_RATING_UNKNOWN


def itunes_content_rating(data: dict[str, object]) -> str:
    ratings = [
        data.get("trackExplicitness"),
        data.get("collectionExplicitness"),
        data.get("contentAdvisoryRating"),
    ]
    normalized = [normalize_content_rating(value) for value in ratings]
    if CONTENT_RATING_EXPLICIT in normalized:
        return CONTENT_RATING_EXPLICIT
    if CONTENT_RATING_CLEAN in normalized:
        return CONTENT_RATING_CLEAN
    if CONTENT_RATING_NOT_EXPLICIT in normalized:
        return CONTENT_RATING_NOT_EXPLICIT
    return CONTENT_RATING_UNKNOWN


def content_ratings_compatible(left: str | None, right: str | None) -> bool:
    left_rating = normalize_content_rating(left)
    right_rating = normalize_content_rating(right)
    return left_rating == right_rating or CONTENT_RATING_UNKNOWN in {left_rating, right_rating}
