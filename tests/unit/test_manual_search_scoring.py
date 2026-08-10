from __future__ import annotations

from app.schemas.search import SearchRequest, SearchResult
from app.services.manual_search import (
    group_ranked_results,
    rank_manual_results,
    score_manual_result,
)


def test_manual_score_rewards_exact_identity_duration_format_and_availability() -> None:
    request = SearchRequest(
        artist="Massive Attack",
        album="Mezzanine",
        track="Teardrop",
        expected_duration_sec=331,
        preferred_format="flac",
        sources=["slskd", "prowlarr"],
    )
    exact = SearchResult(
        source="slskd",
        artist="Massive Attack",
        album="Mezzanine",
        title="Teardrop",
        duration_sec=329,
        format="flac",
        metadata={"available": True, "bitrate": 1000},
    )
    weak = SearchResult(
        source="prowlarr",
        title="Massive Attack live collection",
        format="mp3",
        metadata={"available": False, "bitrate": 128},
    )
    exact_score = score_manual_result(exact, request, {"slskd": 0, "prowlarr": 1})
    weak_score = score_manual_result(weak, request, {"slskd": 0, "prowlarr": 1})
    assert exact_score.score > weak_score.score
    assert exact_score.reasons[:4] == (
        "Exact artist",
        "Exact title",
        "Exact album",
        "Duration within 2 seconds",
    )
    assert {"Preferred format", "Lossless", "Preferred source"}.issubset(exact_score.reasons)


def test_manual_score_is_deterministic_and_rejection_evidence_penalizes() -> None:
    request = SearchRequest(query="artist album", sources=["prowlarr"])
    result = SearchResult(
        source="prowlarr", title="Artist Album", metadata={"seeders": 12, "rejected": True}
    )
    first = score_manual_result(result, request, {"prowlarr": 0})
    assert first == score_manual_result(result, request, {"prowlarr": 0})
    assert "Previously rejected" in first.reasons


def test_ranking_tie_breaker_is_stable_and_exact_result_is_preserved() -> None:
    request = SearchRequest(query="same", sources=["youtube"])
    b = SearchResult(source="youtube", title="Same", url="https://example.test/b")
    a = SearchResult(source="youtube", title="Same", url="https://example.test/a")
    ranked = rank_manual_results([b, a], request, ["youtube"])
    assert [item.result.url for item in ranked] == [
        "https://example.test/a",
        "https://example.test/b",
    ]
    assert ranked[0].result is a


def test_candidate_grouping_requires_shared_stable_artifact_identity() -> None:
    request = SearchRequest(query="same", sources=["slskd", "prowlarr"])
    uncertain = rank_manual_results(
        [
            SearchResult(source="slskd", title="Same Filename.flac"),
            SearchResult(source="prowlarr", title="Same Filename.flac"),
        ],
        request,
        request.sources,
    )
    stable = rank_manual_results(
        [
            SearchResult(
                source="slskd",
                title="A",
                metadata={"artifact_namespace": "music.release", "artifact_id": "abc"},
            ),
            SearchResult(
                source="prowlarr",
                title="B",
                metadata={"artifact_namespace": "music.release", "artifact_id": "abc"},
            ),
        ],
        request,
        request.sources,
    )
    assert len(group_ranked_results(uncertain)) == 2
    grouped = group_ranked_results(stable)
    assert len(grouped) == 1
    assert [option.result.source for option in grouped[0].options] == ["slskd", "prowlarr"]
