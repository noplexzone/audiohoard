from __future__ import annotations

import json

import pytest

from app.services.acoustid_evidence import (
    parse_consistent_acoustid_evidence,
    parse_strict_observed_mbids,
    parse_strict_recording_evidence,
)

MBID = "11111111-1111-1111-1111-111111111111"
OTHER = "22222222-2222-2222-2222-222222222222"


def test_strict_recording_evidence_accepts_complete_unique_payload() -> None:
    assert parse_strict_recording_evidence(
        json.dumps([{"mbid": MBID, "score": 0.99}, {"mbid": OTHER, "score": 0.75}])
    ) == {MBID: 0.99, OTHER: 0.75}


@pytest.mark.parametrize(
    "payload",
    [
        [{"mbid": MBID, "score": 0.99}, "malformed"],
        [{"mbid": MBID, "score": 0.10}, {"mbid": MBID, "score": 0.99}],
        [{"mbid": MBID, "score": 0.99}, {"mbid": "not-an-mbid", "score": 0.99}],
        [{"mbid": MBID, "score": True}],
        [{"mbid": MBID, "score": float("nan")}],
        [{"mbid": MBID, "score": "0.99"}],
        [{"mbid": MBID, "score": 10**400}],
    ],
)
def test_strict_recording_evidence_rejects_entire_malformed_payload(payload: object) -> None:
    assert parse_strict_recording_evidence(json.dumps(payload)) is None


def test_strict_observed_mbids_rejects_invalid_or_duplicate_members() -> None:
    assert parse_strict_observed_mbids(json.dumps([MBID, OTHER])) == (MBID, OTHER)
    assert parse_strict_observed_mbids(json.dumps([MBID, MBID])) is None
    assert parse_strict_observed_mbids(json.dumps([MBID, "not-an-mbid"])) is None


def test_consistent_evidence_requires_exact_observed_keyset() -> None:
    observed = json.dumps([MBID, OTHER])
    assert (
        parse_consistent_acoustid_evidence(
            observed,
            json.dumps([{"mbid": MBID, "score": 0.99}]),
        )
        is None
    )
    assert (
        parse_consistent_acoustid_evidence(
            json.dumps([MBID]),
            json.dumps(
                [
                    {"mbid": MBID, "score": 0.99},
                    {"mbid": OTHER, "score": 0.98},
                ]
            ),
        )
        is None
    )
