from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import sqlalchemy as sa
from alembic.config import Config

from alembic import command


def _config(database: Path) -> Config:
    cfg = Config("alembic.ini")
    cfg.set_main_option("sqlalchemy.url", f"sqlite+aiosqlite:///{database}")
    return cfg


def test_0032_backfills_and_deduplicates_exact_legacy_denied_sources(tmp_path: Path) -> None:
    database = tmp_path / "migration-0032.db"
    cfg = _config(database)
    command.upgrade(cfg, "0031")

    preserved_provenance = json.dumps(
        {
            "source": " SLSKD ",
            "username": "LegacyPeer",
            "filename": r" Album\.\Disc 1\..\01 Song.flac ",
        },
        separators=(",", ":"),
    )
    engine = sa.create_engine(f"sqlite:///{database}")
    try:
        with engine.begin() as connection:
            connection.execute(
                sa.text(
                    "INSERT INTO jobs (id, source, query) VALUES "
                    "(1, 'slskd', 'legacy'), (2, 'slskd', 'existing')"
                )
            )
            connection.execute(
                sa.text(
                    """
                    INSERT INTO tracks
                        (job_id, source, acoustid_verification_state,
                         acquisition_provenance_json)
                    VALUES
                        (1, 'slskd', 'denied', :preserved),
                        (1, 'slskd', 'denied', :legacy),
                        (1, 'slskd', 'denied', '{malformed'),
                        (1, 'slskd', 'denied',
                         '{"source":"slskd","filename":"missing-peer.flac"}'),
                        (1, 'slskd', 'denied',
                         '{"source":"tidal","username":"OtherPeer","filename":"other.flac"}'),
                        (1, 'tidal', 'denied',
                         '{"source":"slskd","username":"WrongTrackSource","filename":"wrong.flac"}'),
                        (2, 'slskd', 'denied',
                         '{"source":"slskd","username":"ExistingPeer","filename":"Folder\\\\Track.flac"}'),
                        (2, 'slskd', 'denied',
                         '{"source":"slskd","username":"ExpiredPeer","filename":"C:../Track.flac"}'),
                        (2, 'slskd', 'denied',
                         '{"source":"slskd","username":"DrivePeer","filename":"C:song.flac"}'),
                        (2, 'slskd', 'denied',
                         '{"source":"slskd","username":"DrivePeer","filename":"C:../song.flac"}'),
                        (2, 'slskd', 'denied',
                         '{"source":"slskd","username":"MalformedUnc","filename":"//server"}')
                    """
                ),
                {
                    "preserved": preserved_provenance,
                    "legacy": json.dumps(
                        {
                            "source": "slskd",
                            "username": "LegacyPeer",
                            "filename": "Album/01 Song.flac",
                        },
                        separators=(",", ":"),
                    ),
                },
            )
            connection.execute(
                sa.text(
                    """
                    INSERT INTO source_candidate_blocks
                        (provider, peer, filename, reason, blocked_until)
                    VALUES
                        ('slskd', 'ExistingPeer', 'Folder/Track.flac', 'operator', NULL),
                        (' SLSKD ', 'ExistingPeer', 'Folder/./Track.flac', 'denied', NULL),
                        (' SLSKD ', 'ExpiredPeer', 'C:folder/../../Track.flac', 'temporary',
                         :expired),
                        ('slskd', 'UnrelatedPeer', 'keep/me.flac', 'operator', :future)
                    """
                ),
                {
                    "expired": datetime.now(UTC) - timedelta(days=1),
                    "future": datetime.now(UTC) + timedelta(days=1),
                },
            )
    finally:
        engine.dispose()

    command.upgrade(cfg, "head")

    engine = sa.create_engine(f"sqlite:///{database}")
    try:
        with engine.connect() as connection:
            rows = connection.execute(
                sa.text(
                    "SELECT provider, peer, filename, reason, blocked_until "
                    "FROM source_candidate_blocks ORDER BY peer, filename"
                )
            ).all()
            provenance_after = connection.scalar(
                sa.text("SELECT acquisition_provenance_json FROM tracks WHERE id = 1")
            )
    finally:
        engine.dispose()

    assert [row[:4] for row in rows] == [
        ("slskd", "DrivePeer", "C:../song.flac", "denied"),
        ("slskd", "DrivePeer", "C:song.flac", "denied"),
        ("slskd", "ExistingPeer", "Folder/Track.flac", "denied"),
        ("slskd", "ExpiredPeer", "C:../Track.flac", "denied"),
        ("slskd", "LegacyPeer", "Album/01 Song.flac", "denied"),
        ("slskd", "UnrelatedPeer", "keep/me.flac", "operator"),
    ]
    assert all(row.blocked_until is None for row in rows[:-1])
    assert rows[-1].blocked_until is not None
    assert provenance_after == preserved_provenance

    command.downgrade(cfg, "0031")
    command.upgrade(cfg, "head")

    engine = sa.create_engine(f"sqlite:///{database}")
    try:
        with engine.connect() as connection:
            assert connection.scalar(sa.text("SELECT count(*) FROM source_candidate_blocks")) == 6
    finally:
        engine.dispose()
