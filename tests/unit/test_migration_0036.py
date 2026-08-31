from __future__ import annotations

from pathlib import Path

import pytest
import sqlalchemy as sa
from alembic.config import Config
from sqlalchemy.exc import IntegrityError

from alembic import command


def _config(database: Path) -> Config:
    cfg = Config("alembic.ini")
    cfg.set_main_option("sqlalchemy.url", f"sqlite+aiosqlite:///{database}")
    return cfg


def _engine(database: Path) -> sa.Engine:
    return sa.create_engine(f"sqlite:///{database}")


def _insert_job(
    connection: sa.Connection, *, query: str, album_id: int, track_id: int | None
) -> int:
    result = connection.execute(
        sa.text(
            "INSERT INTO jobs(source,query,catalog_album_id,catalog_track_id) "
            "VALUES ('test',:query,:album_id,:track_id)"
        ),
        {"query": query, "album_id": album_id, "track_id": track_id},
    )
    assert result.lastrowid is not None
    return result.lastrowid


def test_0036_release_first_schema_preserves_links_and_roundtrips(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("DATABASE_URL", raising=False)
    database = tmp_path / "migration-0036.db"
    cfg = _config(database)
    command.upgrade(cfg, "0035")
    engine = _engine(database)
    with engine.begin() as connection:
        artist_id = connection.execute(
            sa.text("INSERT INTO catalog_artists(name) VALUES ('Artist')")
        ).lastrowid
        album_id = connection.execute(
            sa.text("INSERT INTO catalog_albums(artist_id,title) VALUES (:artist,'Album')"),
            {"artist": artist_id},
        ).lastrowid
        other_album_id = connection.execute(
            sa.text("INSERT INTO catalog_albums(artist_id,title) VALUES (:artist,'Other')"),
            {"artist": artist_id},
        ).lastrowid
        track_id = connection.execute(
            sa.text(
                "INSERT INTO catalog_album_tracks(album_id,disc,position,title) "
                "VALUES (:album,1,1,'Track')"
            ),
            {"album": album_id},
        ).lastrowid
        batch_id = connection.execute(
            sa.text(
                "INSERT INTO discography_batches(scope_kind,scope_json,scope_hash) "
                "VALUES ('artist','{}',:hash)"
            ),
            {"hash": "a" * 64},
        ).lastrowid
        item_id = connection.execute(
            sa.text(
                "INSERT INTO discography_batch_items"
                "(batch_id,release_identity,catalog_album_id,artist_name,release_title) "
                "VALUES (:batch,'catalog_album:1',:album,'Artist','Album')"
            ),
            {"batch": batch_id, "album": album_id},
        ).lastrowid
        assert album_id is not None
        assert other_album_id is not None
        assert track_id is not None
        assert item_id is not None
        existing_job_id = _insert_job(
            connection, query="existing", album_id=album_id, track_id=track_id
        )
        connection.execute(
            sa.text(
                "INSERT INTO discography_batch_item_jobs"
                "(item_id,job_id,generation,catalog_track_id,ownership) "
                "VALUES (:item,:job,1,:track,'created')"
            ),
            {"item": item_id, "job": existing_job_id, "track": track_id},
        )
    engine.dispose()

    command.upgrade(cfg, "head")
    engine = _engine(database)
    inspector = sa.inspect(engine)
    columns = {
        column["name"]: column for column in inspector.get_columns("discography_batch_item_jobs")
    }
    assert columns["role"]["nullable"] is False
    assert "legacy_track" in str(columns["role"]["default"])
    checks = {
        check["name"]: check
        for check in inspector.get_check_constraints("discography_batch_item_jobs")
    }
    assert "discographybatchjobrole" in checks
    assert "ck_discography_batch_job_role_track" in checks
    indexes = {
        index["name"]: index for index in inspector.get_indexes("discography_batch_item_jobs")
    }
    root_index = indexes["uq_discography_batch_item_generation_release_root"]
    assert root_index["unique"] == 1
    assert root_index["column_names"] == ["item_id", "generation"]
    assert "catalog_release_acquisition_claims" in inspector.get_table_names()
    claim_columns = {
        column["name"]: column
        for column in inspector.get_columns("catalog_release_acquisition_claims")
    }
    assert set(claim_columns) == {"catalog_album_id", "job_id", "created_at"}
    assert inspector.get_pk_constraint("catalog_release_acquisition_claims")[
        "constrained_columns"
    ] == ["catalog_album_id"]
    claim_uniques = {
        constraint["name"]: constraint
        for constraint in inspector.get_unique_constraints("catalog_release_acquisition_claims")
    }
    assert claim_uniques["uq_catalog_release_acquisition_claim_job"]["column_names"] == ["job_id"]
    claim_foreign_keys = {
        tuple(foreign_key["constrained_columns"]): foreign_key
        for foreign_key in inspector.get_foreign_keys("catalog_release_acquisition_claims")
    }
    assert claim_foreign_keys[("catalog_album_id",)]["referred_table"] == "catalog_albums"
    assert claim_foreign_keys[("catalog_album_id",)]["options"] == {"ondelete": "CASCADE"}
    assert claim_foreign_keys[("job_id",)]["referred_table"] == "jobs"
    assert claim_foreign_keys[("job_id",)]["options"] == {"ondelete": "CASCADE"}

    with engine.begin() as connection:
        preserved = connection.execute(
            sa.text(
                "SELECT role,catalog_track_id FROM discography_batch_item_jobs WHERE job_id=:job"
            ),
            {"job": existing_job_id},
        ).one()
        assert tuple(preserved) == ("legacy_track", track_id)

        root_job_id = _insert_job(connection, query="root", album_id=album_id, track_id=None)
        connection.execute(
            sa.text(
                "INSERT INTO discography_batch_item_jobs"
                "(item_id,job_id,generation,catalog_track_id,ownership,role) "
                "VALUES (:item,:job,1,NULL,'created','release_root')"
            ),
            {"item": item_id, "job": root_job_id},
        )
        duplicate_root_job_id = _insert_job(
            connection, query="duplicate root", album_id=album_id, track_id=None
        )
        with pytest.raises(IntegrityError):
            connection.execute(
                sa.text(
                    "INSERT INTO discography_batch_item_jobs"
                    "(item_id,job_id,generation,catalog_track_id,ownership,role) "
                    "VALUES (:item,:job,1,NULL,'created','release_root')"
                ),
                {"item": item_id, "job": duplicate_root_job_id},
            )

        invalid_root_job_id = _insert_job(
            connection, query="invalid root", album_id=album_id, track_id=track_id
        )
        with pytest.raises(IntegrityError):
            connection.execute(
                sa.text(
                    "INSERT INTO discography_batch_item_jobs"
                    "(item_id,job_id,generation,catalog_track_id,ownership,role) "
                    "VALUES (:item,:job,2,:track,'created','release_root')"
                ),
                {"item": item_id, "job": invalid_root_job_id, "track": track_id},
            )

        invalid_fallback_job_id = _insert_job(
            connection, query="invalid fallback", album_id=album_id, track_id=None
        )
        with pytest.raises(IntegrityError):
            connection.execute(
                sa.text(
                    "INSERT INTO discography_batch_item_jobs"
                    "(item_id,job_id,generation,catalog_track_id,ownership,role) "
                    "VALUES (:item,:job,2,NULL,'created','track_fallback')"
                ),
                {"item": item_id, "job": invalid_fallback_job_id},
            )

        legacy_with_track_job_id = _insert_job(
            connection, query="legacy track", album_id=album_id, track_id=track_id
        )
        legacy_without_track_job_id = _insert_job(
            connection, query="legacy album", album_id=album_id, track_id=None
        )
        connection.execute(
            sa.text(
                "INSERT INTO discography_batch_item_jobs"
                "(item_id,job_id,generation,catalog_track_id,ownership,role) VALUES "
                "(:item,:with_track,2,:track,'observed','legacy_track'),"
                "(:item,:without_track,3,NULL,'observed','legacy_track')"
            ),
            {
                "item": item_id,
                "with_track": legacy_with_track_job_id,
                "without_track": legacy_without_track_job_id,
                "track": track_id,
            },
        )

        claim_job_id = _insert_job(
            connection, query="claim owner", album_id=album_id, track_id=None
        )
        other_claim_job_id = _insert_job(
            connection, query="other claim", album_id=other_album_id, track_id=None
        )
        connection.execute(
            sa.text(
                "INSERT INTO catalog_release_acquisition_claims(catalog_album_id,job_id) "
                "VALUES (:album,:job)"
            ),
            {"album": album_id, "job": claim_job_id},
        )
        with pytest.raises(IntegrityError):
            connection.execute(
                sa.text(
                    "INSERT INTO catalog_release_acquisition_claims(catalog_album_id,job_id) "
                    "VALUES (:album,:job)"
                ),
                {"album": album_id, "job": other_claim_job_id},
            )
        with pytest.raises(IntegrityError):
            connection.execute(
                sa.text(
                    "INSERT INTO catalog_release_acquisition_claims(catalog_album_id,job_id) "
                    "VALUES (:album,:job)"
                ),
                {"album": other_album_id, "job": claim_job_id},
            )
    engine.dispose()

    command.downgrade(cfg, "0035")
    engine = _engine(database)
    downgraded = sa.inspect(engine)
    assert "role" not in {
        column["name"] for column in downgraded.get_columns("discography_batch_item_jobs")
    }
    assert "catalog_release_acquisition_claims" not in downgraded.get_table_names()
    assert "uq_discography_batch_item_generation_release_root" not in {
        index["name"] for index in downgraded.get_indexes("discography_batch_item_jobs")
    }
    engine.dispose()

    command.upgrade(cfg, "head")
    engine = _engine(database)
    reupgraded = sa.inspect(engine)
    assert "role" in {
        column["name"] for column in reupgraded.get_columns("discography_batch_item_jobs")
    }
    assert "catalog_release_acquisition_claims" in reupgraded.get_table_names()
    with engine.connect() as connection:
        # Downgrade to 0035 cannot encode release-first roles. The row survives,
        # and re-upgrade deliberately classifies it as legacy compatibility work.
        assert (
            connection.scalar(
                sa.text("SELECT role FROM discography_batch_item_jobs WHERE job_id=:job"),
                {"job": root_job_id},
            )
            == "legacy_track"
        )
    engine.dispose()
