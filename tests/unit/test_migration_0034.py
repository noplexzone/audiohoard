from __future__ import annotations

import hashlib
import json
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


def test_0034_backfills_identity_generation_and_roundtrips(tmp_path: Path) -> None:
    database = tmp_path / "migration-0034.db"
    cfg = _config(database)
    command.upgrade(cfg, "0033")
    engine = _engine(database)
    with engine.begin() as connection:
        artist = connection.execute(
            sa.text("INSERT INTO catalog_artists(name) VALUES ('Artist')")
        ).lastrowid
        identity = connection.execute(
            sa.text(
                "INSERT INTO catalog_artist_identities"
                "(artist_id,provider,provider_artist_id,name) "
                "VALUES (:artist,'deezer','artist-1','Artist')"
            ),
            {"artist": artist},
        ).lastrowid
        album = connection.execute(
            sa.text(
                "INSERT INTO catalog_albums"
                "(artist_id,title,deezer_id,mbid,itunes_id) "
                "VALUES (:artist,'Album','dz-1','00000000-0000-0000-0000-000000000001','it-1')"
            ),
            {"artist": artist},
        ).lastrowid
        track = connection.execute(
            sa.text(
                "INSERT INTO catalog_album_tracks(album_id,disc,position,title) "
                "VALUES (:album,1,1,'Track')"
            ),
            {"album": album},
        ).lastrowid
        provider_release = connection.execute(
            sa.text(
                "INSERT INTO catalog_album_providers"
                "(artist_identity_id,catalog_album_id,provider_album_id,title) "
                "VALUES (:identity,:album,'linked-release','Album')"
            ),
            {"identity": identity, "album": album},
        ).lastrowid
        wanted_batch = connection.execute(
            sa.text(
                "INSERT INTO discography_batches(scope_kind,scope_json,scope_hash,state) "
                "VALUES ('wanted_selected','{}',:hash,'queued')"
            ),
            {"hash": "a" * 64},
        ).lastrowid
        duplicate_batch = connection.execute(
            sa.text(
                "INSERT INTO discography_batches"
                "(scope_kind,scope_json,scope_hash,state,error_detail) "
                "VALUES ('wanted_selected','{}',:hash,'queued','pre-existing diagnostic')"
            ),
            {"hash": "a" * 64},
        ).lastrowid
        artist_batch = connection.execute(
            sa.text(
                "INSERT INTO discography_batches(scope_kind,scope_json,scope_hash) "
                "VALUES ('artist','{}',:hash)"
            ),
            {"hash": "b" * 64},
        ).lastrowid
        insert_item = sa.text(
            "INSERT INTO discography_batch_items"
            "(batch_id,release_identity,provider_release_id,catalog_album_id,"
            "artist_name,release_title,provider) VALUES "
            "(:batch,:release_identity,:provider_release,:album,'Artist','Album',:provider)"
        )
        linked_item = connection.execute(
            insert_item,
            {
                "batch": artist_batch,
                "release_identity": "legacy-linked",
                "provider_release": provider_release,
                "album": album,
                "provider": "stale",
            },
        ).lastrowid
        exact_item = connection.execute(
            insert_item,
            {
                "batch": artist_batch,
                "release_identity": "provider:musicbrainz:exact-mbid",
                "provider_release": None,
                "album": None,
                "provider": None,
            },
        ).lastrowid
        wanted_item = connection.execute(
            insert_item,
            {
                "batch": wanted_batch,
                "release_identity": f"catalog_album:{album}",
                "provider_release": None,
                "album": album,
                "provider": None,
            },
        ).lastrowid
        duplicate_item = connection.execute(
            insert_item,
            {
                "batch": duplicate_batch,
                "release_identity": f"catalog_album:{album}",
                "provider_release": None,
                "album": album,
                "provider": None,
            },
        ).lastrowid
        unresolved_item = connection.execute(
            insert_item,
            {
                "batch": artist_batch,
                "release_identity": "legacy-unresolved",
                "provider_release": None,
                "album": None,
                "provider": "legacy",
            },
        ).lastrowid
        job = connection.execute(
            sa.text(
                "INSERT INTO jobs(source,query,catalog_album_id,catalog_track_id) "
                "VALUES ('test','track',:album,:track)"
            ),
            {"album": album, "track": track},
        ).lastrowid
        connection.execute(
            sa.text(
                "INSERT INTO discography_batch_item_jobs(item_id,job_id,ownership) "
                "VALUES (:item,:job,'created')"
            ),
            {"item": wanted_item, "job": job},
        )
        terminal_job = connection.execute(
            sa.text(
                "INSERT INTO jobs(source,query,status,catalog_album_id,catalog_track_id) "
                "VALUES ('test','duplicate terminal','done',:album,:track)"
            ),
            {"album": album, "track": track},
        ).lastrowid
        connection.execute(
            sa.text(
                "INSERT INTO discography_batch_item_jobs(item_id,job_id,ownership) "
                "VALUES (:item,:job,'created')"
            ),
            {"item": wanted_item, "job": terminal_job},
        )

        duplicate_job = connection.execute(
            sa.text(
                "INSERT INTO jobs(source,query,status,catalog_album_id,catalog_track_id) "
                "VALUES ('test','duplicate batch pending','pending',:album,:track)"
            ),
            {"album": album, "track": track},
        ).lastrowid
        connection.execute(
            sa.text(
                "INSERT INTO discography_batch_item_jobs(item_id,job_id,ownership) "
                "VALUES (:item,:job,'created')"
            ),
            {"item": duplicate_item, "job": duplicate_job},
        )

    command.upgrade(cfg, "head")
    engine.dispose()
    engine = _engine(database)
    try:
        inspector = sa.inspect(engine)
        item_columns = {
            column["name"] for column in inspector.get_columns("discography_batch_items")
        }
        link_columns = {
            column["name"] for column in inspector.get_columns("discography_batch_item_jobs")
        }
        assert {"provider_album_id", "execution_generation"} <= item_columns
        assert {"generation", "catalog_track_id"} <= link_columns
        assert "uq_discography_batches_active_scope" in {
            index["name"] for index in inspector.get_indexes("discography_batches")
        }
        with engine.begin() as connection:
            rows = {
                row.id: (row.provider, row.provider_album_id, row.execution_generation)
                for row in connection.execute(
                    sa.text(
                        "SELECT id,provider,provider_album_id,execution_generation "
                        "FROM discography_batch_items"
                    )
                ).mappings()
            }
            assert rows[linked_item] == ("deezer", "linked-release", 1)
            assert rows[exact_item] == ("musicbrainz", "exact-mbid", 1)
            assert rows[wanted_item] == ("deezer", "dz-1", 1)
            assert rows[unresolved_item] == (None, None, 1)
            expected_identity = "provider:deezer:dz-1"
            expected_hash = hashlib.sha256(
                f"{{}}\n{json.dumps([expected_identity], separators=(',', ':'))}".encode()
            ).hexdigest()
            wanted_identity = connection.execute(
                sa.text("SELECT release_identity FROM discography_batch_items WHERE id=:id"),
                {"id": wanted_item},
            ).scalar_one()
            wanted_hash = connection.execute(
                sa.text("SELECT scope_hash FROM discography_batches WHERE id=:id"),
                {"id": wanted_batch},
            ).scalar_one()
            assert wanted_identity == expected_identity
            assert wanted_hash == expected_hash
            duplicate_state = connection.execute(
                sa.text("SELECT state FROM discography_batches WHERE id=:id"),
                {"id": duplicate_batch},
            ).scalar_one()
            duplicate_job_state = connection.execute(
                sa.text("SELECT status,queue_hidden FROM jobs WHERE id=:id"),
                {"id": duplicate_job},
            ).one()
            duplicate_item_state = connection.execute(
                sa.text("SELECT state FROM discography_batch_items WHERE id=:id"),
                {"id": duplicate_item},
            ).scalar_one()
            duplicate_error = connection.execute(
                sa.text("SELECT error_detail FROM discography_batches WHERE id=:id"),
                {"id": duplicate_batch},
            ).scalar_one()
            assert duplicate_state == "cancelled"
            assert duplicate_item_state == "cancelled"
            assert duplicate_error == "pre-existing diagnostic"
            assert tuple(duplicate_job_state) == ("cancelled", 1)
            links = {
                row.job_id: (row.generation, row.catalog_track_id)
                for row in connection.execute(
                    sa.text(
                        "SELECT job_id,generation,catalog_track_id "
                        "FROM discography_batch_item_jobs"
                    )
                ).mappings()
            }
            assert links[job] == (1, track)
            assert links[terminal_job] == (1, None)
            connection.execute(
                sa.text(
                    "INSERT INTO discography_batch_item_jobs"
                    "(item_id,job_id,ownership,generation,catalog_track_id) "
                    "VALUES (:item,:job,'observed',2,NULL)"
                ),
                {"item": wanted_item, "job": job},
            )
            with pytest.raises(IntegrityError):
                connection.execute(
                    sa.text(
                        "UPDATE discography_batch_items SET provider='deezer',"
                        "provider_album_id=NULL WHERE id=:id"
                    ),
                    {"id": wanted_item},
                )
            with pytest.raises(IntegrityError):
                connection.execute(
                    sa.text(
                        "UPDATE discography_batch_items SET execution_generation=0 WHERE id=:id"
                    ),
                    {"id": wanted_item},
                )
    finally:
        engine.dispose()

    command.downgrade(cfg, "0033")
    engine = _engine(database)
    try:
        assert "provider_album_id" not in {
            column["name"] for column in sa.inspect(engine).get_columns("discography_batch_items")
        }
        with engine.connect() as connection:
            collapsed = connection.execute(
                sa.text(
                    "SELECT ownership FROM discography_batch_item_jobs "
                    "WHERE item_id=:item AND job_id=:job"
                ),
                {"item": wanted_item, "job": job},
            ).all()
            assert collapsed == [("created",)]
    finally:
        engine.dispose()
    command.upgrade(cfg, "head")
