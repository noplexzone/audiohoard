from __future__ import annotations

from pathlib import Path

import pytest
import sqlalchemy as sa
from alembic.config import Config
from sqlalchemy.exc import IntegrityError

from alembic import command

TABLES = {"discography_batches", "discography_batch_items", "discography_batch_item_jobs"}


def _config(database: Path) -> Config:
    cfg = Config("alembic.ini")
    cfg.set_main_option("sqlalchemy.url", f"sqlite+aiosqlite:///{database}")
    return cfg


def _engine(database: Path) -> sa.Engine:
    engine = sa.create_engine(f"sqlite:///{database}")

    @sa.event.listens_for(engine, "connect")
    def _fk_on(connection: object, _record: object) -> None:
        cursor = connection.cursor()  # type: ignore[attr-defined]
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()

    return engine


def _seed(connection: sa.Connection) -> tuple[int, int, int, int]:
    batch = connection.execute(
        sa.text(
            "INSERT INTO discography_batches(scope_kind,scope_json,scope_hash) "
            "VALUES ('artist','{}',:hash)"
        ),
        {"hash": "a" * 64},
    ).lastrowid
    artist = connection.execute(
        sa.text("INSERT INTO catalog_artists(name) VALUES ('Artist')")
    ).lastrowid
    album = connection.execute(
        sa.text("INSERT INTO catalog_albums(artist_id,title) VALUES (:artist,'Album')"),
        {"artist": artist},
    ).lastrowid
    identity = connection.execute(
        sa.text(
            "INSERT INTO catalog_artist_identities(artist_id,provider,provider_artist_id,name) "
            "VALUES (:artist,'deezer','artist-1','Artist')"
        ),
        {"artist": artist},
    ).lastrowid
    providers = [
        connection.execute(
            sa.text(
                "INSERT INTO catalog_album_providers"
                "(artist_identity_id,catalog_album_id,provider_album_id,title) "
                "VALUES (:identity,:album,:release,'Album')"
            ),
            {"identity": identity, "album": album, "release": release},
        ).lastrowid
        for release in ("release-1", "release-2")
    ]
    assert all(value is not None for value in (batch, album, *providers))
    return int(batch), int(album), int(providers[0]), int(providers[1])


def test_0033_schema_contract(tmp_path: Path) -> None:
    database = tmp_path / "schema.db"
    command.upgrade(_config(database), "head")
    engine = _engine(database)
    try:
        inspector = sa.inspect(engine)
        assert TABLES.issubset(inspector.get_table_names())
        assert {c["name"] for c in inspector.get_columns("discography_batches")} == {
            "id",
            "scope_kind",
            "scope_json",
            "scope_hash",
            "state",
            "matching_count",
            "complete_count",
            "active_count",
            "hydration_required_count",
            "missing_count",
            "skipped_count",
            "estimated_job_count",
            "lease_token",
            "heartbeat_at",
            "started_at",
            "completed_at",
            "created_at",
            "updated_at",
            "error_detail",
        }
        assert {c["name"] for c in inspector.get_columns("discography_batch_items")} == {
            "id",
            "batch_id",
            "release_identity",
            "provider_release_id",
            "catalog_album_id",
            "artist_name",
            "release_title",
            "release_year",
            "release_kind",
            "provider",
            "expected_track_count",
            "state",
            "reason_code",
            "target_count",
            "active_count",
            "skipped_count",
            "estimated_job_count",
            "attempt_count",
            "lease_token",
            "heartbeat_at",
            "error_detail",
            "created_at",
            "updated_at",
            "started_at",
            "completed_at",
        }
        assert {c["name"] for c in inspector.get_columns("discography_batch_item_jobs")} == {
            "id",
            "item_id",
            "job_id",
            "ownership",
            "created_at",
        }
        assert {i["name"] for i in inspector.get_indexes("discography_batches")} == {
            "ix_discography_batches_state",
            "ix_discography_batches_created_at",
        }
        item_fks = {
            (tuple(f["constrained_columns"]), f["referred_table"], f["options"].get("ondelete"))
            for f in inspector.get_foreign_keys("discography_batch_items")
        }
        assert item_fks == {
            (("batch_id",), "discography_batches", "CASCADE"),
            (("provider_release_id",), "catalog_album_providers", "SET NULL"),
            (("catalog_album_id",), "catalog_albums", "SET NULL"),
        }
        link_fks = {
            (tuple(f["constrained_columns"]), f["referred_table"], f["options"].get("ondelete"))
            for f in inspector.get_foreign_keys("discography_batch_item_jobs")
        }
        assert link_fks == {
            (("item_id",), "discography_batch_items", "CASCADE"),
            (("job_id",), "jobs", "RESTRICT"),
        }
        assert {
            check["name"] for check in inspector.get_check_constraints("discography_batches")
        } >= {
            "discographyscopekind",
            "discographybatchstate",
            "ck_matching_count_nonnegative",
            "ck_complete_count_nonnegative",
            "ck_active_count_nonnegative",
            "ck_hydration_required_count_nonnegative",
            "ck_missing_count_nonnegative",
            "ck_skipped_count_nonnegative",
            "ck_estimated_job_count_nonnegative",
        }
        assert {
            check["name"] for check in inspector.get_check_constraints("discography_batch_items")
        } >= {
            "discographybatchitemstate",
            "ck_discography_batch_item_identity",
            "ck_expected_track_count_nonnegative",
            "ck_target_count_nonnegative",
            "ck_active_count_nonnegative",
            "ck_skipped_count_nonnegative",
            "ck_estimated_job_count_nonnegative",
            "ck_attempt_count_nonnegative",
        }
        with engine.connect() as connection:
            ddl = {
                name: connection.scalar(
                    sa.text("SELECT sql FROM sqlite_master WHERE type='table' AND name=:name"),
                    {"name": name},
                )
                or ""
                for name in TABLES
            }
            indexes = {
                row.name: row.sql
                for row in connection.execute(
                    sa.text(
                        "SELECT name,sql FROM sqlite_master WHERE type='index' "
                        "AND tbl_name='discography_batch_items' AND sql IS NOT NULL"
                    )
                ).mappings()
            }
        assert (
            "scope_kind IN ('artist', 'wanted_selected', 'wanted_page', 'wanted_all_matching')"
            in ddl["discography_batches"]
        )
        assert "'completed_with_failures'" in ddl["discography_batches"]
        assert "trim(release_identity) <> ''" in ddl["discography_batch_items"]
        assert "ownership IN ('created', 'observed')" in ddl["discography_batch_item_jobs"]
        assert indexes["uq_discography_batch_items_provider_release"].endswith(
            "WHERE provider_release_id IS NOT NULL"
        )
        assert indexes["uq_discography_batch_items_catalog_album"].endswith(
            "WHERE provider_release_id IS NULL AND catalog_album_id IS NOT NULL "
            "AND release_identity LIKE 'catalog_album:%'"
        )
        assert any(
            u["column_names"] == ["batch_id", "release_identity"]
            for u in inspector.get_unique_constraints("discography_batch_items")
        )
        assert any(
            u["column_names"] == ["item_id", "job_id"]
            for u in inspector.get_unique_constraints("discography_batch_item_jobs")
        )
    finally:
        engine.dispose()


def test_0033_rejects_invalid_values_and_empty_identity(tmp_path: Path) -> None:
    database = tmp_path / "invalid.db"
    command.upgrade(_config(database), "head")
    engine = _engine(database)
    try:
        with engine.begin() as connection:
            for sql in (
                "INSERT INTO discography_batches(scope_kind,scope_json,scope_hash,state) "
                f"VALUES ('artist','{{}}','{'a' * 64}','invalid')",
                "INSERT INTO discography_batches(scope_kind,scope_json,scope_hash,matching_count) "
                f"VALUES ('artist','{{}}','{'a' * 64}',-1)",
            ):
                with pytest.raises(IntegrityError):
                    connection.execute(sa.text(sql))
            batch, album, _, _ = _seed(connection)
            for sql in (
                "INSERT INTO discography_batch_items(batch_id,artist_name,release_title) "
                f"VALUES ({batch},'Artist','Album')",
                "INSERT INTO discography_batch_items"
                "(batch_id,release_identity,catalog_album_id,artist_name,release_title,state) "
                f"VALUES ({batch},'catalog_album:{album}',{album},'Artist','Album','invalid')",
                "INSERT INTO discography_batch_items"
                "(batch_id,release_identity,catalog_album_id,artist_name,release_title,"
                "target_count) "
                f"VALUES ({batch},'catalog_album:{album}',{album},'Artist','Album',-1)",
                "INSERT INTO discography_batch_items"
                "(batch_id,release_identity,catalog_album_id,artist_name,release_title,"
                "expected_track_count) "
                f"VALUES ({batch},'catalog_album:{album}',{album},'Artist','Album',-1)",
                "INSERT INTO discography_batch_items"
                "(batch_id,release_identity,catalog_album_id,artist_name,release_title) "
                f"VALUES ({batch},'   ',{album},'Artist','Album')",
            ):
                with pytest.raises(IntegrityError):
                    connection.execute(sa.text(sql))
    finally:
        engine.dispose()


def test_0033_overlap_uniqueness_and_job_ownership(tmp_path: Path) -> None:
    database = tmp_path / "semantics.db"
    command.upgrade(_config(database), "head")
    engine = _engine(database)
    try:
        with engine.begin() as connection:
            batch, album, provider_one, provider_two = _seed(connection)
            insert = sa.text(
                "INSERT INTO discography_batch_items"
                "(batch_id,release_identity,provider_release_id,catalog_album_id,"
                "artist_name,release_title) "
                "VALUES (:batch,:release_identity,:provider,:album,'Artist','Album')"
            )
            for provider in (provider_one, provider_two):
                connection.execute(
                    insert,
                    {
                        "batch": batch,
                        "release_identity": f"provider:deezer:release-{provider}",
                        "provider": provider,
                        "album": album,
                    },
                )
            with pytest.raises(IntegrityError):
                connection.execute(
                    insert,
                    {
                        "batch": batch,
                        "release_identity": "provider:deezer:other-id",
                        "provider": provider_one,
                        "album": album,
                    },
                )
            connection.execute(
                insert,
                {
                    "batch": batch,
                    "release_identity": f"catalog_album:{album}",
                    "provider": None,
                    "album": album,
                },
            )
            with pytest.raises(IntegrityError):
                connection.execute(
                    insert,
                    {
                        "batch": batch,
                        "release_identity": f"catalog_album:{album}:duplicate",
                        "provider": None,
                        "album": album,
                    },
                )
            with pytest.raises(IntegrityError):
                connection.execute(
                    insert,
                    {
                        "batch": batch,
                        "release_identity": f"provider:deezer:release-{provider_two}",
                        "provider": None,
                        "album": None,
                    },
                )
            item = connection.scalar(sa.text("SELECT min(id) FROM discography_batch_items"))
            job = connection.execute(
                sa.text("INSERT INTO jobs(source,query) VALUES ('test','owned')")
            ).lastrowid
            with pytest.raises(IntegrityError):
                connection.execute(
                    sa.text(
                        "INSERT INTO discography_batch_item_jobs(item_id,job_id,ownership) "
                        "VALUES (:item,:job,'invalid')"
                    ),
                    {"item": item, "job": job},
                )
            connection.execute(
                sa.text(
                    "INSERT INTO discography_batch_item_jobs(item_id,job_id,ownership) "
                    "VALUES (:item,:job,'created')"
                ),
                {"item": item, "job": job},
            )
            with pytest.raises(IntegrityError):
                connection.execute(
                    sa.text(
                        "INSERT INTO discography_batch_item_jobs(item_id,job_id,ownership) "
                        "VALUES (:item,:job,'observed')"
                    ),
                    {"item": item, "job": job},
                )
            with pytest.raises(IntegrityError):
                connection.execute(sa.text("DELETE FROM jobs WHERE id=:job"), {"job": job})
            connection.execute(
                sa.text("DELETE FROM discography_batches WHERE id=:batch"), {"batch": batch}
            )
            assert connection.scalar(sa.text("SELECT count(*) FROM discography_batch_items")) == 0
            assert (
                connection.scalar(sa.text("SELECT count(*) FROM discography_batch_item_jobs")) == 0
            )
            assert connection.scalar(sa.text("SELECT count(*) FROM jobs")) == 1
    finally:
        engine.dispose()


def test_0033_catalog_cleanup_preserves_durable_item_identity(tmp_path: Path) -> None:
    database = tmp_path / "catalog-cleanup.db"
    command.upgrade(_config(database), "head")
    engine = _engine(database)
    try:
        with engine.begin() as connection:
            batch, album, provider_one, provider_two = _seed(connection)
            insert = sa.text(
                "INSERT INTO discography_batch_items"
                "(batch_id,release_identity,provider_release_id,catalog_album_id,"
                "artist_name,release_title,expected_track_count) VALUES "
                "(:batch,:identity,:provider,:album,'Artist','Album',:expected)"
            )
            provider_only = connection.execute(
                insert,
                {
                    "batch": batch,
                    "identity": "provider:deezer:release-1",
                    "provider": provider_one,
                    "album": None,
                    "expected": 7,
                },
            ).lastrowid
            dual = connection.execute(
                insert,
                {
                    "batch": batch,
                    "identity": "provider:deezer:release-2",
                    "provider": provider_two,
                    "album": album,
                    "expected": 8,
                },
            ).lastrowid
            canonical = connection.execute(
                insert,
                {
                    "batch": batch,
                    "identity": f"catalog_album:{album}",
                    "provider": None,
                    "album": album,
                    "expected": 9,
                },
            ).lastrowid

            connection.execute(
                sa.text("DELETE FROM catalog_album_providers WHERE id IN (:one,:two)"),
                {"one": provider_one, "two": provider_two},
            )
            rows = {
                row.id: row
                for row in connection.execute(
                    sa.text(
                        "SELECT id,release_identity,provider_release_id,catalog_album_id,"
                        "expected_track_count FROM discography_batch_items"
                    )
                ).mappings()
            }
            assert rows[provider_only] == {
                "id": provider_only,
                "release_identity": "provider:deezer:release-1",
                "provider_release_id": None,
                "catalog_album_id": None,
                "expected_track_count": 7,
            }
            assert rows[dual]["release_identity"] == "provider:deezer:release-2"
            assert rows[dual]["provider_release_id"] is None
            assert rows[dual]["catalog_album_id"] == album
            assert rows[dual]["expected_track_count"] == 8
            assert rows[canonical]["catalog_album_id"] == album
            assert rows[canonical]["expected_track_count"] == 9

            connection.execute(
                sa.text("DELETE FROM catalog_albums WHERE id=:album"), {"album": album}
            )
            remaining = (
                connection.execute(
                    sa.text(
                        "SELECT id,release_identity,provider_release_id,catalog_album_id,"
                        "expected_track_count FROM discography_batch_items ORDER BY id"
                    )
                )
                .mappings()
                .all()
            )
            assert [row["release_identity"] for row in remaining] == [
                "provider:deezer:release-1",
                "provider:deezer:release-2",
                f"catalog_album:{album}",
            ]
            assert all(row["provider_release_id"] is None for row in remaining)
            assert all(row["catalog_album_id"] is None for row in remaining)
            assert [row["expected_track_count"] for row in remaining] == [7, 8, 9]
    finally:
        engine.dispose()


def test_0033_fresh_upgrade_downgrade_reupgrade(tmp_path: Path) -> None:
    database = tmp_path / "roundtrip.db"
    cfg = _config(database)
    command.upgrade(cfg, "head")
    command.downgrade(cfg, "0032")
    engine = _engine(database)
    try:
        assert not TABLES.intersection(sa.inspect(engine).get_table_names())
        assert "jobs" in sa.inspect(engine).get_table_names()
    finally:
        engine.dispose()
    command.upgrade(cfg, "head")
    engine = _engine(database)
    try:
        assert TABLES.issubset(sa.inspect(engine).get_table_names())
    finally:
        engine.dispose()
