"""Persist durable batch hydration identity and execution generations.

Revision ID: 0034
Revises: 0033
Create Date: 2026-08-26
"""

from __future__ import annotations

import hashlib
import json

import sqlalchemy as sa

from alembic import op

revision = "0034"
down_revision = "0033"
branch_labels = None
depends_on = None

_ACTIVE_STATES = "'queued', 'running', 'paused'"
_SUPPORTED_PROVIDER_IDENTITY = """
CASE
  WHEN release_identity LIKE 'provider:deezer:%'
       AND trim(substr(release_identity, 17)) <> ''
    THEN 'deezer'
  WHEN release_identity LIKE 'provider:musicbrainz:%'
       AND trim(substr(release_identity, 22)) <> ''
    THEN 'musicbrainz'
  WHEN release_identity LIKE 'provider:itunes:%'
       AND trim(substr(release_identity, 16)) <> ''
    THEN 'itunes'
END
"""
_SUPPORTED_PROVIDER_ALBUM_ID = """
CASE
  WHEN release_identity LIKE 'provider:deezer:%'
    THEN trim(substr(release_identity, 17))
  WHEN release_identity LIKE 'provider:musicbrainz:%'
    THEN trim(substr(release_identity, 22))
  WHEN release_identity LIKE 'provider:itunes:%'
    THEN trim(substr(release_identity, 16))
END
"""
_PROVIDER_IDENTITY_CHECK = (
    "(provider IS NULL AND provider_album_id IS NULL) OR "
    "(provider IS NOT NULL AND trim(provider) <> '' AND "
    "provider_album_id IS NOT NULL AND trim(provider_album_id) <> '')"
)


def upgrade() -> None:
    op.add_column(
        "discography_batch_items",
        sa.Column("provider_album_id", sa.String(length=128), nullable=True),
    )
    op.add_column(
        "discography_batch_items",
        sa.Column("execution_generation", sa.Integer(), server_default="1", nullable=False),
    )
    op.add_column(
        "discography_batch_item_jobs",
        sa.Column("generation", sa.Integer(), server_default="1", nullable=False),
    )
    op.add_column(
        "discography_batch_item_jobs",
        sa.Column("catalog_track_id", sa.Integer(), nullable=True),
    )

    bind = op.get_bind()
    bind.execute(
        sa.text(
            "UPDATE discography_batch_items AS items SET "
            "provider = (SELECT identities.provider FROM catalog_album_providers AS releases "
            "JOIN catalog_artist_identities AS identities "
            "ON identities.id = releases.artist_identity_id "
            "WHERE releases.id = items.provider_release_id), "
            "provider_album_id = (SELECT releases.provider_album_id "
            "FROM catalog_album_providers AS releases "
            "WHERE releases.id = items.provider_release_id) "
            "WHERE items.provider_release_id IS NOT NULL AND EXISTS "
            "(SELECT 1 FROM catalog_album_providers AS releases "
            "WHERE releases.id = items.provider_release_id)"
        )
    )
    bind.execute(
        sa.text(
            "UPDATE discography_batch_items SET "
            f"provider = ({_SUPPORTED_PROVIDER_IDENTITY}), "
            f"provider_album_id = ({_SUPPORTED_PROVIDER_ALBUM_ID}) "
            "WHERE (provider IS NULL OR provider_album_id IS NULL) AND ("
            "release_identity LIKE 'provider:deezer:%' OR "
            "release_identity LIKE 'provider:musicbrainz:%' OR "
            "release_identity LIKE 'provider:itunes:%')"
        )
    )
    bind.execute(
        sa.text(
            "UPDATE discography_batch_items AS items SET "
            "provider = CASE "
            "WHEN trim(albums.deezer_id) <> '' THEN 'deezer' "
            "WHEN trim(albums.mbid) <> '' THEN 'musicbrainz' "
            "WHEN trim(albums.itunes_id) <> '' THEN 'itunes' END, "
            "provider_album_id = CASE "
            "WHEN trim(albums.deezer_id) <> '' THEN trim(albums.deezer_id) "
            "WHEN trim(albums.mbid) <> '' THEN trim(albums.mbid) "
            "WHEN trim(albums.itunes_id) <> '' THEN trim(albums.itunes_id) END "
            "FROM catalog_albums AS albums, discography_batches AS batches "
            "WHERE albums.id = items.catalog_album_id "
            "AND batches.id = items.batch_id "
            "AND batches.scope_kind IN ('wanted_selected','wanted_page','wanted_all_matching') "
            "AND (items.provider IS NULL OR items.provider_album_id IS NULL)"
        )
    )
    bind.execute(
        sa.text(
            "UPDATE discography_batch_items SET provider = NULL, provider_album_id = NULL "
            "WHERE provider IS NULL OR trim(provider) = '' "
            "OR provider_album_id IS NULL OR trim(provider_album_id) = ''"
        )
    )
    # Wanted batches previously hashed catalog row IDs even though current admission
    # hashes immutable provider identities. Re-key every wanted snapshot before the
    # active-scope uniqueness fence so an upgraded active batch remains equivalent.
    wanted_batches = bind.execute(
        sa.text(
            "SELECT id, scope_json FROM discography_batches "
            "WHERE scope_kind IN ('wanted_selected','wanted_page','wanted_all_matching') "
            "ORDER BY id"
        )
    ).all()
    for batch_id, scope_json in wanted_batches:
        item_rows = bind.execute(
            sa.text(
                "SELECT id, release_identity, provider, provider_album_id "
                "FROM discography_batch_items WHERE batch_id = :batch_id ORDER BY id"
            ),
            {"batch_id": batch_id},
        ).all()
        identities: list[str] = []
        for item_id, release_identity, provider, provider_album_id in item_rows:
            if provider is not None and provider_album_id is not None:
                release_identity = (
                    f"provider:{str(provider).strip()}:{str(provider_album_id).strip()}"
                )
                bind.execute(
                    sa.text(
                        "UPDATE discography_batch_items SET release_identity = :identity "
                        "WHERE id = :item_id"
                    ),
                    {"identity": release_identity, "item_id": item_id},
                )
            identities.append(str(release_identity))
        encoded = json.dumps(identities, separators=(",", ":"))
        scope_hash = hashlib.sha256(f"{scope_json}\n{encoded}".encode()).hexdigest()
        bind.execute(
            sa.text(
                "UPDATE discography_batches SET scope_hash = :scope_hash WHERE id = :batch_id"
            ),
            {"scope_hash": scope_hash, "batch_id": batch_id},
        )

    # Preserve every historical link while choosing one representative attempt
    # per item/track for generation 1. Prefer active work so an upgraded runner
    # observes it instead of declaring the generation exhausted.
    bind.execute(
        sa.text(
            "WITH ranked AS ("
            " SELECT links.id, jobs.catalog_track_id,"
            " ROW_NUMBER() OVER ("
            " PARTITION BY links.item_id, jobs.catalog_track_id"
            " ORDER BY CASE WHEN jobs.status IN ('pending','running') THEN 0 ELSE 1 END,"
            " links.id DESC) AS position"
            " FROM discography_batch_item_jobs AS links"
            " JOIN jobs ON jobs.id = links.job_id"
            " WHERE jobs.catalog_track_id IS NOT NULL"
            ") UPDATE discography_batch_item_jobs SET catalog_track_id = ("
            " SELECT CASE WHEN ranked.position = 1 THEN ranked.catalog_track_id END"
            " FROM ranked WHERE ranked.id = discography_batch_item_jobs.id"
            ") WHERE id IN (SELECT id FROM ranked)"
        )
    )

    # Capture duplicate active scope IDs before terminalizing them. A temporary
    # relation avoids depending on mutable state or pre-existing error text.
    bind.execute(sa.text("CREATE TEMPORARY TABLE _0034_duplicate_batches(id INTEGER PRIMARY KEY)"))
    bind.execute(
        sa.text(
            "INSERT INTO _0034_duplicate_batches(id) "
            "SELECT id FROM discography_batches "
            f"WHERE state IN ({_ACTIVE_STATES}) AND id NOT IN ("
            "SELECT min(id) FROM discography_batches "
            f"WHERE state IN ({_ACTIVE_STATES}) GROUP BY scope_hash)"
        )
    )
    bind.execute(
        sa.text(
            "UPDATE discography_batches SET state = 'cancelled', "
            "completed_at = COALESCE(completed_at, CURRENT_TIMESTAMP), "
            "lease_token = NULL, heartbeat_at = NULL, "
            "error_detail = COALESCE(error_detail, "
            "'duplicate active scope cancelled by migration 0034') "
            "WHERE id IN (SELECT id FROM _0034_duplicate_batches)"
        )
    )
    bind.execute(
        sa.text(
            "UPDATE discography_batch_items SET state = 'cancelled', "
            "reason_code = COALESCE(reason_code, 'duplicate_active_scope'), "
            "completed_at = COALESCE(completed_at, CURRENT_TIMESTAMP), "
            "lease_token = NULL, heartbeat_at = NULL "
            "WHERE batch_id IN (SELECT id FROM _0034_duplicate_batches) "
            "AND state NOT IN ('complete','skipped','failed','cancelled')"
        )
    )
    bind.execute(
        sa.text(
            "WITH RECURSIVE duplicate_created(id) AS ("
            " SELECT links.job_id FROM discography_batch_item_jobs AS links"
            " JOIN discography_batch_items AS items ON items.id = links.item_id"
            " WHERE links.ownership = 'created'"
            " AND items.batch_id IN (SELECT id FROM _0034_duplicate_batches)"
            " UNION"
            " SELECT jobs.id FROM jobs JOIN duplicate_created"
            " ON jobs.parent_job_id = duplicate_created.id"
            ") UPDATE jobs SET status = 'cancelled', queue_hidden = 1"
            " WHERE id IN (SELECT id FROM duplicate_created) AND status = 'pending'"
        )
    )
    bind.execute(sa.text("DROP TABLE _0034_duplicate_batches"))

    with op.batch_alter_table("discography_batch_items") as batch:
        batch.create_check_constraint(
            "ck_discography_batch_item_provider_identity", _PROVIDER_IDENTITY_CHECK
        )
        batch.create_check_constraint(
            "ck_execution_generation_positive", "execution_generation >= 1"
        )

    with op.batch_alter_table("discography_batch_item_jobs") as batch:
        batch.drop_constraint("uq_discography_batch_item_job", type_="unique")
        batch.create_unique_constraint(
            "uq_discography_batch_item_generation_job", ["item_id", "generation", "job_id"]
        )
        batch.create_check_constraint(
            "ck_discography_batch_job_generation_positive", "generation >= 1"
        )

    op.create_index(
        "uq_discography_batch_item_generation_track",
        "discography_batch_item_jobs",
        ["item_id", "generation", "catalog_track_id"],
        unique=True,
        sqlite_where=sa.text("catalog_track_id IS NOT NULL"),
    )
    op.create_index(
        "uq_discography_batches_active_scope",
        "discography_batches",
        ["scope_hash"],
        unique=True,
        sqlite_where=sa.text(f"state IN ({_ACTIVE_STATES})"),
    )


def downgrade() -> None:
    op.drop_index("uq_discography_batches_active_scope", table_name="discography_batches")
    op.drop_index(
        "uq_discography_batch_item_generation_track",
        table_name="discography_batch_item_jobs",
    )
    bind = op.get_bind()
    bind.execute(
        sa.text(
            "WITH ranked AS ("
            " SELECT id, ROW_NUMBER() OVER ("
            " PARTITION BY item_id, job_id"
            " ORDER BY CASE WHEN ownership = 'created' THEN 0 ELSE 1 END,"
            " generation, id) AS position"
            " FROM discography_batch_item_jobs"
            ") DELETE FROM discography_batch_item_jobs"
            " WHERE id IN (SELECT id FROM ranked WHERE position > 1)"
        )
    )
    with op.batch_alter_table("discography_batch_item_jobs") as batch:
        batch.drop_constraint("ck_discography_batch_job_generation_positive", type_="check")
        batch.drop_constraint("uq_discography_batch_item_generation_job", type_="unique")
        batch.create_unique_constraint("uq_discography_batch_item_job", ["item_id", "job_id"])
        batch.drop_column("catalog_track_id")
        batch.drop_column("generation")
    with op.batch_alter_table("discography_batch_items") as batch:
        batch.drop_constraint("ck_execution_generation_positive", type_="check")
        batch.drop_constraint("ck_discography_batch_item_provider_identity", type_="check")
        batch.drop_column("execution_generation")
        batch.drop_column("provider_album_id")
