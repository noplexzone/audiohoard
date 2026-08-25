"""Add durable scoped discography batches.

Revision ID: 0033
Revises: 0032
Create Date: 2026-08-24
"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "0033"
down_revision = "0032"
branch_labels = None
depends_on = None

_SCOPE_KIND = sa.Enum(
    "artist",
    "wanted_selected",
    "wanted_page",
    "wanted_all_matching",
    name="discographyscopekind",
    native_enum=False,
    create_constraint=True,
)
_BATCH_STATE = sa.Enum(
    "preview",
    "queued",
    "running",
    "paused",
    "completed",
    "completed_with_failures",
    "cancelled",
    name="discographybatchstate",
    native_enum=False,
    create_constraint=True,
)
_ITEM_STATE = sa.Enum(
    "preview",
    "pending",
    "hydrating",
    "expanding",
    "waiting",
    "complete",
    "skipped",
    "failed",
    "cancelled",
    name="discographybatchitemstate",
    native_enum=False,
    create_constraint=True,
)
_JOB_OWNERSHIP = sa.Enum(
    "created",
    "observed",
    name="discographyjobownership",
    native_enum=False,
    create_constraint=True,
)


def _nonnegative(*columns: str) -> list[sa.CheckConstraint]:
    return [
        sa.CheckConstraint(f"{column} >= 0", name=f"ck_{column}_nonnegative") for column in columns
    ]


def upgrade() -> None:
    op.create_table(
        "discography_batches",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("scope_kind", _SCOPE_KIND, nullable=False),
        sa.Column("scope_json", sa.Text(), nullable=False),
        sa.Column("scope_hash", sa.String(length=64), nullable=False),
        sa.Column("state", _BATCH_STATE, server_default="preview", nullable=False),
        sa.Column("matching_count", sa.Integer(), server_default="0", nullable=False),
        sa.Column("complete_count", sa.Integer(), server_default="0", nullable=False),
        sa.Column("active_count", sa.Integer(), server_default="0", nullable=False),
        sa.Column("hydration_required_count", sa.Integer(), server_default="0", nullable=False),
        sa.Column("missing_count", sa.Integer(), server_default="0", nullable=False),
        sa.Column("skipped_count", sa.Integer(), server_default="0", nullable=False),
        sa.Column("estimated_job_count", sa.Integer(), server_default="0", nullable=False),
        sa.Column("lease_token", sa.String(length=64), nullable=True),
        sa.Column("heartbeat_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.Column(
            "updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.Column("error_detail", sa.Text(), nullable=True),
        *_nonnegative(
            "matching_count",
            "complete_count",
            "active_count",
            "hydration_required_count",
            "missing_count",
            "skipped_count",
            "estimated_job_count",
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_discography_batches_state", "discography_batches", ["state"])
    op.create_index("ix_discography_batches_created_at", "discography_batches", ["created_at"])

    op.create_table(
        "discography_batch_items",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("batch_id", sa.Integer(), nullable=False),
        sa.Column("provider_release_id", sa.Integer(), nullable=True),
        sa.Column("catalog_album_id", sa.Integer(), nullable=True),
        sa.Column("artist_name", sa.Text(), nullable=False),
        sa.Column("release_title", sa.Text(), nullable=False),
        sa.Column("release_year", sa.String(length=4), nullable=True),
        sa.Column("release_kind", sa.String(length=32), nullable=True),
        sa.Column("provider", sa.String(length=32), nullable=True),
        sa.Column("state", _ITEM_STATE, server_default="preview", nullable=False),
        sa.Column("reason_code", sa.String(length=64), nullable=True),
        sa.Column("target_count", sa.Integer(), server_default="0", nullable=False),
        sa.Column("active_count", sa.Integer(), server_default="0", nullable=False),
        sa.Column("skipped_count", sa.Integer(), server_default="0", nullable=False),
        sa.Column("estimated_job_count", sa.Integer(), server_default="0", nullable=False),
        sa.Column("attempt_count", sa.Integer(), server_default="0", nullable=False),
        sa.Column("lease_token", sa.String(length=64), nullable=True),
        sa.Column("heartbeat_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("error_detail", sa.Text(), nullable=True),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.Column(
            "updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint(
            "provider_release_id IS NOT NULL OR catalog_album_id IS NOT NULL",
            name="ck_discography_batch_item_identity",
        ),
        *_nonnegative(
            "target_count", "active_count", "skipped_count", "estimated_job_count", "attempt_count"
        ),
        sa.ForeignKeyConstraint(["batch_id"], ["discography_batches.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(
            ["provider_release_id"], ["catalog_album_providers.id"], ondelete="SET NULL"
        ),
        sa.ForeignKeyConstraint(["catalog_album_id"], ["catalog_albums.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "uq_discography_batch_items_provider_release",
        "discography_batch_items",
        ["batch_id", "provider_release_id"],
        unique=True,
        sqlite_where=sa.text("provider_release_id IS NOT NULL"),
    )
    op.create_index(
        "uq_discography_batch_items_catalog_album",
        "discography_batch_items",
        ["batch_id", "catalog_album_id"],
        unique=True,
        sqlite_where=sa.text("provider_release_id IS NULL AND catalog_album_id IS NOT NULL"),
    )

    op.create_table(
        "discography_batch_item_jobs",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("item_id", sa.Integer(), nullable=False),
        sa.Column("job_id", sa.Integer(), nullable=False),
        sa.Column("ownership", _JOB_OWNERSHIP, nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.ForeignKeyConstraint(["item_id"], ["discography_batch_items.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["job_id"], ["jobs.id"], ondelete="RESTRICT"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("item_id", "job_id", name="uq_discography_batch_item_job"),
    )


def downgrade() -> None:
    op.drop_table("discography_batch_item_jobs")
    op.drop_index("uq_discography_batch_items_catalog_album", table_name="discography_batch_items")
    op.drop_index(
        "uq_discography_batch_items_provider_release", table_name="discography_batch_items"
    )
    op.drop_table("discography_batch_items")
    op.drop_index("ix_discography_batches_created_at", table_name="discography_batches")
    op.drop_index("ix_discography_batches_state", table_name="discography_batches")
    op.drop_table("discography_batches")
