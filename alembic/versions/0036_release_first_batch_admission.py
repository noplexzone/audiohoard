"""Add release-first batch admission schema.

Revision ID: 0036
Revises: 0035
Create Date: 2026-08-30
"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "0036"
down_revision = "0035"
branch_labels = None
depends_on = None

_JOB_ROLE = sa.Enum(
    "legacy_track",
    "release_root",
    "track_fallback",
    name="discographybatchjobrole",
    native_enum=False,
    create_constraint=False,
)
_ROLE_TRACK_CHECK = (
    "role = 'legacy_track' OR "
    "(role = 'release_root' AND catalog_track_id IS NULL) OR "
    "(role = 'track_fallback' AND catalog_track_id IS NOT NULL)"
)


def upgrade() -> None:
    with op.batch_alter_table("discography_batch_item_jobs") as batch:
        batch.add_column(
            sa.Column(
                "role",
                _JOB_ROLE,
                server_default="legacy_track",
                nullable=False,
            )
        )
        batch.create_check_constraint(
            "discographybatchjobrole",
            "role IN ('legacy_track', 'release_root', 'track_fallback')",
        )
        batch.create_check_constraint(
            "ck_discography_batch_job_role_track",
            _ROLE_TRACK_CHECK,
        )

    op.create_index(
        "uq_discography_batch_item_generation_release_root",
        "discography_batch_item_jobs",
        ["item_id", "generation"],
        unique=True,
        sqlite_where=sa.text("role = 'release_root'"),
        postgresql_where=sa.text("role = 'release_root'"),
    )
    op.create_table(
        "catalog_release_acquisition_claims",
        sa.Column("catalog_album_id", sa.Integer(), nullable=False),
        sa.Column("job_id", sa.Integer(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["catalog_album_id"],
            ["catalog_albums.id"],
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(["job_id"], ["jobs.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("catalog_album_id"),
        sa.UniqueConstraint(
            "job_id",
            name="uq_catalog_release_acquisition_claim_job",
        ),
    )


def downgrade() -> None:
    op.drop_table("catalog_release_acquisition_claims")
    op.drop_index(
        "uq_discography_batch_item_generation_release_root",
        table_name="discography_batch_item_jobs",
    )
    with op.batch_alter_table("discography_batch_item_jobs") as batch:
        batch.drop_constraint("ck_discography_batch_job_role_track", type_="check")
        batch.drop_constraint("discographybatchjobrole", type_="check")
        batch.drop_column("role")
