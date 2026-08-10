"""Add exact catalog acquisition dispatch claims.

Revision ID: 0029
Revises: 0028
Create Date: 2026-08-09
"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "0029"
down_revision = "0028"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "acquisition_dispatch_claims",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("catalog_album_id", sa.Integer(), nullable=False),
        sa.Column("catalog_track_id", sa.Integer(), nullable=False),
        sa.Column("job_id", sa.Integer(), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.ForeignKeyConstraint(["catalog_album_id"], ["catalog_albums.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(
            ["catalog_track_id"], ["catalog_album_tracks.id"], ondelete="CASCADE"
        ),
        sa.ForeignKeyConstraint(["job_id"], ["jobs.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "catalog_album_id",
            "catalog_track_id",
            name="uq_acquisition_dispatch_catalog_identity",
        ),
    )


def downgrade() -> None:
    op.drop_table("acquisition_dispatch_claims")
