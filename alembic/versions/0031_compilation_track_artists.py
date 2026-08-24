"""Persist compilation album and per-track artist credits.

Revision ID: 0031
Revises: 0030
Create Date: 2026-08-23
"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "0031"
down_revision = "0030"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("catalog_albums") as batch_op:
        batch_op.add_column(sa.Column("album_artist_name", sa.Text(), nullable=True))
        batch_op.add_column(sa.Column("album_artist_provider_id", sa.String(128), nullable=True))
        batch_op.add_column(
            sa.Column("is_compilation", sa.Boolean(), server_default="0", nullable=False)
        )
    with op.batch_alter_table("catalog_album_tracks") as batch_op:
        batch_op.add_column(sa.Column("artist_name", sa.Text(), nullable=True))
        batch_op.add_column(sa.Column("artist_provider_id", sa.String(128), nullable=True))
    op.execute(
        sa.text(
            """
            UPDATE catalog_albums
            SET is_compilation = 1
            WHERE lower(coalesce(release_type, '')) IN ('compile', 'compilation')
               OR id IN (
                    SELECT catalog_album_id
                    FROM catalog_album_providers
                    WHERE lower(coalesce(release_kind, '')) = 'compilation'
               )
            """
        )
    )


def downgrade() -> None:
    with op.batch_alter_table("catalog_album_tracks") as batch_op:
        batch_op.drop_column("artist_provider_id")
        batch_op.drop_column("artist_name")
    with op.batch_alter_table("catalog_albums") as batch_op:
        batch_op.drop_column("is_compilation")
        batch_op.drop_column("album_artist_provider_id")
        batch_op.drop_column("album_artist_name")
