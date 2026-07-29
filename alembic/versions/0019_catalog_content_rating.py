"""Track catalog release content rating.

Revision ID: 0019
Revises: 0018
Create Date: 2026-07-29
"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "0019"
down_revision = "0018"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "catalog_albums",
        sa.Column(
            "content_rating", sa.String(length=16), server_default="unknown", nullable=False
        ),
    )
    op.add_column("catalog_albums", sa.Column("upc", sa.String(length=64), nullable=True))
    op.add_column(
        "catalog_album_providers",
        sa.Column(
            "content_rating", sa.String(length=16), server_default="unknown", nullable=False
        ),
    )
    op.add_column("catalog_album_providers", sa.Column("upc", sa.String(length=64), nullable=True))
    op.add_column(
        "catalog_album_tracks",
        sa.Column(
            "content_rating", sa.String(length=16), server_default="unknown", nullable=False
        ),
    )


def downgrade() -> None:
    op.drop_column("catalog_album_tracks", "content_rating")
    op.drop_column("catalog_album_providers", "upc")
    op.drop_column("catalog_album_providers", "content_rating")
    op.drop_column("catalog_albums", "upc")
    op.drop_column("catalog_albums", "content_rating")
