"""Add indexes for grouped library artist aggregates.

Revision ID: 0025
Revises: 0024
Create Date: 2026-08-05
"""

from __future__ import annotations

from alembic import op

revision = "0025"
down_revision = "0024"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_index("ix_catalog_albums_artist_id", "catalog_albums", ["artist_id"])
    op.create_index("ix_catalog_album_tracks_album_id", "catalog_album_tracks", ["album_id"])
    op.create_index("ix_import_plans_track_id", "import_plans", ["track_id"])


def downgrade() -> None:
    op.drop_index("ix_import_plans_track_id", table_name="import_plans")
    op.drop_index("ix_catalog_album_tracks_album_id", table_name="catalog_album_tracks")
    op.drop_index("ix_catalog_albums_artist_id", table_name="catalog_albums")
