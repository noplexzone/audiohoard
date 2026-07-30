"""Persist artist watchlist defaults.

Revision ID: 0020
Revises: 0019
Create Date: 2026-07-29
"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "0020"
down_revision = "0019"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "catalog_artists",
        sa.Column("watchlist_release_albums", sa.Boolean(), server_default="1", nullable=False),
    )
    op.add_column(
        "catalog_artists",
        sa.Column("watchlist_release_singles", sa.Boolean(), server_default="0", nullable=False),
    )
    op.add_column(
        "catalog_artists",
        sa.Column("watchlist_release_eps", sa.Boolean(), server_default="0", nullable=False),
    )
    op.add_column(
        "catalog_artists",
        sa.Column("watchlist_monitor_upgrades", sa.Boolean(), server_default="0", nullable=False),
    )


def downgrade() -> None:
    op.drop_column("catalog_artists", "watchlist_monitor_upgrades")
    op.drop_column("catalog_artists", "watchlist_release_eps")
    op.drop_column("catalog_artists", "watchlist_release_singles")
    op.drop_column("catalog_artists", "watchlist_release_albums")
