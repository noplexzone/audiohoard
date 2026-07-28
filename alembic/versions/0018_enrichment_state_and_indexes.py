"""Persist artist enrichment state and add hot-path indexes.

Revision ID: 0018
Revises: 0017
Create Date: 2026-07-28
"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "0018"
down_revision = "0017"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "catalog_artists",
        sa.Column(
            "enrichment_state",
            sa.String(length=16),
            server_default="idle",
            nullable=False,
        ),
    )
    op.create_index("ix_tracks_catalog_album_id", "tracks", ["catalog_album_id"], unique=False)
    op.create_index("ix_tracks_catalog_track_id", "tracks", ["catalog_track_id"], unique=False)
    op.create_index("ix_tracks_import_state", "tracks", ["import_state"], unique=False)
    op.create_index("ix_tracks_acquisition_state", "tracks", ["acquisition_state"], unique=False)
    op.create_index("ix_tracks_job_id", "tracks", ["job_id"], unique=False)
    op.create_index("ix_jobs_status", "jobs", ["status"], unique=False)
    op.create_index("ix_jobs_parent_job_id", "jobs", ["parent_job_id"], unique=False)
    op.create_index("ix_jobs_catalog_album_id", "jobs", ["catalog_album_id"], unique=False)


def downgrade() -> None:
    op.drop_index("ix_jobs_catalog_album_id", table_name="jobs")
    op.drop_index("ix_jobs_parent_job_id", table_name="jobs")
    op.drop_index("ix_jobs_status", table_name="jobs")
    op.drop_index("ix_tracks_job_id", table_name="tracks")
    op.drop_index("ix_tracks_acquisition_state", table_name="tracks")
    op.drop_index("ix_tracks_import_state", table_name="tracks")
    op.drop_index("ix_tracks_catalog_track_id", table_name="tracks")
    op.drop_index("ix_tracks_catalog_album_id", table_name="tracks")
    op.drop_column("catalog_artists", "enrichment_state")
