"""Add durable acquisition attempts.

Revision ID: 0028
Revises: 0027
Create Date: 2026-08-09
"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "0028"
down_revision = "0027"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "acquisition_attempts",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("job_id", sa.Integer(), nullable=False),
        sa.Column("track_id", sa.Integer(), nullable=True),
        sa.Column("catalog_album_id", sa.Integer(), nullable=True),
        sa.Column("catalog_track_id", sa.Integer(), nullable=True),
        sa.Column("provider", sa.String(length=64), nullable=False),
        sa.Column("peer", sa.Text(), nullable=True),
        sa.Column("remote_path", sa.Text(), nullable=True),
        sa.Column("provider_transfer_id", sa.String(length=128), nullable=True),
        sa.Column(
            "provider_state", sa.String(length=11), server_default="pending", nullable=False
        ),
        sa.Column("artifact_state", sa.String(length=8), server_default="none", nullable=False),
        sa.Column("outcome", sa.String(length=10), server_default="pending", nullable=False),
        sa.Column(
            "provider_cleanup_state",
            sa.String(length=12),
            server_default="pending",
            nullable=False,
        ),
        sa.Column(
            "file_cleanup_state", sa.String(length=12), server_default="pending", nullable=False
        ),
        sa.Column("staged_path", sa.Text(), nullable=True),
        sa.Column("partial_path", sa.Text(), nullable=True),
        sa.Column("artifact_device", sa.BigInteger(), nullable=True),
        sa.Column("artifact_inode", sa.BigInteger(), nullable=True),
        sa.Column("artifact_mtime_ns", sa.BigInteger(), nullable=True),
        sa.Column("artifact_size", sa.BigInteger(), nullable=True),
        sa.Column("artifact_sha256", sa.String(length=64), nullable=True),
        sa.Column("claim_token", sa.String(length=36), nullable=True),
        sa.Column("claim_version", sa.Integer(), server_default="0", nullable=False),
        sa.Column("claimed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("error_code", sa.String(length=64), nullable=True),
        sa.Column("error_detail", sa.Text(), nullable=True),
        sa.Column("terminal_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.Column(
            "updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.CheckConstraint(
            "provider_state IN ('pending', 'enqueued', 'downloading', "
            "'completed', 'failed', 'cancelled')",
            name="providertransferstate",
        ),
        sa.CheckConstraint(
            "artifact_state IN ('none', 'partial', 'staged', 'imported', 'missing')",
            name="artifactstate",
        ),
        sa.CheckConstraint(
            "outcome IN ('pending', 'selected', 'rejected', 'superseded', 'failed')",
            name="attemptoutcome",
        ),
        sa.CheckConstraint(
            "provider_cleanup_state IN ('pending', 'claimed', 'completed', "
            "'blocked', 'failed', 'not_required')",
            name="cleanupstate",
        ),
        sa.CheckConstraint(
            "file_cleanup_state IN ('pending', 'claimed', 'completed', "
            "'blocked', 'failed', 'not_required')",
            name="cleanupstate_file",
        ),
        sa.ForeignKeyConstraint(["catalog_album_id"], ["catalog_albums.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(
            ["catalog_track_id"], ["catalog_album_tracks.id"], ondelete="SET NULL"
        ),
        sa.ForeignKeyConstraint(["job_id"], ["jobs.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["track_id"], ["tracks.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_acquisition_attempts_catalog_identity",
        "acquisition_attempts",
        ["catalog_album_id", "catalog_track_id"],
    )
    op.create_index(
        "ix_acquisition_attempts_provider_identity",
        "acquisition_attempts",
        ["provider", "provider_transfer_id"],
    )
    op.create_index(
        "ix_acquisition_attempts_cleanup",
        "acquisition_attempts",
        ["provider_cleanup_state", "file_cleanup_state"],
    )


def downgrade() -> None:
    op.drop_index("ix_acquisition_attempts_cleanup", table_name="acquisition_attempts")
    op.drop_index("ix_acquisition_attempts_provider_identity", table_name="acquisition_attempts")
    op.drop_index("ix_acquisition_attempts_catalog_identity", table_name="acquisition_attempts")
    op.drop_table("acquisition_attempts")
