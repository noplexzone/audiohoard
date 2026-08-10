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
        sa.Column("provisional_transfer_id", sa.String(length=512), nullable=True),
        sa.Column("provider_uuid", sa.String(length=36), nullable=True),
        sa.Column(
            "provider_state", sa.String(length=11), server_default="pending", nullable=False
        ),
        sa.Column("artifact_state", sa.String(length=8), server_default="none", nullable=False),
        sa.Column("outcome", sa.String(length=15), server_default="pending", nullable=False),
        sa.Column("provider_enqueued_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("provider_uuid_discovered_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("provider_terminal_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "provider_cleanup_state",
            sa.String(length=12),
            server_default="pending",
            nullable=False,
        ),
        sa.Column(
            "file_cleanup_state", sa.String(length=12), server_default="pending", nullable=False
        ),
        sa.Column(
            "provider_cleanup_attempt_count", sa.Integer(), server_default="0", nullable=False
        ),
        sa.Column("provider_cleanup_last_attempted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("provider_cleanup_completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("provider_cleanup_retry_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("file_cleanup_attempt_count", sa.Integer(), server_default="0", nullable=False),
        sa.Column("file_cleanup_last_attempted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("file_cleanup_completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("file_cleanup_retry_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("staged_path", sa.Text(), nullable=True),
        sa.Column("partial_path", sa.Text(), nullable=True),
        sa.Column("quarantine_path", sa.Text(), nullable=True),
        sa.Column("artifact_device", sa.BigInteger(), nullable=True),
        sa.Column("artifact_inode", sa.BigInteger(), nullable=True),
        sa.Column("artifact_mtime_ns", sa.BigInteger(), nullable=True),
        sa.Column("artifact_size", sa.BigInteger(), nullable=True),
        sa.Column("artifact_sha256", sa.String(length=64), nullable=True),
        sa.Column(
            "file_cleanup_eligible", sa.Boolean(), server_default=sa.false(), nullable=False
        ),
        sa.Column(
            "retention_disposition",
            sa.String(length=16),
            server_default="workflow_pending",
            nullable=False,
        ),
        sa.Column("cleanup_claim_token", sa.String(length=36), nullable=True),
        sa.Column("cleanup_claim_version", sa.Integer(), server_default="0", nullable=False),
        sa.Column("cleanup_claimed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("cleanup_lease_expires_at", sa.DateTime(timezone=True), nullable=True),
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
            "provider_state IN ('pending', 'enqueued', 'queued', 'downloading', "
            "'completed', 'failed', 'cancelled')",
            name="providertransferstate",
        ),
        sa.CheckConstraint(
            "artifact_state IN ('none', 'partial', 'staged', 'imported', 'missing')",
            name="artifactstate",
        ),
        sa.CheckConstraint(
            "outcome IN ('pending', 'selected', 'rejected', 'superseded', 'failed', "
            "'downloaded', 'review_retained', 'imported')",
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
        sa.CheckConstraint(
            "retention_disposition IN ('workflow_pending', 'retain_review', "
            "'retain_recovery', 'cleanup_eligible', 'retained', 'removed')",
            name="retentiondisposition",
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
        ["provider", "provider_uuid"],
    )
    op.create_index(
        "ix_acquisition_attempts_candidate",
        "acquisition_attempts",
        ["job_id", "provider", "peer", "remote_path"],
    )
    op.create_index(
        "ix_acquisition_attempts_cleanup",
        "acquisition_attempts",
        ["provider_cleanup_state", "file_cleanup_state"],
    )


def downgrade() -> None:
    op.drop_index("ix_acquisition_attempts_cleanup", table_name="acquisition_attempts")
    op.drop_index("ix_acquisition_attempts_candidate", table_name="acquisition_attempts")
    op.drop_index("ix_acquisition_attempts_provider_identity", table_name="acquisition_attempts")
    op.drop_index("ix_acquisition_attempts_catalog_identity", table_name="acquisition_attempts")
    op.drop_table("acquisition_attempts")
