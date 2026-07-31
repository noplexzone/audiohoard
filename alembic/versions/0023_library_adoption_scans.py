"""Add persisted library adoption scans and candidates.

Revision ID: 0023
Revises: 0022
Create Date: 2026-07-31
"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "0023"
down_revision = "0022"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "library_adoption_scans",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("scope_kind", sa.String(length=16), nullable=False),
        sa.Column("scope_id", sa.Integer(), nullable=True),
        sa.Column("scope_json", sa.Text(), nullable=True),
        sa.Column("state", sa.String(length=16), server_default="queued", nullable=False),
        sa.Column("scanned_count", sa.Integer(), server_default="0", nullable=False),
        sa.Column("adopted_count", sa.Integer(), server_default="0", nullable=False),
        sa.Column("review_count", sa.Integer(), server_default="0", nullable=False),
        sa.Column("unmatched_count", sa.Integer(), server_default="0", nullable=False),
        sa.Column("stale_count", sa.Integer(), server_default="0", nullable=False),
        sa.Column("error_count", sa.Integer(), server_default="0", nullable=False),
        sa.Column("error_detail", sa.Text(), nullable=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("heartbeat_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.Column(
            "updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.CheckConstraint(
            "scope_kind IN ('full', 'catalog_artist', 'catalog_album', "
            "'imported_artist', 'imported_release')",
            name="adoptionscopekind",
        ),
        sa.CheckConstraint(
            "state IN ('queued', 'running', 'completed', 'failed', "
            "'cancel_requested', 'cancelled')",
            name="adoptionscanstate",
        ),
    )
    op.create_index("ix_library_adoption_scans_state", "library_adoption_scans", ["state"])
    op.create_index(
        "ix_library_adoption_scans_created_at", "library_adoption_scans", ["created_at"]
    )
    op.create_table(
        "library_adoption_candidates",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("scan_id", sa.Integer(), nullable=False),
        sa.Column("path", sa.Text(), nullable=False),
        sa.Column("device", sa.BigInteger(), nullable=False),
        sa.Column("inode", sa.BigInteger(), nullable=False),
        sa.Column("size_bytes", sa.BigInteger(), nullable=False),
        sa.Column("mtime_ns", sa.BigInteger(), nullable=False),
        sa.Column("content_sha256", sa.String(length=64), nullable=False),
        sa.Column("evidence_json", sa.Text(), nullable=False),
        sa.Column("snapshot_token", sa.String(length=64), nullable=False),
        sa.Column("proposed_artist_id", sa.Integer(), nullable=True),
        sa.Column("proposed_album_id", sa.Integer(), nullable=True),
        sa.Column("proposed_catalog_track_id", sa.Integer(), nullable=True),
        sa.Column("proposed_track_id", sa.Integer(), nullable=True),
        sa.Column("confidence", sa.String(length=32), nullable=False),
        sa.Column("reason_codes_json", sa.Text(), nullable=False),
        sa.Column("state", sa.String(length=9), server_default="pending", nullable=False),
        sa.Column("resulting_track_id", sa.Integer(), nullable=True),
        sa.Column("resulting_import_plan_id", sa.Integer(), nullable=True),
        sa.Column("error_detail", sa.Text(), nullable=True),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.Column(
            "updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.CheckConstraint(
            "state IN ('pending', 'adopted', 'review', 'unmatched', 'stale', 'ignored', 'failed')",
            name="adoptioncandidatestate",
        ),
        sa.ForeignKeyConstraint(["scan_id"], ["library_adoption_scans.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["resulting_track_id"], ["tracks.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(
            ["resulting_import_plan_id"], ["import_plans.id"], ondelete="SET NULL"
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "uq_library_adoption_candidate_scan_path",
        "library_adoption_candidates",
        ["scan_id", "path"],
        unique=True,
    )
    op.create_index(
        "ix_library_adoption_candidates_state", "library_adoption_candidates", ["state"]
    )
    op.create_index("ix_library_adoption_candidates_path", "library_adoption_candidates", ["path"])


def downgrade() -> None:
    op.drop_table("library_adoption_candidates")
    op.drop_table("library_adoption_scans")
