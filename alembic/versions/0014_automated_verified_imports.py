"""Automated verified imports: verification state, continuation jobs, review items.

Revision ID: 0014
Revises: 0013
Create Date: 2026-07-26
"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "0014"
down_revision = "0013"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # --- tracks: acoustid verification fields ---
    track_columns = {col["name"] for col in sa.inspect(op.get_bind()).get_columns("tracks")}
    if "acoustid_verification_state" not in track_columns:
        op.add_column(
            "tracks",
            sa.Column(
                "acoustid_verification_state",
                sa.String(32),
                nullable=False,
                server_default="pending",
            ),
        )
    if "acoustid_evidence_json" not in track_columns:
        op.add_column(
            "tracks",
            sa.Column("acoustid_evidence_json", sa.Text, nullable=True),
        )

    # --- jobs: partial continuation fields ---
    job_columns = {col["name"] for col in sa.inspect(op.get_bind()).get_columns("jobs")}
    if "parent_job_id" not in job_columns:
        with op.batch_alter_table("jobs") as batch_op:
            batch_op.add_column(sa.Column("parent_job_id", sa.Integer, nullable=True))
            batch_op.create_foreign_key(
                "fk_jobs_parent_job_id_jobs",
                "jobs",
                ["parent_job_id"],
                ["id"],
                ondelete="SET NULL",
            )
    if "partial_attempt" not in job_columns:
        op.add_column(
            "jobs",
            sa.Column(
                "partial_attempt",
                sa.Integer,
                nullable=False,
                server_default="0",
            ),
        )

    # --- staging_review_items: durable verification review records ---
    tables = set(sa.inspect(op.get_bind()).get_table_names())
    if "staging_review_items" not in tables:
        op.create_table(
            "staging_review_items",
            sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
            sa.Column(
                "track_id",
                sa.Integer,
                sa.ForeignKey("tracks.id", ondelete="CASCADE"),
                nullable=False,
            ),
            sa.Column(
                "release_id",
                sa.Integer,
                sa.ForeignKey("releases.id", ondelete="CASCADE"),
                nullable=False,
            ),
            sa.Column("expected_recording_mbid", sa.String(36), nullable=True),
            sa.Column("expected_title", sa.Text, nullable=True),
            sa.Column("observed_acoustid_mbids_json", sa.Text, nullable=True),
            sa.Column("fingerprint_duration_sec", sa.Integer, nullable=True),
            sa.Column("acoustid_score", sa.Float, nullable=True),
            sa.Column("verification_reason", sa.String(64), nullable=False, server_default=""),
            sa.Column(
                "review_state",
                sa.String(32),
                nullable=False,
                server_default="pending",
            ),
            sa.Column("reviewed_at", sa.DateTime(timezone=True), nullable=True),
            sa.Column(
                "created_at",
                sa.DateTime(timezone=True),
                server_default=sa.func.now(),
                nullable=False,
            ),
        )


def downgrade() -> None:
    tables = set(sa.inspect(op.get_bind()).get_table_names())
    if "staging_review_items" in tables:
        op.drop_table("staging_review_items")

    job_columns = {col["name"] for col in sa.inspect(op.get_bind()).get_columns("jobs")}
    with op.batch_alter_table("jobs") as batch_op:
        if "partial_attempt" in job_columns:
            batch_op.drop_column("partial_attempt")
        if "parent_job_id" in job_columns:
            batch_op.drop_column("parent_job_id")

    track_columns = {col["name"] for col in sa.inspect(op.get_bind()).get_columns("tracks")}
    with op.batch_alter_table("tracks") as batch_op:
        if "acoustid_evidence_json" in track_columns:
            batch_op.drop_column("acoustid_evidence_json")
        if "acoustid_verification_state" in track_columns:
            batch_op.drop_column("acoustid_verification_state")
