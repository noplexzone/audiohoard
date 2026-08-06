from __future__ import annotations

import json
from datetime import datetime
from typing import TYPE_CHECKING

from sqlalchemy import DateTime, Float, ForeignKey, Index, Integer, String, Text, func, text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.database import Base
from app.models.workflow import ReviewDecision

if TYPE_CHECKING:
    from app.models.release import Release
    from app.models.track import Track


class StagingReviewItem(Base):
    __tablename__ = "staging_review_items"
    __table_args__ = (
        Index(
            "ix_staging_review_automation_candidates",
            "review_state",
            "automation_state",
            "automation_next_attempt_at",
            "id",
        ),
        Index(
            "ix_staging_review_automation_claims",
            "automation_state",
            "automation_claimed_at",
        ),
    )

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    track_id: Mapped[int] = mapped_column(
        ForeignKey("tracks.id", ondelete="CASCADE"), nullable=False
    )
    release_id: Mapped[int] = mapped_column(
        ForeignKey("releases.id", ondelete="CASCADE"), nullable=False
    )
    expected_recording_mbid: Mapped[str | None] = mapped_column(String(36), nullable=True)
    expected_title: Mapped[str | None] = mapped_column(Text, nullable=True)
    observed_acoustid_mbids_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    observed_acoustid_evidence_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    evidence_revision: Mapped[int] = mapped_column(
        Integer, nullable=False, default=1, server_default="1"
    )
    fingerprint_duration_sec: Mapped[int | None] = mapped_column(Integer, nullable=True)
    acoustid_score: Mapped[float | None] = mapped_column(Float, nullable=True)
    verification_reason: Mapped[str] = mapped_column(
        String(64), nullable=False, default="", server_default=text("''")
    )
    review_state: Mapped[ReviewDecision] = mapped_column(
        String(32),
        nullable=False,
        default=ReviewDecision.pending,
        server_default=ReviewDecision.pending.value,
    )
    reviewed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    automation_state: Mapped[str] = mapped_column(
        String(32), nullable=False, default="pending", server_default="pending"
    )
    automation_attempt_count: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default="0"
    )
    automation_claim_token: Mapped[str | None] = mapped_column(String(36), nullable=True)
    automation_claimed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    automation_next_attempt_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    automation_last_attempted_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    automation_decision_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    import_dispatch_state: Mapped[str] = mapped_column(
        String(32), nullable=False, default="none", server_default="none"
    )
    import_dispatch_claim_token: Mapped[str | None] = mapped_column(
        String(36), nullable=True, index=True
    )
    import_dispatch_attempt_count: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default="0"
    )
    import_dispatch_next_attempt_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    import_dispatch_claimed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    import_dispatch_outcome_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    track: Mapped[Track] = relationship("Track")
    release: Mapped[Release] = relationship("Release")
    automation_attempts: Mapped[list[ReviewAutomationAttempt]] = relationship(
        "ReviewAutomationAttempt", back_populates="review_item", passive_deletes=True
    )

    @property
    def observed_acoustid_mbids(self) -> list[str]:
        if not self.observed_acoustid_mbids_json:
            return []
        try:
            loaded = json.loads(self.observed_acoustid_mbids_json)
        except (TypeError, ValueError):
            return []
        return [str(m) for m in loaded] if isinstance(loaded, list) else []

    @property
    def automation_decision(self) -> dict[str, object]:
        if not self.automation_decision_json:
            return {}
        try:
            loaded = json.loads(self.automation_decision_json)
        except (TypeError, ValueError):
            return {}
        return loaded if isinstance(loaded, dict) else {}

    @property
    def source_label(self) -> str | None:
        source = str(self.track.source or "").strip().casefold()
        if not source:
            provenance = self._acquisition_provenance
            source = (
                str(provenance.get("source") or provenance.get("provider") or "")
                .strip()
                .casefold()
            )
        labels = {
            "slskd": "Soulseek (slskd)",
            "prowlarr": "Prowlarr / SABnzbd",
            "sabnzbd": "Prowlarr / SABnzbd",
            "tidal": "TIDAL",
            "youtube": "YouTube",
        }
        return labels.get(source, source or None)

    @property
    def source_username(self) -> str | None:
        raw_username = self._acquisition_provenance.get("username")
        if not isinstance(raw_username, str):
            return None
        username = raw_username.strip()
        return username or None

    @property
    def source_folder(self) -> str | None:
        raw_name = self._acquisition_provenance.get("filename")
        if not isinstance(raw_name, str):
            return None
        remote_path = raw_name.strip().rstrip("/\\")
        separator = max(remote_path.rfind("/"), remote_path.rfind("\\"))
        if separator <= 0:
            return None
        folder = remote_path[:separator].rstrip("/\\").strip()
        return folder or None

    @property
    def original_filename(self) -> str | None:
        provenance = self._acquisition_provenance
        raw_name = provenance.get("original_filename") or provenance.get("filename")
        if not isinstance(raw_name, str) or not raw_name.strip():
            raw_name = self.track.source_path or self.track.staging_path
        if not raw_name:
            return None
        normalized = str(raw_name).strip().replace("\\", "/").rstrip("/")
        return normalized.rsplit("/", 1)[-1] or None

    @property
    def _acquisition_provenance(self) -> dict[str, object]:
        if not self.track.acquisition_provenance_json:
            return {}
        try:
            loaded = json.loads(self.track.acquisition_provenance_json)
        except (TypeError, ValueError):
            return {}
        return loaded if isinstance(loaded, dict) else {}


class ReviewAutomationAttempt(Base):
    """Append-only, sanitized audit record for one automation claim."""

    __tablename__ = "review_automation_attempts"
    __table_args__ = (
        Index("ix_review_automation_attempt_review", "review_item_id", "attempt_number"),
    )

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    review_item_id: Mapped[int | None] = mapped_column(
        ForeignKey("staging_review_items.id", ondelete="SET NULL"), nullable=True
    )
    track_id: Mapped[int] = mapped_column(Integer, nullable=False)
    release_id: Mapped[int] = mapped_column(Integer, nullable=False)
    attempt_number: Mapped[int] = mapped_column(Integer, nullable=False)
    evidence_revision: Mapped[int] = mapped_column(Integer, nullable=False)
    claim_token: Mapped[str] = mapped_column(String(36), nullable=False, unique=True)
    state: Mapped[str] = mapped_column(String(32), nullable=False, default="claimed")
    claimed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    input_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    decision_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    import_outcome_json: Mapped[str | None] = mapped_column(Text, nullable=True)

    review_item: Mapped[StagingReviewItem | None] = relationship(
        "StagingReviewItem", back_populates="automation_attempts"
    )
