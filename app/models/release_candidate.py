from __future__ import annotations

import json
from datetime import datetime
from enum import StrEnum
from typing import TYPE_CHECKING

from sqlalchemy import (
    Boolean,
    DateTime,
    Enum,
    Float,
    ForeignKey,
    Integer,
    String,
    Text,
    false,
    func,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.database import Base

if TYPE_CHECKING:
    from app.models.release import Release
    from app.models.track import Track


class MatchReviewState(StrEnum):
    pending = "pending"
    auto_selected = "auto_selected"
    needs_review = "needs_review"
    manual_selected = "manual_selected"
    rejected = "rejected"


class ReleaseCandidate(Base):
    __tablename__ = "release_candidates"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    release_id: Mapped[int] = mapped_column(
        ForeignKey("releases.id", ondelete="CASCADE"), nullable=False
    )
    track_id: Mapped[int | None] = mapped_column(
        ForeignKey("tracks.id", ondelete="CASCADE"), nullable=True
    )
    recording_mbid: Mapped[str | None] = mapped_column(String(36), nullable=True)
    release_mbid: Mapped[str | None] = mapped_column(String(36), nullable=True)
    medium_position: Mapped[int | None] = mapped_column(Integer, nullable=True)
    track_position: Mapped[int | None] = mapped_column(Integer, nullable=True)
    duration_sec: Mapped[int | None] = mapped_column(Integer, nullable=True)
    track_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    year: Mapped[str | None] = mapped_column(String(4), nullable=True)
    country: Mapped[str | None] = mapped_column(String(8), nullable=True)
    label: Mapped[str | None] = mapped_column(Text, nullable=True)
    catalog_number: Mapped[str | None] = mapped_column(String(128), nullable=True)
    barcode: Mapped[str | None] = mapped_column(String(64), nullable=True)
    quality_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    evidence_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    match_score: Mapped[float | None] = mapped_column(Float, nullable=True)
    match_reasons_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    review_state: Mapped[MatchReviewState] = mapped_column(
        Enum(MatchReviewState, native_enum=False, create_constraint=True),
        nullable=False,
        default=MatchReviewState.pending,
        server_default=MatchReviewState.pending.value,
    )
    review_audit_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    selected: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False, server_default=false()
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    release: Mapped[Release] = relationship("Release", back_populates="candidates")
    track: Mapped[Track | None] = relationship("Track", back_populates="release_candidates")

    @property
    def match_reasons(self) -> list[str]:
        if not self.match_reasons_json:
            return []
        loaded = json.loads(self.match_reasons_json)
        if isinstance(loaded, list):
            return [str(item) for item in loaded]
        return []
