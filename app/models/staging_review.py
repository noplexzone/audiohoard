from __future__ import annotations

import json
from datetime import datetime
from typing import TYPE_CHECKING

from sqlalchemy import DateTime, Float, ForeignKey, Integer, String, Text, func
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.database import Base
from app.models.workflow import ReviewDecision

if TYPE_CHECKING:
    from app.models.release import Release
    from app.models.track import Track


class StagingReviewItem(Base):
    __tablename__ = "staging_review_items"

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
    fingerprint_duration_sec: Mapped[int | None] = mapped_column(Integer, nullable=True)
    acoustid_score: Mapped[float | None] = mapped_column(Float, nullable=True)
    verification_reason: Mapped[str] = mapped_column(String(64), nullable=False, default="")
    review_state: Mapped[ReviewDecision] = mapped_column(
        String(32), nullable=False, default=ReviewDecision.pending
    )
    reviewed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    track: Mapped[Track] = relationship("Track")
    release: Mapped[Release] = relationship("Release")

    @property
    def observed_acoustid_mbids(self) -> list[str]:
        if not self.observed_acoustid_mbids_json:
            return []
        loaded = json.loads(self.observed_acoustid_mbids_json)
        return [str(m) for m in loaded] if isinstance(loaded, list) else []
