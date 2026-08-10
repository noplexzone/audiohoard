from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

from sqlalchemy import DateTime, ForeignKey, UniqueConstraint, func
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.database import Base

if TYPE_CHECKING:
    from app.models.job import Job


class AcquisitionDispatchClaim(Base):
    __tablename__ = "acquisition_dispatch_claims"
    __table_args__ = (
        UniqueConstraint(
            "catalog_album_id", "catalog_track_id", name="uq_acquisition_dispatch_catalog_identity"
        ),
    )

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    catalog_album_id: Mapped[int] = mapped_column(
        ForeignKey("catalog_albums.id", ondelete="CASCADE"), nullable=False
    )
    catalog_track_id: Mapped[int] = mapped_column(
        ForeignKey("catalog_album_tracks.id", ondelete="CASCADE"), nullable=False
    )
    job_id: Mapped[int] = mapped_column(ForeignKey("jobs.id", ondelete="CASCADE"), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    job: Mapped[Job] = relationship("Job")
