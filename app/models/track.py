from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import TYPE_CHECKING

from sqlalchemy import BigInteger, DateTime, Enum, ForeignKey, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.database import Base
from app.models.workflow import AcquisitionState, ImportWorkflowState

if TYPE_CHECKING:
    from app.models.catalog_entities import CatalogAlbum, CatalogAlbumTrack
    from app.models.import_plan import ImportPlan
    from app.models.job import Job
    from app.models.path_preview import PathPreview
    from app.models.release import Release
    from app.models.release_candidate import ReleaseCandidate


class FingerprintState(StrEnum):
    pending = "pending"
    done = "done"
    failed = "failed"
    skipped = "skipped"


class IdentityResolutionState(StrEnum):
    pending = "pending"
    resolved = "resolved"
    unresolved = "unresolved"


class Track(Base):
    __tablename__ = "tracks"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    job_id: Mapped[int] = mapped_column(ForeignKey("jobs.id", ondelete="CASCADE"), nullable=False)
    release_id: Mapped[int | None] = mapped_column(
        ForeignKey("releases.id", ondelete="SET NULL"), nullable=True
    )
    catalog_album_id: Mapped[int | None] = mapped_column(
        ForeignKey("catalog_albums.id", ondelete="SET NULL"), nullable=True
    )
    catalog_track_id: Mapped[int | None] = mapped_column(
        ForeignKey("catalog_album_tracks.id", ondelete="SET NULL"), nullable=True
    )
    title: Mapped[str | None] = mapped_column(Text, nullable=True)
    artist: Mapped[str | None] = mapped_column(Text, nullable=True)
    album_artist: Mapped[str | None] = mapped_column(Text, nullable=True)
    album: Mapped[str | None] = mapped_column(Text, nullable=True)
    year: Mapped[str | None] = mapped_column(String(4), nullable=True)
    disc: Mapped[int | None] = mapped_column(Integer, nullable=True)
    disc_total: Mapped[int | None] = mapped_column(Integer, nullable=True)
    track_no: Mapped[int | None] = mapped_column(Integer, nullable=True)
    duration_sec: Mapped[int | None] = mapped_column(Integer, nullable=True)
    mbid: Mapped[str | None] = mapped_column(String(36), nullable=True)
    identity_state: Mapped[IdentityResolutionState] = mapped_column(
        Enum(IdentityResolutionState),
        nullable=False,
        default=IdentityResolutionState.pending,
    )
    acoustid: Mapped[str | None] = mapped_column(Text, nullable=True)
    deezer_id: Mapped[str | None] = mapped_column(String(32), nullable=True)
    source_path: Mapped[str | None] = mapped_column(Text, nullable=True)
    source_job_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    source_status: Mapped[str | None] = mapped_column(String(128), nullable=True)
    source: Mapped[str] = mapped_column(String(64), nullable=False)
    acquisition_state: Mapped[AcquisitionState] = mapped_column(
        Enum(AcquisitionState, native_enum=False, create_constraint=True),
        nullable=False,
        default=AcquisitionState.queued,
    )
    import_state: Mapped[ImportWorkflowState] = mapped_column(
        Enum(ImportWorkflowState, native_enum=False, create_constraint=True),
        nullable=False,
        default=ImportWorkflowState.discovered,
    )
    staging_path: Mapped[str | None] = mapped_column(Text, nullable=True)
    content_sha256: Mapped[str | None] = mapped_column(String(64), nullable=True)
    acquisition_provenance_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    file_format: Mapped[str | None] = mapped_column(String(16), nullable=True)
    file_size_bytes: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    file_metadata_checked_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    fingerprint_state: Mapped[FingerprintState] = mapped_column(
        Enum(FingerprintState),
        nullable=False,
        default=FingerprintState.pending,
    )

    job: Mapped[Job] = relationship("Job", back_populates="tracks")
    release: Mapped[Release | None] = relationship("Release", back_populates="tracks")
    catalog_album: Mapped[CatalogAlbum | None] = relationship(
        "CatalogAlbum", back_populates="library_tracks"
    )
    catalog_track: Mapped[CatalogAlbumTrack | None] = relationship(
        "CatalogAlbumTrack", back_populates="library_tracks"
    )
    path_previews: Mapped[list[PathPreview]] = relationship(
        "PathPreview", back_populates="track", cascade="all, delete-orphan"
    )
    release_candidates: Mapped[list[ReleaseCandidate]] = relationship(
        "ReleaseCandidate", back_populates="track", cascade="all, delete-orphan"
    )
    import_plans: Mapped[list[ImportPlan]] = relationship(
        "ImportPlan",
        back_populates="track",
        cascade="all, delete-orphan",
        order_by="ImportPlan.id",
    )
