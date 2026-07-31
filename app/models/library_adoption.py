from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import TYPE_CHECKING

from sqlalchemy import BigInteger, DateTime, Enum, ForeignKey, Index, Integer, String, Text, func
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.database import Base

if TYPE_CHECKING:
    from app.models.import_plan import ImportPlan
    from app.models.track import Track


class AdoptionScopeKind(StrEnum):
    full = "full"
    catalog_artist = "catalog_artist"
    catalog_album = "catalog_album"
    imported_artist = "imported_artist"
    imported_release = "imported_release"


class AdoptionScanState(StrEnum):
    queued = "queued"
    running = "running"
    completed = "completed"
    failed = "failed"
    cancel_requested = "cancel_requested"
    cancelled = "cancelled"


class AdoptionCandidateState(StrEnum):
    pending = "pending"
    adopted = "adopted"
    review = "review"
    unmatched = "unmatched"
    stale = "stale"
    ignored = "ignored"
    failed = "failed"


class LibraryAdoptionScan(Base):
    __tablename__ = "library_adoption_scans"
    __table_args__ = (
        Index("ix_library_adoption_scans_state", "state"),
        Index("ix_library_adoption_scans_created_at", "created_at"),
    )

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    scope_kind: Mapped[AdoptionScopeKind] = mapped_column(
        Enum(AdoptionScopeKind, native_enum=False, create_constraint=True), nullable=False
    )
    scope_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    scope_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    library_root: Mapped[str] = mapped_column(Text, nullable=False)
    lease_token: Mapped[str | None] = mapped_column(String(36), nullable=True)
    state: Mapped[AdoptionScanState] = mapped_column(
        Enum(AdoptionScanState, native_enum=False, create_constraint=True),
        nullable=False,
        default=AdoptionScanState.queued,
        server_default=AdoptionScanState.queued.value,
    )
    scanned_count: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default="0"
    )
    adopted_count: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default="0"
    )
    review_count: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default="0"
    )
    unmatched_count: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default="0"
    )
    stale_count: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default="0"
    )
    error_count: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default="0"
    )
    error_detail: Mapped[str | None] = mapped_column(Text, nullable=True)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    heartbeat_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )

    candidates: Mapped[list[LibraryAdoptionCandidate]] = relationship(
        "LibraryAdoptionCandidate", back_populates="scan", cascade="all, delete-orphan"
    )


class LibraryAdoptionCandidate(Base):
    __tablename__ = "library_adoption_candidates"
    __table_args__ = (
        Index("uq_library_adoption_candidate_scan_path", "scan_id", "path", unique=True),
        Index("ix_library_adoption_candidates_state", "state"),
        Index("ix_library_adoption_candidates_path", "path"),
    )

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    scan_id: Mapped[int] = mapped_column(
        ForeignKey("library_adoption_scans.id", ondelete="CASCADE"), nullable=False
    )
    path: Mapped[str] = mapped_column(Text, nullable=False)
    device: Mapped[int] = mapped_column(BigInteger, nullable=False)
    inode: Mapped[int] = mapped_column(BigInteger, nullable=False)
    size_bytes: Mapped[int] = mapped_column(BigInteger, nullable=False)
    mtime_ns: Mapped[int] = mapped_column(BigInteger, nullable=False)
    content_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    evidence_json: Mapped[str] = mapped_column(Text, nullable=False)
    snapshot_token: Mapped[str] = mapped_column(String(64), nullable=False)
    proposed_artist_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    proposed_album_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    proposed_catalog_track_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    proposed_track_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    confidence: Mapped[str] = mapped_column(String(32), nullable=False)
    reason_codes_json: Mapped[str] = mapped_column(Text, nullable=False)
    state: Mapped[AdoptionCandidateState] = mapped_column(
        Enum(AdoptionCandidateState, native_enum=False, create_constraint=True),
        nullable=False,
        default=AdoptionCandidateState.pending,
        server_default=AdoptionCandidateState.pending.value,
    )
    resulting_track_id: Mapped[int | None] = mapped_column(
        ForeignKey("tracks.id", ondelete="SET NULL"), nullable=True
    )
    resulting_import_plan_id: Mapped[int | None] = mapped_column(
        ForeignKey("import_plans.id", ondelete="SET NULL"), nullable=True
    )
    error_detail: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )

    scan: Mapped[LibraryAdoptionScan] = relationship(
        "LibraryAdoptionScan", back_populates="candidates"
    )
    resulting_track: Mapped[Track | None] = relationship("Track")
    resulting_import_plan: Mapped[ImportPlan | None] = relationship("ImportPlan")
