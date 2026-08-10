from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import TYPE_CHECKING

from sqlalchemy import (
    BigInteger,
    Boolean,
    DateTime,
    Enum,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    false,
    func,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship, validates

from app.database import Base
from app.services.acquisition_attempts import canonical_provider_uuid

if TYPE_CHECKING:
    from app.models.catalog_entities import CatalogAlbum, CatalogAlbumTrack
    from app.models.job import Job
    from app.models.track import Track


class ProviderTransferState(StrEnum):
    pending = "pending"
    enqueued = "enqueued"
    queued = "queued"
    downloading = "downloading"
    completed = "completed"
    failed = "failed"
    cancelled = "cancelled"


class ArtifactState(StrEnum):
    none = "none"
    partial = "partial"
    staged = "staged"
    imported = "imported"
    missing = "missing"


class AttemptOutcome(StrEnum):
    pending = "pending"
    selected = "selected"
    rejected = "rejected"
    superseded = "superseded"
    failed = "failed"
    downloaded = "downloaded"
    review_retained = "review_retained"
    imported = "imported"


class CleanupState(StrEnum):
    pending = "pending"
    claimed = "claimed"
    completed = "completed"
    blocked = "blocked"
    failed = "failed"
    not_required = "not_required"


class RetentionDisposition(StrEnum):
    workflow_pending = "workflow_pending"
    retain_review = "retain_review"
    retain_recovery = "retain_recovery"
    cleanup_eligible = "cleanup_eligible"
    retained = "retained"
    removed = "removed"


class AcquisitionAttempt(Base):
    __tablename__ = "acquisition_attempts"
    __table_args__ = (
        Index("ix_acquisition_attempts_catalog_identity", "catalog_album_id", "catalog_track_id"),
        Index("ix_acquisition_attempts_provider_identity", "provider", "provider_uuid"),
        Index("ix_acquisition_attempts_candidate", "job_id", "provider", "peer", "remote_path"),
        Index("ix_acquisition_attempts_cleanup", "provider_cleanup_state", "file_cleanup_state"),
    )
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    job_id: Mapped[int] = mapped_column(ForeignKey("jobs.id", ondelete="CASCADE"), nullable=False)
    track_id: Mapped[int | None] = mapped_column(ForeignKey("tracks.id", ondelete="SET NULL"))
    catalog_album_id: Mapped[int | None] = mapped_column(
        ForeignKey("catalog_albums.id", ondelete="SET NULL")
    )
    catalog_track_id: Mapped[int | None] = mapped_column(
        ForeignKey("catalog_album_tracks.id", ondelete="SET NULL")
    )
    provider: Mapped[str] = mapped_column(String(64), nullable=False)
    peer: Mapped[str | None] = mapped_column(Text)
    remote_path: Mapped[str | None] = mapped_column(Text)
    provisional_transfer_id: Mapped[str | None] = mapped_column(String(512))
    provider_uuid: Mapped[str | None] = mapped_column(String(36))
    provider_state: Mapped[ProviderTransferState] = mapped_column(
        Enum(ProviderTransferState, native_enum=False, create_constraint=True),
        nullable=False,
        default=ProviderTransferState.pending,
        server_default=ProviderTransferState.pending.value,
    )
    artifact_state: Mapped[ArtifactState] = mapped_column(
        Enum(ArtifactState, native_enum=False, create_constraint=True),
        nullable=False,
        default=ArtifactState.none,
        server_default=ArtifactState.none.value,
    )
    outcome: Mapped[AttemptOutcome] = mapped_column(
        Enum(AttemptOutcome, native_enum=False, create_constraint=True),
        nullable=False,
        default=AttemptOutcome.pending,
        server_default=AttemptOutcome.pending.value,
    )
    provider_enqueued_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    provider_uuid_discovered_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    provider_terminal_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    terminal_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    provider_cleanup_state: Mapped[CleanupState] = mapped_column(
        Enum(CleanupState, native_enum=False, create_constraint=True),
        nullable=False,
        default=CleanupState.pending,
        server_default=CleanupState.pending.value,
    )
    provider_cleanup_attempt_count: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default="0"
    )
    provider_cleanup_last_attempted_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True)
    )
    provider_cleanup_completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    provider_cleanup_retry_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    file_cleanup_state: Mapped[CleanupState] = mapped_column(
        Enum(CleanupState, native_enum=False, create_constraint=True),
        nullable=False,
        default=CleanupState.pending,
        server_default=CleanupState.pending.value,
    )
    file_cleanup_attempt_count: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default="0"
    )
    file_cleanup_last_attempted_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True)
    )
    file_cleanup_completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    file_cleanup_retry_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    cleanup_claim_token: Mapped[str | None] = mapped_column(String(36))
    cleanup_claim_version: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default="0"
    )
    cleanup_claimed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    cleanup_lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    staged_path: Mapped[str | None] = mapped_column(Text)
    partial_path: Mapped[str | None] = mapped_column(Text)
    quarantine_path: Mapped[str | None] = mapped_column(Text)
    artifact_device: Mapped[int | None] = mapped_column(BigInteger)
    artifact_inode: Mapped[int | None] = mapped_column(BigInteger)
    artifact_mtime_ns: Mapped[int | None] = mapped_column(BigInteger)
    artifact_size: Mapped[int | None] = mapped_column(BigInteger)
    artifact_sha256: Mapped[str | None] = mapped_column(String(64))
    file_cleanup_eligible: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False, server_default=false()
    )
    retention_disposition: Mapped[RetentionDisposition] = mapped_column(
        Enum(RetentionDisposition, native_enum=False, create_constraint=True),
        nullable=False,
        default=RetentionDisposition.workflow_pending,
        server_default=RetentionDisposition.workflow_pending.value,
    )
    error_code: Mapped[str | None] = mapped_column(String(64))
    error_detail: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )

    @validates("provider_uuid")
    def _validate_provider_uuid(self, _key: str, value: str | None) -> str | None:
        if value is None:
            return None
        canonical = canonical_provider_uuid(value)
        if canonical is None:
            raise ValueError("provider_uuid must be a canonical UUID")
        return canonical

    job: Mapped[Job] = relationship("Job", back_populates="acquisition_attempts")
    track: Mapped[Track | None] = relationship("Track", back_populates="acquisition_attempts")
    catalog_album: Mapped[CatalogAlbum | None] = relationship("CatalogAlbum")
    catalog_track: Mapped[CatalogAlbumTrack | None] = relationship("CatalogAlbumTrack")
