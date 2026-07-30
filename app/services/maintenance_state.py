from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime

from app.services.library_scan import LibraryScanResult
from app.services.quality_upgrade import QualityDuplicateResult


@dataclass
class DuplicateAlbumSummary:
    album_id: int
    result: QualityDuplicateResult


@dataclass
class DuplicateScanSummary:
    deleted_files: int = 0
    review_required: int = 0
    would_delete_paths: tuple[str, ...] = ()
    albums: tuple[DuplicateAlbumSummary, ...] = ()
    scanned_at: datetime | None = None

    @property
    def safe_album_ids(self) -> tuple[int, ...]:
        return tuple(album.album_id for album in self.albums if album.result.review_required == 0)


@dataclass
class QualityUpgradeScanSummary:
    checked_records: int = 0
    scanned_at: datetime | None = None


@dataclass
class MaintenanceState:
    library_scan: LibraryScanResult | None = None
    duplicate_scan: DuplicateScanSummary = field(default_factory=DuplicateScanSummary)
    quality_upgrade_scan: QualityUpgradeScanSummary = field(
        default_factory=QualityUpgradeScanSummary
    )
    ignored_orphans: set[str] = field(default_factory=set)
    library_scanned_at: datetime | None = None

    def store_library_scan(self, result: LibraryScanResult) -> None:
        self.library_scan = result
        self.library_scanned_at = datetime.now(UTC)

    def store_duplicate_scan(self, result: DuplicateScanSummary) -> None:
        self.duplicate_scan = result

    def store_quality_upgrade_scan(self, checked_records: int) -> None:
        self.quality_upgrade_scan = QualityUpgradeScanSummary(
            checked_records=checked_records, scanned_at=datetime.now(UTC)
        )


def empty_maintenance_state() -> MaintenanceState:
    return MaintenanceState()
