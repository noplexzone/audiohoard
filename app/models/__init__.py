from __future__ import annotations

from app.models.acquisition_attempt import AcquisitionAttempt
from app.models.acquisition_claim import AcquisitionDispatchClaim
from app.models.auth import AppUser, AuthSession, UserRole
from app.models.catalog_entities import (
    CatalogAlbum,
    CatalogAlbumProvider,
    CatalogAlbumTrack,
    CatalogArtist,
    CatalogArtistIdentity,
)
from app.models.discography_batch import (
    DiscographyBatch,
    DiscographyBatchItem,
    DiscographyBatchItemJob,
    DiscographyBatchItemState,
    DiscographyBatchState,
    DiscographyJobOwnership,
    DiscographyScopeKind,
)
from app.models.import_plan import ImportPlan
from app.models.job import Job
from app.models.library_adoption import (
    AdoptionCandidateState,
    AdoptionScanState,
    AdoptionScopeKind,
    LibraryAdoptionCandidate,
    LibraryAdoptionScan,
)
from app.models.monitoring import MonitoringRecord
from app.models.path_preview import PathPreview
from app.models.release import Release
from app.models.release_candidate import MatchReviewState, ReleaseCandidate
from app.models.settings import AppSetting, ProviderSetting
from app.models.source_candidate_block import SourceCandidateBlock
from app.models.staging_review import ReviewAutomationAttempt, StagingReviewItem
from app.models.track import Track

__all__ = [
    "AcquisitionAttempt",
    "AcquisitionDispatchClaim",
    "AppSetting",
    "AppUser",
    "AuthSession",
    "CatalogAlbum",
    "CatalogAlbumProvider",
    "CatalogAlbumTrack",
    "CatalogArtist",
    "CatalogArtistIdentity",
    "AdoptionCandidateState",
    "AdoptionScanState",
    "AdoptionScopeKind",
    "DiscographyBatch",
    "DiscographyBatchItem",
    "DiscographyBatchItemJob",
    "DiscographyBatchItemState",
    "DiscographyBatchState",
    "DiscographyJobOwnership",
    "DiscographyScopeKind",
    "ImportPlan",
    "Job",
    "LibraryAdoptionCandidate",
    "LibraryAdoptionScan",
    "MonitoringRecord",
    "PathPreview",
    "ProviderSetting",
    "Release",
    "MatchReviewState",
    "ReleaseCandidate",
    "ReviewAutomationAttempt",
    "StagingReviewItem",
    "SourceCandidateBlock",
    "Track",
    "UserRole",
]
