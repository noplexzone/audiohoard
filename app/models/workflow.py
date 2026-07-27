from __future__ import annotations

from enum import StrEnum


class AcquisitionState(StrEnum):
    queued = "queued"
    searching = "searching"
    acquiring = "acquiring"
    downloaded = "downloaded"
    failed = "failed"
    cancelled = "cancelled"


class ImportWorkflowState(StrEnum):
    discovered = "discovered"
    staged = "staged"
    matching = "matching"
    needs_review = "needs_review"
    ready = "ready"
    importing = "importing"
    imported = "imported"
    failed = "failed"
    rolled_back = "rolled_back"


class AcoustIDVerificationState(StrEnum):
    pending = "pending"
    verified = "verified"
    mismatch = "mismatch"
    unavailable = "unavailable"
    approved = "approved"
    denied = "denied"


class ReviewDecision(StrEnum):
    pending = "pending"
    approved = "approved"
    denied = "denied"
