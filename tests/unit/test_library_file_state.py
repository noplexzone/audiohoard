from __future__ import annotations

from app.models.import_plan import (
    DeletionOperation,
    DeletionOperationState,
    ImportPlan,
    LibraryFileState,
)


def test_import_plan_file_state_contract() -> None:
    columns = ImportPlan.__table__.c
    assert set(LibraryFileState) == {"unknown", "present", "missing", "removed"}
    assert columns.file_state.default.arg == LibraryFileState.unknown
    assert columns.file_state.server_default.arg == LibraryFileState.unknown.value
    assert columns.file_checked_at.nullable is True
    assert columns.file_removed_at.nullable is True
    assert columns.file_removal_reason.nullable is True


def test_deletion_operation_journal_contract() -> None:
    columns = DeletionOperation.__table__.c
    assert set(DeletionOperationState) == {"prepared", "committed", "finalized"}
    assert columns.group_id.nullable is False
    assert columns.import_plan_id.foreign_keys
    assert columns.original_path.nullable is False
    assert columns.temporary_path.nullable is False
    assert columns.state.server_default.arg == DeletionOperationState.prepared.value
    assert {index.name for index in DeletionOperation.__table__.indexes} >= {
        "ix_deletion_operations_group_id",
        "ix_deletion_operations_state",
    }
