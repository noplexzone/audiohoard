from __future__ import annotations

import asyncio
import hashlib
import json
import sqlite3
from pathlib import Path

import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

import app.models  # noqa: F401
from app.config import Settings
from app.database import Base
from app.maintenance.slskd_reconcile import build_report, create_parser, open_readonly
from app.models.acquisition_attempt import AcquisitionAttempt, CleanupState, ProviderTransferState
from app.models.import_plan import ImportPlan
from app.models.job import Job
from app.models.release import Release
from app.models.track import Track
from app.models.workflow import ImportWorkflowState

UUID_LIVE = "2d93899b-cf9a-4567-8f10-993610f274cf"
UUID_MISSING = "06fdfa12-6d4a-4f9e-aa13-bc35685fef65"
UUID_MISMATCH = "41e0a21a-54f8-4216-aa67-cf9a9f2f880f"
UUID_ORPHAN = "ec6cc005-c7ac-46cb-acb5-65e4f238e71b"


class Adapter:
    def __init__(self, snapshot=None, error: Exception | None = None):
        self.snapshot = snapshot or []
        self.error = error
        self.calls = 0

    async def downloads(self, *, force_refresh: bool = False):
        assert force_refresh is True
        self.calls += 1
        if self.error:
            raise self.error
        return self.snapshot


async def _fixture_database(path: Path, artifact: Path) -> None:
    engine = create_async_engine(f"sqlite+aiosqlite:///{path}")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    stat = await asyncio.to_thread(artifact.stat)
    contents = await asyncio.to_thread(artifact.read_bytes)
    digest = hashlib.sha256(contents).hexdigest()
    async with factory() as db:
        job = Job(source="slskd", query="fixture")
        db.add(job)
        await db.flush()
        release = Release(job=job, source="slskd", title="Review debt")
        track = Track(job=job, release=release, source="slskd", staging_path=str(artifact))
        db.add(
            ImportPlan(
                release=release,
                track=track,
                source_path=str(artifact),
                staging_path=str(artifact),
                destination_path=str(artifact.parent / "library.flac"),
                status=ImportWorkflowState.needs_review,
            )
        )
        db.add_all(
            [
                AcquisitionAttempt(
                    job_id=job.id,
                    provider="slskd",
                    peer="peer",
                    remote_path="Album/live.flac",
                    provider_uuid=UUID_LIVE,
                    provider_state=ProviderTransferState.downloading,
                    staged_path=str(artifact),
                    artifact_device=stat.st_dev,
                    artifact_inode=stat.st_ino,
                    artifact_mtime_ns=stat.st_mtime_ns,
                    artifact_size=stat.st_size,
                    artifact_sha256=digest,
                ),
                AcquisitionAttempt(
                    job_id=job.id,
                    provider="slskd",
                    peer="peer",
                    remote_path="Album/missing.flac",
                    provider_uuid=UUID_MISSING,
                    provider_state=ProviderTransferState.completed,
                    staged_path=str(artifact.parent / "missing.flac"),
                    artifact_device=1,
                    artifact_inode=2,
                    artifact_mtime_ns=3,
                    artifact_size=4,
                    artifact_sha256="0" * 64,
                ),
                AcquisitionAttempt(
                    job_id=job.id,
                    provider="slskd",
                    peer="peer",
                    remote_path="Album/mismatch.flac",
                    provider_uuid=UUID_MISMATCH,
                    provider_state=ProviderTransferState.completed,
                ),
                AcquisitionAttempt(
                    job_id=job.id,
                    provider="slskd",
                    peer="resolved",
                    remote_path="Album/resolved.flac",
                    provider_state=ProviderTransferState.completed,
                    provider_cleanup_state=CleanupState.completed,
                    file_cleanup_state=CleanupState.not_required,
                ),
                AcquisitionAttempt(
                    job_id=job.id,
                    provider="slskd",
                    peer="ambiguous",
                    remote_path="Album/ambiguous.flac",
                    provider_state=ProviderTransferState.failed,
                ),
            ]
        )
        await db.commit()
    await engine.dispose()


async def test_report_classifies_fixture_and_performs_zero_mutations(tmp_path: Path) -> None:
    database = tmp_path / "audiohoard.db"
    complete = tmp_path / "complete"
    incomplete = tmp_path / "incomplete"
    artifact = complete / "Album" / "live.flac"
    artifact.parent.mkdir(parents=True)
    artifact.write_bytes(b"exact artifact")
    empty = incomplete / "old-empty"
    empty.mkdir(parents=True)
    stray = complete / "Album" / "unreferenced.flac"
    stray.write_bytes(b"unreferenced")
    partial = incomplete / "peer" / "partial.tmp"
    partial.parent.mkdir(parents=True)
    partial.write_bytes(b"partial")
    import os
    import time

    old = time.time() - 172800
    os.utime(empty, (old, old))
    await _fixture_database(database, artifact)
    settings = Settings(
        secret_key="test",
        database_url=f"sqlite+aiosqlite:///{database}",
        slskd_url="http://provider.invalid",
        slskd_api_key="not-reported",
        slskd_complete_root=complete,
        slskd_incomplete_root=incomplete,
    )
    snapshot = [
        {
            "id": UUID_LIVE,
            "username": "peer",
            "filename": "Album/live.flac",
            "state": "InProgress",
            "localPath": str(artifact),
        },
        {
            "id": UUID_MISMATCH,
            "username": "different",
            "filename": "other.flac",
            "state": "Completed",
        },
        {"id": UUID_ORPHAN, "username": "orphan", "filename": "orphan.flac", "state": "Completed"},
    ]
    before_db = database.read_bytes()
    before_artifact = artifact.read_bytes()

    report = await build_report(database, settings=settings, adapter=Adapter(snapshot), limit=50)

    categories = report["categories"]
    assert [row["provider_uuid"] for row in categories["queue_uuids_with_attempt_owner"]] == [
        UUID_LIVE,
        UUID_MISMATCH,
    ]
    assert categories["queue_rows_without_attempt_owner"] == [{"provider_uuid": UUID_ORPHAN}]
    assert categories["unmatched_terminal_transfers"] == [{"provider_uuid": UUID_ORPHAN}]
    assert categories["ambiguous_attempts"] == [
        {"attempt_id": 5, "reason": "missing_canonical_provider_uuid"}
    ]
    assert categories["review_import_debt"] == [{"plan_id": 1, "status": "needs_review"}]
    assert categories["unreferenced_complete_files"] == [
        {"root": "complete", "path": "Album/unreferenced.flac"}
    ]
    assert {tuple(sorted(row.items())) for row in categories["incomplete_tree_entries"]} >= {
        tuple(sorted({"root": "incomplete", "path": "old-empty", "kind": "directory"}.items())),
        tuple(sorted({"root": "incomplete", "path": "peer/partial.tmp", "kind": "file"}.items())),
    }
    assert [row["attempt_id"] for row in categories["attempt_obligations_live_queue_uuid"]] == [1]
    assert [row["attempt_id"] for row in categories["attempt_obligations_missing_queue_uuid"]] == [
        2,
        5,
    ]
    assert [
        row["attempt_id"] for row in categories["attempt_obligations_mismatched_queue_uuid"]
    ] == [3]
    assert [row["attempt_id"] for row in categories["exact_artifact_present"]] == [1]
    assert [row["attempt_id"] for row in categories["exact_artifact_missing"]] == [2]
    assert categories["exact_artifact_mismatched"] == []
    assert categories["empty_directories_eligible"] == [
        {"root": "incomplete", "path": "old-empty"}
    ]
    assert database.read_bytes() == before_db
    assert artifact.read_bytes() == before_artifact
    sidecars = await asyncio.to_thread(lambda: list(tmp_path.glob("audiohoard.db-*")))
    assert not sidecars
    assert "not-reported" not in json.dumps(report)


def test_readonly_connection_enforces_query_only_and_parser_has_no_mutation_flags(
    tmp_path: Path,
) -> None:
    database = tmp_path / "db.sqlite"
    sqlite3.connect(database).close()
    with open_readonly(database) as connection:
        assert connection.execute("PRAGMA query_only").fetchone()[0] == 1
        with pytest.raises(sqlite3.OperationalError):
            connection.execute("CREATE TABLE forbidden (id INTEGER)")
    parser = create_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(["--apply"])
    with pytest.raises(SystemExit):
        parser.parse_args(["--delete"])


async def test_provider_unavailable_is_truthful_and_does_not_classify_absence(
    tmp_path: Path,
) -> None:
    database = tmp_path / "audiohoard.db"
    artifact = tmp_path / "artifact.flac"
    artifact.write_bytes(b"x")
    await _fixture_database(database, artifact)
    settings = Settings(secret_key="test", database_url=f"sqlite+aiosqlite:///{database}")

    report = await build_report(
        database,
        settings=settings,
        adapter=Adapter(error=RuntimeError("https://user:secret@example.invalid/key")),
    )

    assert report["provider"] == {"status": "unavailable", "error_code": "provider_unavailable"}
    assert report["categories"]["attempt_obligations_missing_queue_uuid"] == []
    assert "secret" not in json.dumps(report)
