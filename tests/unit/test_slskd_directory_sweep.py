from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta
from pathlib import Path

from app.services.acquisition_cleanup import (
    inspect_empty_slskd_directories,
    sweep_empty_slskd_directories,
)


class SnapshotAdapter:
    def __init__(self, snapshot=None, error: Exception | None = None):
        self.snapshot = snapshot or []
        self.error = error
        self.calls: list[bool] = []

    async def downloads(self, *, force_refresh: bool = False):
        self.calls.append(force_refresh)
        if self.error:
            raise self.error
        return self.snapshot


def _old(path: Path, *, hours: int = 48) -> None:
    stamp = (datetime.now(UTC) - timedelta(hours=hours)).timestamp()
    os.utime(path, (stamp, stamp), follow_symlinks=False)


async def test_sweeps_old_empty_complete_and_incomplete_directories_bottom_up(
    tmp_path: Path,
) -> None:
    complete = tmp_path / "complete"
    incomplete = tmp_path / "incomplete"
    leaf = complete / "artist" / "album"
    partial = incomplete / "peer"
    leaf.mkdir(parents=True)
    partial.mkdir(parents=True)
    for path in (leaf, leaf.parent, partial):
        _old(path)

    result = await sweep_empty_slskd_directories(
        SnapshotAdapter([]), (complete, incomplete), minimum_age=timedelta(hours=24)
    )

    assert result.snapshot_available is True
    assert set(result.removed) == {leaf, leaf.parent, partial}
    assert complete.is_dir() and incomplete.is_dir()
    assert not leaf.exists() and not partial.exists()


async def test_sweep_preserves_nonempty_symlink_new_and_active_directories(tmp_path: Path) -> None:
    root = tmp_path / "incomplete"
    nonempty = root / "nonempty"
    linked = root / "linked"
    new = root / "new"
    active = root / "active"
    outside = tmp_path / "outside"
    for path in (nonempty, linked, new, active, outside):
        path.mkdir(parents=True)
    (nonempty / "unknown.bin").write_bytes(b"keep")
    (linked / "escape").symlink_to(outside, target_is_directory=True)
    for path in (nonempty, linked, active):
        _old(path)
    adapter = SnapshotAdapter(
        [{"id": "transfer", "state": "InProgress", "localPath": str(active / "song.flac")}]
    )

    result = await sweep_empty_slskd_directories(adapter, (root,), minimum_age=timedelta(hours=24))

    assert result.removed == ()
    assert all(path.exists() for path in (nonempty, linked, new, active))
    reasons = {item.path: item.reason for item in result.not_eligible}
    assert reasons[nonempty] == "nonempty"
    assert reasons[linked] == "symlink_content"
    assert reasons[new] == "too_new"
    assert reasons[active] == "active_transfer"
    assert adapter.calls == [True]


async def test_provider_unavailable_skips_sweep_without_guessing(tmp_path: Path) -> None:
    root = tmp_path / "complete"
    candidate = root / "old"
    candidate.mkdir(parents=True)
    _old(candidate)

    result = await sweep_empty_slskd_directories(
        SnapshotAdapter(error=RuntimeError("https://secret:key@example.test")),
        (root,),
        minimum_age=timedelta(hours=24),
    )

    assert result.snapshot_available is False
    assert result.removed == ()
    assert candidate.is_dir()
    assert result.error_code == "provider_unavailable"


async def test_sweep_is_idempotent_and_inspection_never_mutates(tmp_path: Path) -> None:
    root = tmp_path / "complete"
    candidate = root / "old"
    candidate.mkdir(parents=True)
    _old(candidate)
    snapshot: list[dict[str, object]] = []

    report = inspect_empty_slskd_directories((root,), snapshot, minimum_age=timedelta(hours=24))
    assert report.eligible == (candidate,)
    assert candidate.exists()

    adapter = SnapshotAdapter(snapshot)
    first = await sweep_empty_slskd_directories(adapter, (root,), minimum_age=timedelta(hours=24))
    second = await sweep_empty_slskd_directories(adapter, (root,), minimum_age=timedelta(hours=24))
    assert first.removed == (candidate,)
    assert second.removed == ()
    assert root.is_dir()


def test_slskd_sweep_roots_default_disabled_and_accept_explicit_paths(tmp_path: Path) -> None:
    from app.config import Settings

    disabled = Settings(secret_key="test")
    configured = Settings(
        secret_key="test",
        slskd_complete_root=tmp_path / "complete",
        slskd_incomplete_root=tmp_path / "incomplete",
    )
    assert disabled.slskd_complete_root is None
    assert disabled.slskd_incomplete_root is None
    assert configured.slskd_complete_root == tmp_path / "complete"
    assert configured.slskd_incomplete_root == tmp_path / "incomplete"
