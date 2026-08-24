from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace

import pytest

import app.services.library_adoption_runner as runner_module
from app.services.library_adoption_runner import LibraryAdoptionRunner


@pytest.mark.asyncio
async def test_runner_retries_after_transient_outer_loop_failure(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    attempts = 0
    completed = asyncio.Event()

    @asynccontextmanager
    async def session_factory():
        yield object()

    async def recover(_db):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise RuntimeError("transient database failure")
        return [7]

    async def effective(_db, _settings):
        return SimpleNamespace(library_root=tmp_path)

    async def run(_db, *, scan_id, library_root):
        assert scan_id == 7
        assert library_root == tmp_path
        completed.set()

    monkeypatch.setattr(runner_module, "recover_library_adoption_scans", recover)
    monkeypatch.setattr(runner_module, "build_effective_settings", effective)
    monkeypatch.setattr(runner_module, "run_library_adoption_scan", run)
    monkeypatch.setattr(runner_module, "get_settings", lambda: object())

    runner = LibraryAdoptionRunner(session_factory, interval_seconds=0.01)  # type: ignore[arg-type]
    await runner.start()
    try:
        await asyncio.wait_for(completed.wait(), timeout=1)
    finally:
        await runner.stop()

    assert attempts >= 2


async def test_start_can_wait_for_initial_cycle(monkeypatch) -> None:
    cycles: list[str] = []

    @asynccontextmanager
    async def session_factory():
        yield object()

    async def recover(_db):
        cycles.append("initial")
        return []

    monkeypatch.setattr(runner_module, "recover_library_adoption_scans", recover)
    runner = LibraryAdoptionRunner(session_factory, interval_seconds=60)  # type: ignore[arg-type]
    await asyncio.wait_for(runner.start(wait_for_initial_cycle=True), timeout=1)
    try:
        assert cycles == ["initial"]
    finally:
        await runner.stop()
