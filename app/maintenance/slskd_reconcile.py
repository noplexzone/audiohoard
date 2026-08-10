from __future__ import annotations

import argparse
import asyncio
import contextlib
import hashlib
import json
import os
import sqlite3
import stat
from collections.abc import Iterator
from datetime import timedelta
from pathlib import Path
from typing import Protocol
from urllib.parse import quote

from app.config import Settings, get_settings
from app.crypto import decrypt_secret
from app.services.acquisition_attempts import canonical_provider_uuid
from app.services.acquisition_cleanup import inspect_empty_slskd_directories
from app.sources.slskd import SlskdAdapter

_SCHEMA_VERSION = 1
_MAX_LIMIT = 1000
_CATEGORY_NAMES = (
    "queue_uuids_with_attempt_owner",
    "queue_rows_without_attempt_owner",
    "attempt_obligations_missing_queue_uuid",
    "attempt_obligations_live_queue_uuid",
    "attempt_obligations_mismatched_queue_uuid",
    "exact_artifact_present",
    "exact_artifact_missing",
    "exact_artifact_mismatched",
    "empty_directories_eligible",
    "empty_directories_not_eligible",
    "unmatched_terminal_transfers",
    "ambiguous_attempts",
    "review_import_debt",
    "unreferenced_complete_files",
    "incomplete_tree_entries",
)


class SnapshotAdapter(Protocol):
    async def downloads(self, *, force_refresh: bool = False) -> list[dict[str, object]]: ...


@contextlib.contextmanager
def open_readonly(database: Path) -> Iterator[sqlite3.Connection]:
    """Open SQLite in URI read-only and query-only modes."""
    resolved = database.expanduser().resolve(strict=True)
    uri = f"file:{quote(str(resolved))}?mode=ro"
    connection = sqlite3.connect(uri, uri=True, isolation_level=None)
    connection.row_factory = sqlite3.Row
    try:
        connection.execute("PRAGMA query_only=ON")
        yield connection
    finally:
        connection.close()


def _table_exists(connection: sqlite3.Connection, table: str) -> bool:
    row = connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?", (table,)
    ).fetchone()
    return row is not None


def _effective_settings(connection: sqlite3.Connection, settings: Settings) -> Settings:
    if not _table_exists(connection, "provider_settings"):
        return settings
    rows = connection.execute(
        "SELECT key, value_plain, value_encrypted FROM provider_settings"
    ).fetchall()
    stored: dict[str, str] = {}
    for row in rows:
        key = str(row["key"])
        if row["value_plain"] is not None:
            stored[key] = str(row["value_plain"])
        elif row["value_encrypted"] is not None:
            try:
                stored[key] = decrypt_secret(str(row["value_encrypted"]), settings.secret_key)
            except Exception:
                continue
    updates: dict[str, object] = {}
    for key in (
        "slskd_url",
        "slskd_api_key",
        "slskd_complete_root",
        "slskd_incomplete_root",
    ):
        if key in settings.model_fields_set or not stored.get(key):
            continue
        updates[key] = Path(stored[key]) if key.endswith("_root") else stored[key]
    return settings.model_copy(update=updates)


def _attempt_rows(connection: sqlite3.Connection) -> list[sqlite3.Row]:
    if not _table_exists(connection, "acquisition_attempts"):
        return []
    return list(
        connection.execute(
            """
            SELECT id, job_id, peer, remote_path, provider_uuid, provider_state,
                   provider_cleanup_state, file_cleanup_state, file_cleanup_eligible,
                   retention_disposition, outcome, terminal_at, staged_path, partial_path,
                   artifact_device, artifact_inode, artifact_mtime_ns, artifact_size,
                   artifact_sha256
            FROM acquisition_attempts
            WHERE provider = 'slskd'
            ORDER BY id
            """
        )
    )


def _snapshot_uuid(item: dict[str, object]) -> str | None:
    return canonical_provider_uuid(item.get("id") or item.get("transferId"))


def _snapshot_is_terminal(item: dict[str, object]) -> bool:
    terminal = {
        "completed",
        "complete",
        "succeeded",
        "failed",
        "cancelled",
        "canceled",
        "timedout",
        "timed_out",
    }
    values: list[str] = []

    def collect(value: object) -> None:
        if isinstance(value, dict):
            for key, child in value.items():
                if str(key).casefold() in {"state", "status", "downloadstate"}:
                    values.append(str(child).casefold().replace(" ", "").replace("-", "_"))
                collect(child)
        elif isinstance(value, list):
            for child in value:
                collect(child)

    collect(item)
    return any(value in terminal for value in values)


def _reference_paths(connection: sqlite3.Connection, attempts: list[sqlite3.Row]) -> set[Path]:
    paths = {
        Path(str(value)).absolute()
        for row in attempts
        for value in (row["staged_path"], row["partial_path"])
        if value
    }
    for table, columns in (
        ("tracks", ("source_path", "staging_path")),
        (
            "import_plans",
            ("source_path", "staging_path", "destination_path", "destination_temp_path"),
        ),
    ):
        if not _table_exists(connection, table):
            continue
        available = {str(row[1]) for row in connection.execute(f"PRAGMA table_info({table})")}
        selected = [column for column in columns if column in available]
        if not selected:
            continue
        query = "SELECT " + ", ".join(selected) + f" FROM {table}"
        for row in connection.execute(query):
            paths.update(Path(str(value)).absolute() for value in row if value)
    return paths


def _review_import_debt(connection: sqlite3.Connection) -> list[dict[str, object]]:
    if not _table_exists(connection, "import_plans"):
        return []
    return [
        {"plan_id": int(row["id"]), "status": str(row["status"])}
        for row in connection.execute(
            """
            SELECT id, status FROM import_plans
            WHERE status IN ('needs_review', 'ready', 'importing')
            ORDER BY id
            """
        )
    ]


def _root_entries(root: Path) -> tuple[list[Path], list[tuple[Path, str]]]:
    files: list[Path] = []
    entries: list[tuple[Path, str]] = []
    try:
        configured = root.resolve(strict=True)
    except OSError:
        return files, entries
    if configured.is_symlink() or not configured.is_dir():
        return files, entries
    for current, dirs, names in os.walk(configured, topdown=True, followlinks=False):
        current_path = Path(current)
        dirs[:] = sorted(name for name in dirs if not (current_path / name).is_symlink())
        for name in dirs:
            entries.append((current_path / name, "directory"))
        for name in sorted(names):
            candidate = current_path / name
            try:
                current_stat = candidate.stat(follow_symlinks=False)
            except OSError:
                continue
            if stat.S_ISREG(current_stat.st_mode):
                files.append(candidate)
                entries.append((candidate, "file"))
    return files, entries


def _normalized_path(value: object) -> str:
    return str(value or "").replace("\\", "/")


def _path_exists(path: Path) -> bool:
    return path.exists() or path.is_symlink()


def _artifact_identity(path: Path) -> tuple[int, int, int, int, str] | None:
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    try:
        fd = os.open(path, flags)
    except OSError:
        return None
    try:
        current = os.fstat(fd)
        if not stat.S_ISREG(current.st_mode):
            return None
        digest = hashlib.sha256()
        while chunk := os.read(fd, 1024 * 1024):
            digest.update(chunk)
        return (
            current.st_dev,
            current.st_ino,
            current.st_mtime_ns,
            current.st_size,
            digest.hexdigest(),
        )
    finally:
        os.close(fd)


def _safe_path(path: Path, roots: tuple[tuple[str, Path], ...]) -> dict[str, object]:
    absolute = path.absolute()
    for label, root in roots:
        configured = root.absolute()
        if absolute.is_relative_to(configured):
            relative = absolute.relative_to(configured)
            return {"root": label, "path": relative.as_posix() or "."}
    return {"root": "outside_configured_roots", "path": "<redacted>"}


def _bounded(
    categories: dict[str, list[dict[str, object]]], limit: int
) -> tuple[dict[str, int], bool]:
    counts = {key: len(values) for key, values in categories.items()}
    truncated = any(count > limit for count in counts.values())
    for key in categories:
        categories[key] = categories[key][:limit]
    return counts, truncated


async def build_report(
    database: Path,
    *,
    settings: Settings,
    adapter: SnapshotAdapter | None = None,
    limit: int = 200,
) -> dict[str, object]:
    """Build a bounded reconciliation report without any provider, DB, or filesystem mutation."""
    limit = max(1, min(limit, _MAX_LIMIT))
    categories: dict[str, list[dict[str, object]]] = {name: [] for name in _CATEGORY_NAMES}
    with open_readonly(database) as connection:
        effective = _effective_settings(connection, settings)
        attempts = _attempt_rows(connection)
        referenced_paths = _reference_paths(connection, attempts)
        review_debt = _review_import_debt(connection)

    provider: dict[str, str]
    snapshot: list[dict[str, object]] | None = None
    source = adapter
    if source is None and effective.slskd_configured:
        source = SlskdAdapter(effective.slskd_url, effective.slskd_api_key)
    if source is None:
        provider = {"status": "disabled", "error_code": "provider_not_configured"}
    else:
        try:
            snapshot = await source.downloads(force_refresh=True)
            provider = {"status": "available"}
        except asyncio.CancelledError:
            raise
        except Exception:
            provider = {"status": "unavailable", "error_code": "provider_unavailable"}

    attempts_by_uuid = {
        str(row["provider_uuid"]): row
        for row in attempts
        if canonical_provider_uuid(row["provider_uuid"]) is not None
    }
    snapshot_by_uuid: dict[str, dict[str, object]] = {}
    if snapshot is not None:
        for queue_item in snapshot:
            provider_uuid = _snapshot_uuid(queue_item)
            if provider_uuid is None:
                categories["queue_rows_without_attempt_owner"].append({"provider_uuid": None})
                continue
            snapshot_by_uuid[provider_uuid] = queue_item
            owner = attempts_by_uuid.get(provider_uuid)
            if owner is None:
                categories["queue_rows_without_attempt_owner"].append(
                    {"provider_uuid": provider_uuid}
                )
                if _snapshot_is_terminal(queue_item):
                    categories["unmatched_terminal_transfers"].append(
                        {"provider_uuid": provider_uuid}
                    )
            else:
                categories["queue_uuids_with_attempt_owner"].append(
                    {"provider_uuid": provider_uuid, "attempt_id": int(owner["id"])}
                )
        for row in attempts:
            if str(row["provider_cleanup_state"]) in {"completed", "not_required"}:
                continue
            attempt_id = int(row["id"])
            provider_uuid = canonical_provider_uuid(row["provider_uuid"])
            if provider_uuid is None or provider_uuid not in snapshot_by_uuid:
                categories["attempt_obligations_missing_queue_uuid"].append(
                    {"attempt_id": attempt_id, "provider_uuid": provider_uuid}
                )
                continue
            item = snapshot_by_uuid[provider_uuid]
            if str(item.get("username") or "") != str(row["peer"] or "") or _normalized_path(
                item.get("filename")
            ) != _normalized_path(row["remote_path"]):
                categories["attempt_obligations_mismatched_queue_uuid"].append(
                    {"attempt_id": attempt_id, "provider_uuid": provider_uuid}
                )
            else:
                categories["attempt_obligations_live_queue_uuid"].append(
                    {"attempt_id": attempt_id, "provider_uuid": provider_uuid}
                )

    for row in attempts:
        if canonical_provider_uuid(row["provider_uuid"]) is None and str(
            row["provider_cleanup_state"]
        ) not in {"completed", "not_required"}:
            categories["ambiguous_attempts"].append(
                {
                    "attempt_id": int(row["id"]),
                    "reason": "missing_canonical_provider_uuid",
                }
            )
    categories["review_import_debt"].extend(review_debt)

    roots = tuple(
        (label, root)
        for label, root in (
            ("complete", effective.slskd_complete_root),
            ("incomplete", effective.slskd_incomplete_root),
        )
        if root is not None
    )
    for row in attempts:
        if str(row["file_cleanup_state"]) in {"completed", "not_required"}:
            continue
        attempt_id = int(row["id"])
        raw_path = row["staged_path"] or row["partial_path"]
        if not raw_path:
            if bool(row["file_cleanup_eligible"]) or str(row["retention_disposition"]) == (
                "cleanup_eligible"
            ):
                categories["exact_artifact_mismatched"].append(
                    {"attempt_id": attempt_id, "reason": "path_unbound"}
                )
            continue
        path = Path(str(raw_path))
        safe = _safe_path(path, roots)
        try:
            exists = await asyncio.to_thread(_path_exists, path)
        except OSError:
            exists = False
        if not exists:
            categories["exact_artifact_missing"].append({"attempt_id": attempt_id, **safe})
            continue
        expected_values = (
            row["artifact_device"],
            row["artifact_inode"],
            row["artifact_mtime_ns"],
            row["artifact_size"],
            row["artifact_sha256"],
        )
        current = await asyncio.to_thread(_artifact_identity, path)
        if any(value is None for value in expected_values) or current != expected_values:
            categories["exact_artifact_mismatched"].append(
                {"attempt_id": attempt_id, "reason": "identity_mismatch", **safe}
            )
        else:
            categories["exact_artifact_present"].append({"attempt_id": attempt_id, **safe})

    if effective.slskd_complete_root is not None:
        complete_files, _complete_entries = await asyncio.to_thread(
            _root_entries, effective.slskd_complete_root
        )
        for path in complete_files:
            if path.absolute() not in referenced_paths:
                categories["unreferenced_complete_files"].append(_safe_path(path, roots))
    if effective.slskd_incomplete_root is not None:
        _incomplete_files, incomplete_entries = await asyncio.to_thread(
            _root_entries, effective.slskd_incomplete_root
        )
        for path, kind in incomplete_entries:
            categories["incomplete_tree_entries"].append({**_safe_path(path, roots), "kind": kind})

    configured_roots = tuple(root for _, root in roots)
    if snapshot is not None and configured_roots:
        inspection = inspect_empty_slskd_directories(
            configured_roots,
            snapshot,
            minimum_age=timedelta(seconds=effective.slskd_directory_sweep_min_age_seconds),
        )
        for path in inspection.eligible:
            eligible_row: dict[str, object] = _safe_path(path, roots)
            categories["empty_directories_eligible"].append(eligible_row)
        for sweep_item in inspection.not_eligible:
            categories["empty_directories_not_eligible"].append(
                {**_safe_path(sweep_item.path, roots), "reason": sweep_item.reason}
            )
    elif configured_roots:
        for label, _root in roots:
            categories["empty_directories_not_eligible"].append(
                {"root": label, "path": ".", "reason": "provider_snapshot_unavailable"}
            )

    for values in categories.values():
        values.sort(key=lambda item: json.dumps(item, sort_keys=True))
    counts, truncated = _bounded(categories, limit)
    return {
        "schema_version": _SCHEMA_VERSION,
        "mode": "report_only",
        "database": {"status": "available", "query_only": True},
        "provider": provider,
        "categories": categories,
        "counts": counts,
        "limit": limit,
        "truncated": truncated,
    }


def _database_path(value: str) -> Path:
    for prefix in ("sqlite+aiosqlite:///", "sqlite:///"):
        if value.startswith(prefix):
            value = value[len(prefix) :]
            break
    if "://" in value or value == ":memory:":
        raise ValueError("report requires a file-backed SQLite database")
    return Path(value).expanduser().resolve()


def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Print a non-mutating JSON slskd ownership and filesystem reconciliation report."
        )
    )
    parser.add_argument(
        "--database",
        help="SQLite path/URL; defaults to configured DATABASE_URL",
    )
    parser.add_argument("--limit", type=int, default=200, choices=range(1, _MAX_LIMIT + 1))
    return parser


async def _main(argv: list[str] | None = None) -> int:
    parser = create_parser()
    args = parser.parse_args(argv)
    settings = get_settings()
    try:
        database = _database_path(args.database or settings.database_url)
        report = await build_report(database, settings=settings, limit=args.limit)
    except (FileNotFoundError, ValueError, sqlite3.Error) as exc:
        parser.error(str(exc))
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


def main(argv: list[str] | None = None) -> int:
    return asyncio.run(_main(argv))


if __name__ == "__main__":
    raise SystemExit(main())
