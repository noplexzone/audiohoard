"""Materialize legacy denied slskd provenance as exact active blocks.

Revision ID: 0032
Revises: 0031
Create Date: 2026-08-24
"""

from __future__ import annotations

import json
import re
from collections import defaultdict
from typing import Any

import sqlalchemy as sa

from alembic import op

revision = "0032"
down_revision = "0031"
branch_labels = None
depends_on = None

_DRIVE_PREFIX = re.compile(r"^([A-Za-z]:)(.*)$")


def _collapse_path_parts(parts: list[str], *, anchored: bool) -> list[str]:
    normalized: list[str] = []
    for part in parts:
        if not part or part == ".":
            continue
        if part == "..":
            if normalized and normalized[-1] != "..":
                normalized.pop()
            elif not anchored:
                normalized.append(part)
            continue
        normalized.append(part)
    return normalized


def _normalize_remote_path(value: str) -> str:
    path = value.strip().replace("\\", "/")
    if not path:
        return ""
    if path.startswith("//"):
        components = [part for part in path[2:].split("/") if part]
        if len(components) < 3 or any(anchor in {"", ".", ".."} for anchor in components[:2]):
            return ""
        server, share, *tail = components
        normalized_tail = _collapse_path_parts(tail, anchored=True)
        if not normalized_tail:
            return ""
        return "//" + "/".join([server, share, *normalized_tail])
    drive_match = _DRIVE_PREFIX.match(path)
    if drive_match is not None:
        drive, remainder = drive_match.groups()
        absolute = remainder.startswith("/")
        normalized = _collapse_path_parts(remainder.split("/"), anchored=absolute)
        if not normalized or all(part == ".." for part in normalized):
            return ""
        separator = "/" if absolute else ""
        return drive + separator + "/".join(normalized)
    if path.startswith("/"):
        normalized = _collapse_path_parts(path.split("/"), anchored=True)
        if not normalized:
            return ""
        return "/" + "/".join(normalized)
    normalized = _collapse_path_parts(path.split("/"), anchored=False)
    if not normalized or all(part == ".." for part in normalized):
        return ""
    return "/".join(normalized)


def _normalize_identity(
    provider: object, peer: object, remote_path: object
) -> tuple[str, str, str] | None:
    normalized_provider = str(provider or "").strip().casefold()
    normalized_peer = str(peer or "").strip()
    normalized_path = _normalize_remote_path(str(remote_path or ""))
    if normalized_provider != "slskd" or not normalized_peer or not normalized_path:
        return None
    return normalized_provider, normalized_peer, normalized_path


def _canonicalize_existing_blocks(
    bind: sa.Connection, denied_identities: set[tuple[str, str, str]]
) -> set[tuple[str, str, str]]:
    rows = bind.execute(
        sa.text(
            "SELECT id, provider, peer, filename, reason, retry_count, "
            "last_failure_at, blocked_until "
            "FROM source_candidate_blocks ORDER BY id"
        )
    ).mappings()
    groups: defaultdict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        identity = _normalize_identity(row["provider"], row["peer"], row["filename"])
        if identity is not None:
            groups[identity].append(dict(row))

    for identity, duplicates in groups.items():
        keeper = max(
            duplicates,
            key=lambda row: (
                row["blocked_until"] is None,
                str(row["blocked_until"] or ""),
                int(row["retry_count"] or 0),
                str(row["last_failure_at"] or ""),
                -int(row["id"]),
            ),
        )
        if identity in denied_identities:
            retry_count = max(int(row["retry_count"] or 0) for row in duplicates)
            last_failure_at = max(
                (row["last_failure_at"] for row in duplicates),
                key=lambda value: str(value or ""),
            )
            bind.execute(
                sa.text(
                    "UPDATE source_candidate_blocks SET reason = 'denied', blocked_until = NULL, "
                    "retry_count = :retry_count, last_failure_at = :last_failure_at "
                    "WHERE id = :id"
                ),
                {
                    "retry_count": retry_count,
                    "last_failure_at": last_failure_at,
                    "id": keeper["id"],
                },
            )
        duplicate_ids = [row["id"] for row in duplicates if row["id"] != keeper["id"]]
        if duplicate_ids:
            bind.execute(
                sa.text("DELETE FROM source_candidate_blocks WHERE id IN :ids").bindparams(
                    sa.bindparam("ids", expanding=True)
                ),
                {"ids": duplicate_ids},
            )
        bind.execute(
            sa.text(
                "UPDATE source_candidate_blocks "
                "SET provider = :provider, peer = :peer, filename = :filename WHERE id = :id"
            ),
            {
                "provider": identity[0],
                "peer": identity[1],
                "filename": identity[2],
                "id": keeper["id"],
            },
        )
    return set(groups)


def _legacy_denied_identities(bind: sa.Connection) -> set[tuple[str, str, str]]:
    identities: set[tuple[str, str, str]] = set()
    provenance_rows = bind.execute(
        sa.text(
            "SELECT acquisition_provenance_json FROM tracks "
            "WHERE source = 'slskd' "
            "AND acoustid_verification_state = 'denied' "
            "AND acquisition_provenance_json IS NOT NULL"
        )
    ).scalars()
    for provenance_json in provenance_rows:
        try:
            provenance = json.loads(provenance_json)
        except (json.JSONDecodeError, TypeError):
            continue
        if not isinstance(provenance, dict):
            continue
        identity = _normalize_identity(
            provenance.get("source"), provenance.get("username"), provenance.get("filename")
        )
        if identity is not None:
            identities.add(identity)
    return identities


def upgrade() -> None:
    bind = op.get_bind()
    denied_identities = _legacy_denied_identities(bind)
    existing_identities = _canonicalize_existing_blocks(bind, denied_identities)
    for identity in denied_identities - existing_identities:
        bind.execute(
            sa.text(
                "INSERT INTO source_candidate_blocks (provider, peer, filename, reason) "
                "VALUES (:provider, :peer, :filename, 'denied')"
            ),
            {"provider": identity[0], "peer": identity[1], "filename": identity[2]},
        )


def downgrade() -> None:
    # Data preserving: migrated policy rows cannot be distinguished safely from user-created rows.
    pass
