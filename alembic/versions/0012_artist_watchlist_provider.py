"""Normalize artist provider identities and provider-native release snapshots.

Revision ID: 0012
Revises: 0011
Create Date: 2026-07-26
"""

from __future__ import annotations

import json
from collections.abc import Mapping

import sqlalchemy as sa

from alembic import op

revision = "0012"
down_revision = "0011"
branch_labels = None
depends_on = None

_PROVIDER_FIELDS = (
    ("musicbrainz", "mbid"),
    ("deezer", "deezer_id"),
    ("itunes", "itunes_id"),
)


def _tables() -> set[str]:
    return set(sa.inspect(op.get_bind()).get_table_names())


def _cols(table: str) -> set[str]:
    return {column["name"] for column in sa.inspect(op.get_bind()).get_columns(table)}


def _indexes(table: str) -> set[str]:
    return {
        str(index["name"])
        for index in sa.inspect(op.get_bind()).get_indexes(table)
        if index.get("name")
    }


def _release_kind(raw: object) -> str:
    value = str(raw or "").casefold()
    if "compilation" in value:
        return "compilation"
    if "single" in value:
        return "single"
    if value.strip() in {"ep", "e.p."} or " ep" in value:
        return "ep"
    if "album" in value:
        return "album"
    return "unknown"


def _providers(raw: object) -> set[str]:
    try:
        parsed = json.loads(str(raw or "[]"))
    except (json.JSONDecodeError, TypeError):
        return set()
    return (
        {str(value) for value in parsed if str(value) in dict(_PROVIDER_FIELDS)}
        if isinstance(parsed, list)
        else set()
    )


def _create_tables() -> None:
    tables = _tables()
    if "catalog_artist_identities" not in tables:
        op.create_table(
            "catalog_artist_identities",
            sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
            sa.Column(
                "artist_id",
                sa.Integer(),
                sa.ForeignKey("catalog_artists.id", ondelete="CASCADE"),
                nullable=False,
            ),
            sa.Column("provider", sa.String(32), nullable=False),
            sa.Column("provider_artist_id", sa.String(128), nullable=False),
            sa.Column("name", sa.Text(), nullable=False),
            sa.Column("artwork_url", sa.Text(), nullable=True),
            sa.Column("metadata_json", sa.Text(), nullable=True),
            sa.Column("provenance_json", sa.Text(), nullable=True),
            sa.Column("last_enriched_at", sa.DateTime(timezone=True), nullable=True),
            sa.Column("last_discography_at", sa.DateTime(timezone=True), nullable=True),
            sa.Column(
                "created_at",
                sa.DateTime(timezone=True),
                server_default=sa.func.now(),
                nullable=False,
            ),
            sa.Column(
                "updated_at",
                sa.DateTime(timezone=True),
                server_default=sa.func.now(),
                nullable=False,
            ),
            sa.UniqueConstraint(
                "provider", "provider_artist_id", name="uq_artist_identity_provider_id"
            ),
            sa.UniqueConstraint(
                "artist_id", "provider", name="uq_artist_identity_artist_provider"
            ),
        )
    tables = _tables()
    if "catalog_album_providers" not in tables:
        op.create_table(
            "catalog_album_providers",
            sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
            sa.Column(
                "artist_identity_id",
                sa.Integer(),
                sa.ForeignKey("catalog_artist_identities.id", ondelete="CASCADE"),
                nullable=False,
            ),
            sa.Column(
                "catalog_album_id",
                sa.Integer(),
                sa.ForeignKey("catalog_albums.id", ondelete="SET NULL"),
                nullable=True,
            ),
            sa.Column("provider_album_id", sa.String(128), nullable=False),
            sa.Column("title", sa.Text(), nullable=False),
            sa.Column("year", sa.String(4), nullable=True),
            sa.Column("artwork_url", sa.Text(), nullable=True),
            sa.Column("track_count", sa.Integer(), nullable=True),
            sa.Column("release_kind", sa.String(32), server_default="unknown", nullable=False),
            sa.Column("release_type_raw", sa.String(128), nullable=True),
            sa.Column("metadata_json", sa.Text(), nullable=True),
            sa.Column("monitored", sa.Boolean(), server_default=sa.false(), nullable=False),
            sa.Column(
                "created_at",
                sa.DateTime(timezone=True),
                server_default=sa.func.now(),
                nullable=False,
            ),
            sa.Column(
                "updated_at",
                sa.DateTime(timezone=True),
                server_default=sa.func.now(),
                nullable=False,
            ),
            sa.UniqueConstraint(
                "artist_identity_id",
                "provider_album_id",
                name="uq_album_provider_identity_release",
            ),
        )
    for table, name, columns in (
        ("catalog_artist_identities", "ix_catalog_artist_identities_artist_id", ["artist_id"]),
        ("catalog_artist_identities", "ix_catalog_artist_identities_provider", ["provider"]),
        (
            "catalog_album_providers",
            "ix_catalog_album_providers_identity_id",
            ["artist_identity_id"],
        ),
        (
            "catalog_album_providers",
            "ix_catalog_album_providers_catalog_album_id",
            ["catalog_album_id"],
        ),
    ):
        if name not in _indexes(table):
            op.create_index(name, table, columns)


def _identity_id(
    bind: sa.Connection, artist: Mapping[str, object], provider: str, provider_id: str
) -> int:
    existing = bind.execute(
        sa.text(
            "SELECT id FROM catalog_artist_identities "
            "WHERE artist_id=:artist_id AND provider=:provider"
        ),
        {"artist_id": artist["id"], "provider": provider},
    ).scalar_one_or_none()
    if existing is None:
        bind.execute(
            sa.text(
                "INSERT INTO catalog_artist_identities "
                "(artist_id, provider, provider_artist_id, name, artwork_url, "
                "metadata_json, provenance_json) "
                "VALUES (:artist_id, :provider, :provider_id, :name, :artwork, "
                ":metadata, :provenance)"
            ),
            {
                "artist_id": artist["id"],
                "provider": provider,
                "provider_id": provider_id,
                "name": artist["name"],
                "artwork": artist.get("artwork_url"),
                "metadata": json.dumps({"legacy_backfill": True, "lossy": True}),
                "provenance": json.dumps({"source": "0012_legacy_backfill"}),
            },
        )
        existing = bind.execute(
            sa.text(
                "SELECT id FROM catalog_artist_identities "
                "WHERE artist_id=:artist_id AND provider=:provider"
            ),
            {"artist_id": artist["id"], "provider": provider},
        ).scalar_one()
    return int(existing)


def _backfill() -> None:
    bind = op.get_bind()
    artists = list(bind.execute(sa.text("SELECT * FROM catalog_artists")).mappings())
    albums_by_artist: dict[int, list[Mapping[str, object]]] = {}
    for album in bind.execute(sa.text("SELECT * FROM catalog_albums")).mappings():
        albums_by_artist.setdefault(int(album["artist_id"]), []).append(album)

    for artist in artists:
        identity_ids: dict[str, int] = {}
        inferred = set()
        for album in albums_by_artist.get(int(artist["id"]), []):
            inferred.update(_providers(album.get("providers_json")))
            for provider, field in _PROVIDER_FIELDS:
                if album.get(field):
                    inferred.add(provider)
        for provider, field in _PROVIDER_FIELDS:
            provider_id = artist.get(field)
            if provider_id:
                identity_ids[provider] = _identity_id(bind, artist, provider, str(provider_id))
        for provider in inferred:
            if provider not in identity_ids:
                identity_ids[provider] = _identity_id(
                    bind, artist, provider, f"legacy:artist:{artist['id']}:{provider}"
                )

        selected = next(
            (provider for provider, _ in _PROVIDER_FIELDS if provider in identity_ids), None
        )
        if selected and artist.get("monitored"):
            bind.execute(
                sa.text("UPDATE catalog_artists SET watchlist_provider=:provider WHERE id=:id"),
                {"provider": selected, "id": artist["id"]},
            )

        for album in albums_by_artist.get(int(artist["id"]), []):
            album_providers = _providers(album.get("providers_json"))
            album_providers.update(
                provider for provider, field in _PROVIDER_FIELDS if album.get(field)
            )
            for provider in album_providers:
                identity_id = identity_ids.get(provider)
                if identity_id is None:
                    continue
                field = dict(_PROVIDER_FIELDS)[provider]
                provider_album_id = str(
                    album.get(field) or f"legacy:album:{album['id']}:{provider}"
                )
                monitored = bool(album.get("monitored")) and provider == selected
                bind.execute(
                    sa.text(
                        "INSERT OR IGNORE INTO catalog_album_providers "
                        "(artist_identity_id, catalog_album_id, provider_album_id, "
                        "title, year, artwork_url, "
                        "track_count, release_kind, release_type_raw, metadata_json, monitored) "
                        "VALUES (:identity_id, :album_id, :provider_album_id, :title, "
                        ":year, :artwork, "
                        ":track_count, :kind, :raw, :metadata, :monitored)"
                    ),
                    {
                        "identity_id": identity_id,
                        "album_id": album["id"],
                        "provider_album_id": provider_album_id,
                        "title": album["title"],
                        "year": album.get("year"),
                        "artwork": album.get("artwork_url"),
                        "track_count": album.get("track_count"),
                        "kind": _release_kind(album.get("release_type")),
                        "raw": album.get("release_type"),
                        "metadata": json.dumps(
                            {"legacy_backfill": True, "lossy": True, "refresh_required": True}
                        ),
                        "monitored": monitored,
                    },
                )


def upgrade() -> None:
    if "watchlist_provider" not in _cols("catalog_artists"):
        with op.batch_alter_table("catalog_artists") as batch:
            batch.add_column(sa.Column("watchlist_provider", sa.String(32), nullable=True))
    _create_tables()
    _backfill()


def downgrade() -> None:
    tables = _tables()
    if "catalog_album_providers" in tables:
        op.drop_table("catalog_album_providers")
    if "catalog_artist_identities" in tables:
        op.drop_table("catalog_artist_identities")
    if "watchlist_provider" in _cols("catalog_artists"):
        with op.batch_alter_table("catalog_artists") as batch:
            batch.drop_column("watchlist_provider")
