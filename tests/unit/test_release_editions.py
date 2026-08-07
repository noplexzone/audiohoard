from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.catalog_entities import (
    CatalogAlbum,
    CatalogAlbumProvider,
    CatalogArtist,
    CatalogArtistIdentity,
)
from app.services.release_editions import (
    apply_release_monitoring_policy,
    project_release_families,
    reconcile_release_monitoring,
    release_family_key,
)


def _release(
    release_id: int,
    *,
    provider: str = "deezer",
    identity_id: int = 1,
    title: str = "Album",
    year: str | None = "2024",
    kind: str = "album",
    rating: str = "unknown",
    tracks: int | None = 10,
    override: bool | None = None,
    artwork: str | None = None,
) -> CatalogAlbumProvider:
    release = CatalogAlbumProvider(
        id=release_id,
        artist_identity_id=identity_id,
        provider_album_id=f"{provider}-{release_id}",
        title=title,
        year=year,
        release_kind=kind,
        content_rating=rating,
        track_count=tracks,
        monitor_override=override,
        artwork_url=artwork,
    )
    release.artist_identity = CatalogArtistIdentity(
        id=identity_id,
        artist_id=1,
        provider=provider,
        provider_artist_id=f"{provider}-artist",
        name="Artist",
    )
    return release


def _artist(**values: object) -> CatalogArtist:
    defaults: dict[str, object] = {
        "name": "Artist",
        "monitored": True,
        "watchlist_provider": "deezer",
        "monitor_policy": "all",
        "watchlist_release_albums": True,
        "watchlist_release_singles": True,
        "watchlist_release_eps": True,
    }
    defaults.update(values)
    return CatalogArtist(**defaults)


def test_release_family_projection_groups_rating_siblings_but_preserves_boundaries() -> None:
    siblings = [
        _release(1, rating="explicit"),
        _release(2, rating="clean"),
        _release(3, rating="not_explicit"),
        _release(4, rating="unknown", tracks=None),
    ]
    boundaries = [
        _release(5, title="Album (Deluxe)", rating="explicit"),
        _release(6, title="Album (2024 Remaster)", rating="explicit"),
        _release(7, title="Album (Live)", rating="explicit"),
        _release(8, year="2023", rating="explicit"),
        _release(9, kind="ep", rating="explicit"),
        _release(10, tracks=11, rating="explicit"),
        _release(11, provider="itunes", identity_id=2, rating="explicit"),
    ]

    families = project_release_families([*siblings, *boundaries])

    assert [release.id for release in families[0].releases] == [1, 2, 3]
    assert [release.id for release in families[1].releases] == [10]
    assert [release.id for release in families[2].releases] == [4]
    assert len(families) == 9
    assert release_family_key(siblings[0]) == release_family_key(siblings[1])
    assert release_family_key(siblings[0]) != release_family_key(boundaries[0])
    assert release_family_key(siblings[0]) != release_family_key(boundaries[-1])


def test_release_family_projection_ignores_rating_labels_in_title() -> None:
    families = project_release_families(
        [
            _release(1, title="Album (Explicit)", rating="explicit"),
            _release(2, title="Album (Clean)", rating="clean"),
        ]
    )
    assert len(families) == 1


def test_family_display_prefers_single_configured_rating_then_policy_order() -> None:
    explicit = _release(1, rating="explicit", override=False)
    clean = _release(2, rating="clean", override=True)
    unknown = _release(3, rating="unknown", override=False)

    family = project_release_families([explicit, clean, unknown])[0]

    assert [release.id for release in family.selected_representatives] == [2]
    assert family.display_release.id == 2
    assert family.has_overrides is True

    clean.monitor_override = False
    explicit.monitor_override = True
    unknown.monitor_override = True
    family = project_release_families([explicit, clean, unknown])[0]
    assert {release.id for release in family.selected_representatives} == {1, 3}
    assert family.display_release.id == 1


def test_family_display_does_not_target_an_unselected_explicit_edition() -> None:
    explicit = _release(1, rating="explicit", override=False)
    clean = _release(2, rating="clean", override=True)
    unknown = _release(3, rating="unknown", override=True)

    family = project_release_families([explicit, clean, unknown])[0]

    assert {release.id for release in family.selected_representatives} == {2, 3}
    assert family.display_release.id == 3


def test_family_policy_selection_is_explicit_then_unknown_and_clean_only_is_empty() -> None:
    explicit = _release(1, rating="explicit")
    unknown = _release(2, rating="unknown")
    clean = _release(3, rating="clean")

    family = project_release_families([explicit, unknown, clean])[0]
    assert [release.id for release in family.selected_representatives] == [1]
    assert family.display_release.id == 1

    clean_only = project_release_families([_release(4, rating="clean")])[0]
    assert clean_only.selected_representatives == ()
    assert clean_only.display_release.id == 4


def test_release_family_projection_preserves_bare_rating_words_in_real_titles() -> None:
    releases = [
        _release(1, title="Keep It Explicit", rating="explicit"),
        _release(2, title="Keep It", rating="clean"),
        _release(3, title="Coming Clean", rating="clean"),
        _release(4, title="Coming", rating="explicit"),
    ]

    assert len(project_release_families(releases)) == 4
    assert release_family_key(releases[0]) != release_family_key(releases[1])
    assert release_family_key(releases[2]) != release_family_key(releases[3])


def test_release_family_projection_strips_delimited_rating_labels() -> None:
    releases = [
        _release(1, title="Album - Explicit", rating="explicit"),
        _release(2, title="Album: Clean", rating="clean"),
        _release(3, title="Album | Not Explicit", rating="not_explicit"),
    ]

    assert len(project_release_families(releases)) == 1


def test_release_family_projection_preserves_punctuation_and_diacritic_identity() -> None:
    releases = [
        _release(1, title="AC/DC", rating="explicit"),
        _release(2, title="AC DC", rating="clean"),
        _release(3, title="Beyoncé", rating="explicit"),
        _release(4, title="Beyonce", rating="clean"),
    ]

    assert len(project_release_families(releases)) == 4


def test_policy_selects_one_deterministic_explicit_then_unknown_and_never_clean() -> None:
    explicit_plain = _release(1, rating="explicit")
    explicit_rich = _release(2, rating="explicit", artwork="cover")
    unknown = _release(3, rating="unknown")
    clean = _release(4, rating="clean")
    only_unknown = _release(5, title="Unknown Album", rating="unknown")
    unknown_duplicate = _release(6, title="Unknown Album", rating="unknown", tracks=None)
    only_clean = _release(7, title="Clean Album", rating="clean")

    changed = apply_release_monitoring_policy(
        _artist(),
        [
            explicit_plain,
            explicit_rich,
            unknown,
            clean,
            only_unknown,
            unknown_duplicate,
            only_clean,
        ],
    )

    assert changed == 2
    assert [release.monitored for release in [explicit_plain, explicit_rich, unknown, clean]] == [
        False,
        True,
        False,
        False,
    ]
    assert [only_unknown.monitored, unknown_duplicate.monitored, only_clean.monitored] == [
        True,
        False,
        False,
    ]


def test_policy_outer_gates_and_provider_isolation() -> None:
    deezer_album = _release(1, rating="explicit")
    deezer_single = _release(2, title="Single", kind="single", rating="explicit")
    itunes_album = _release(3, provider="itunes", identity_id=2, rating="explicit")

    apply_release_monitoring_policy(
        _artist(watchlist_release_albums=False, watchlist_release_singles=True),
        [deezer_album, deezer_single, itunes_album],
    )
    assert [deezer_album.monitored, deezer_single.monitored, itunes_album.monitored] == [
        False,
        True,
        False,
    ]

    apply_release_monitoring_policy(_artist(monitored=False), [deezer_album, deezer_single])
    assert [deezer_album.monitored, deezer_single.monitored] == [False, False]


def test_manual_family_overrides_survive_new_siblings_and_remain_exact() -> None:
    explicit = _release(1, rating="explicit", override=False)
    clean = _release(2, rating="clean", override=True)
    arriving_unknown = _release(3, rating="unknown")

    apply_release_monitoring_policy(_artist(), [explicit, clean, arriving_unknown])

    assert [explicit.monitored, clean.monitored, arriving_unknown.monitored] == [
        False,
        True,
        False,
    ]


def test_manual_rating_choice_pins_one_same_rating_representative() -> None:
    chosen = _release(1, rating="explicit", override=True)
    richer = _release(2, rating="explicit", override=False, artwork="cover")
    duplicate_true = _release(3, rating="explicit", override=True, artwork="new-cover")

    apply_release_monitoring_policy(_artist(), [chosen, richer])
    assert [chosen.monitored, richer.monitored] == [True, False]

    apply_release_monitoring_policy(_artist(), [chosen, duplicate_true])
    assert sum(release.monitored for release in [chosen, duplicate_true]) == 1


def test_policy_managed_unknown_shifts_to_new_explicit_and_is_idempotent() -> None:
    unknown = _release(1, rating="unknown")
    apply_release_monitoring_policy(_artist(), [unknown])
    assert unknown.monitored is True

    explicit = _release(2, rating="explicit")
    changed = apply_release_monitoring_policy(_artist(), [unknown, explicit])
    assert changed == 2
    assert [unknown.monitored, explicit.monitored] == [False, True]
    assert apply_release_monitoring_policy(_artist(), [unknown, explicit]) == 0


async def test_reconcile_release_monitoring_existing_rows_is_provider_safe_and_idempotent(
    db_session: AsyncSession,
) -> None:
    artist = CatalogArtist(
        name="Reconcile",
        monitored=True,
        watchlist_provider="deezer",
        monitor_policy="all",
        watchlist_release_albums=True,
        watchlist_release_singles=False,
        watchlist_release_eps=False,
    )
    deezer = CatalogArtistIdentity(provider="deezer", provider_artist_id="dz", name=artist.name)
    other = CatalogArtistIdentity(
        provider="musicbrainz", provider_artist_id="mb", name=artist.name
    )
    artist.identities.extend([deezer, other])

    def persisted(
        identity: CatalogArtistIdentity,
        provider_id: str,
        title: str,
        rating: str,
        *,
        kind: str = "album",
        monitored: bool = False,
        override: bool | None = None,
    ) -> CatalogAlbumProvider:
        album = CatalogAlbum(artist=artist, title=f"{title} {provider_id}", release_type=kind)
        row = CatalogAlbumProvider(
            artist_identity=identity,
            catalog_album=album,
            provider_album_id=provider_id,
            title=title,
            year="2024",
            release_kind=kind,
            content_rating=rating,
            track_count=10,
            monitored=monitored,
            monitor_override=override,
        )
        db_session.add(row)
        return row

    unknown = persisted(deezer, "unknown", "Family", "unknown", monitored=True)
    explicit = persisted(deezer, "explicit", "Family", "explicit")
    clean = persisted(deezer, "clean", "Family", "clean", monitored=True)
    fallback = persisted(deezer, "fallback", "Fallback", "unknown")
    clean_only = persisted(deezer, "clean-only", "Clean Only", "clean", monitored=True)
    manual_explicit = persisted(
        deezer, "manual-explicit", "Manual", "explicit", monitored=True, override=False
    )
    manual_clean = persisted(deezer, "manual-clean", "Manual", "clean", override=True)
    disabled_single = persisted(
        deezer, "single", "Single", "explicit", kind="single", monitored=True
    )
    other_provider = persisted(other, "other", "Other", "explicit", monitored=True)
    db_session.add(artist)
    await db_session.flush()

    changed = await reconcile_release_monitoring(db_session, artist.id)
    await db_session.flush()

    assert changed == 9
    assert [unknown.monitored, explicit.monitored, clean.monitored] == [False, True, False]
    assert fallback.monitored is True
    assert clean_only.monitored is False
    assert [manual_explicit.monitored, manual_clean.monitored] == [False, True]
    assert disabled_single.monitored is False
    assert other_provider.monitored is False
    monitored_album_ids = set(
        (
            await db_session.scalars(
                select(CatalogAlbum.id).where(CatalogAlbum.monitored.is_(True))
            )
        ).all()
    )
    assert monitored_album_ids == {
        explicit.catalog_album_id,
        fallback.catalog_album_id,
        manual_clean.catalog_album_id,
    }
    assert await reconcile_release_monitoring(db_session, artist.id) == 0
