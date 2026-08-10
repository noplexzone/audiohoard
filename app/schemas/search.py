from __future__ import annotations

from pydantic import BaseModel, Field, model_validator

from app.metadata.filename_parse import compose_search_query
from app.schemas.health import SourceStatus


class SearchRequest(BaseModel):
    query: str = Field(default="", max_length=500)
    artist: str | None = Field(default=None, max_length=250)
    album: str | None = Field(default=None, max_length=250)
    track: str | None = Field(default=None, max_length=250)
    sources: list[str] = Field(default_factory=list)
    expected_duration_sec: int | None = Field(default=None, ge=1, le=86400)
    preferred_format: str | None = Field(default=None, max_length=20)
    expected_track_count: int | None = Field(default=None, ge=1, le=1000)
    catalog_ids: dict[str, str] = Field(default_factory=dict)

    @model_validator(mode="after")
    def require_search_terms(self) -> SearchRequest:
        if not self.search_text:
            raise ValueError("at least one search term is required")
        return self

    @property
    def search_text(self) -> str:
        return compose_search_query(self.query, self.artist, self.album, self.track)


class SearchResult(BaseModel):
    source: str
    title: str | None = None
    artist: str | None = None
    album: str | None = None
    duration_sec: int | None = None
    size_bytes: int | None = None
    format: str | None = None
    url: str | None = None
    metadata: dict[str, object] = Field(default_factory=dict)


class SearchResponse(BaseModel):
    results: list[SearchResult]
    source_states: dict[str, SourceStatus]
