from __future__ import annotations

from datetime import date, datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


class RegisteredSource(BaseModel):
    model_config = ConfigDict(extra="allow")
    source_id: str
    source_name: str
    domain: str
    source_type: str
    source_role: str
    official_status: str
    seed_urls: list[str] = Field(default_factory=list)
    list_page_urls: list[str] = Field(default_factory=list)
    search_url_template: str | None = None
    parser_adapter: str = "generic_government"
    crawl_enabled: bool = False
    priority: int = 3
    rate_limit: float = 0.5
    source_health_score: float | None = None
    recommended_enabled: bool = False
    last_health_at: datetime | None = None
    last_error: str | None = None


class CrawlItem(BaseModel):
    item_id: str
    run_id: str
    source_id: str
    url: str
    canonical_url: str
    status: Literal["pending", "fetched", "unchanged", "failed", "blocked"] = "pending"
    city_id: str | None = None
    query_year: int | None = None
    keyword_group: str | None = None
    retry_count: int = 0
    first_seen_at: datetime
    last_seen_at: datetime
    created_at: datetime
    updated_at: datetime


class FetchResult(BaseModel):
    requested_url: str
    final_url: str
    status_code: int
    content_type: str | None
    body: bytes
    response_sha256: str
    retrieved_at: datetime
    etag: str | None = None
    last_modified: str | None = None
    not_modified: bool = False


class DiscoveryCandidate(BaseModel):
    candidate_id: str
    run_id: str
    discovery_mode: str
    source_id: str | None = None
    url: str
    canonical_url: str
    parent_url: str | None = None
    title_hint: str | None = None
    date_hint: date | None = None
    city_hint: str | None = None
    keyword_group: str | None = None
    source_role: str = "discovery_lead"
    discovered_at: datetime
    discovery_score: float = 0.0
    status: str = "pending"


class DiscoveryRequest(BaseModel):
    run_id: str
    mode: str
    start_date: date | None = None
    end_date: date | None = None
    cities: list[str] = Field(default_factory=list)
    topics: list[str] = Field(default_factory=list)
    max_pages: int = 5
    max_candidates: int = 200
