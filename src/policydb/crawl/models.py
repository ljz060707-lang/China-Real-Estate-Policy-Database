from __future__ import annotations

from datetime import date, datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

AGENCY_TYPE_ALIASES = {
    "central_government": "state_council",
    "local_government": "municipal_government",
    "housing": "housing_department",
    "finance": "financial_regulator",
    "development_reform": "ministry",
    "tax": "ministry",
    "media_or_aggregator": "secondary_source",
    "unknown": "secondary_source",
}


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
    city_ids: list[str] = Field(default_factory=list)
    province_codes: list[str] = Field(default_factory=list)
    scope_type: Literal[
        "national", "provincial", "municipal", "county", "multi_region", "unknown"
    ] = "unknown"
    agency_type: Literal[
        "central_office",
        "state_council",
        "ministry",
        "central_bank",
        "financial_regulator",
        "provincial_government",
        "municipal_government",
        "housing_department",
        "natural_resources_department",
        "provident_fund_center",
        "official_media",
        "other_official",
        "secondary_source",
    ] = "secondary_source"
    required_level: Literal["required", "recommended", "supplemental"] = "supplemental"
    coverage_start_date: date | None = None
    coverage_end_date: date | None = None
    expected_frequency: Literal[
        "daily", "weekly", "monthly", "quarterly", "irregular", "unknown"
    ] = "unknown"
    gazette_url: str | None = None
    homepage_url: str | None = None
    is_valid: bool = True
    verified_at: datetime | None = None
    replacement_source_id: str | None = None
    parser_version: str = "1"
    last_scan_at: datetime | None = None
    consecutive_failures: int = 0

    @field_validator("agency_type", mode="before")
    @classmethod
    def normalize_legacy_agency_type(cls, value: object) -> object:
        return AGENCY_TYPE_ALIASES.get(str(value or "unknown"), value or "secondary_source")

    @field_validator("city_ids", "province_codes", mode="before")
    @classmethod
    def normalize_list_fields(cls, value: object) -> list[str]:
        if value in (None, ""):
            return []
        if isinstance(value, str):
            return [part.strip() for part in value.split(",") if part.strip()]
        return [str(part) for part in value]


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
    task_key: str | None = None
    scan_method: str | None = None
    requested_url: str | None = None
    final_url: str | None = None
    etag: str | None = None
    last_modified: str | None = None
    last_checked_at: datetime | None = None
    next_check_at: datetime | None = None


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
