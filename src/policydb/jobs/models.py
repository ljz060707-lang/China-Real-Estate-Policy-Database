from __future__ import annotations

from datetime import UTC, date, datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

JobStatus = Literal[
    "queued",
    "preparing",
    "discovering",
    "fetching",
    "parsing",
    "deduplicating",
    "enriching",
    "verifying",
    "rebuilding",
    "validating",
    "reporting",
    "completed",
    "completed_with_warnings",
    "failed",
    "cancelled",
]


class CrawlJobRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    mode: Literal[
        "smart",
        "official_update",
        "web_discovery",
        "seed_backtrack",
        "historical_105",
        "recover_missing",
        "source_health",
    ]
    start_date: date | None = None
    end_date: date | None = None
    cities: list[str] = Field(default_factory=list)
    provinces: list[str] = Field(default_factory=list)
    topics: list[str] = Field(default_factory=list)
    source_types: list[str] = Field(default_factory=list)
    missing_types: list[str] = Field(default_factory=list)
    max_candidates: int = Field(default=200, ge=1, le=100000)
    max_fetches: int = Field(default=100, ge=1, le=10000)
    enabled_only: bool = True
    include_recommended: bool = False
    run_glm: bool = False
    run_verification: bool = True
    rebuild_database: bool = True
    run_validation: bool = True
    official_first: bool = True
    confirmed_recommended_source_ids: list[str] = Field(default_factory=list)
    demo_mode: bool = False
    processing_mode: Literal["staged_only", "glm", "glm_verify", "full"] = "full"

    def estimate(self, enabled_source_count: int) -> dict[str, int]:
        """Return a UI-only estimate without constructing the crawl pipeline."""
        cities = len(self.cities) or (105 if self.mode == "historical_105" else 1)
        topics = len(self.topics) or 1
        query_count = (
            cities * topics * 8
            if self.mode in {"web_discovery", "historical_105", "smart"}
            else 0
        )
        return {
            "city_count": cities,
            "topic_count": topics,
            "source_count": enabled_source_count,
            "query_count": min(query_count, self.max_candidates),
            "max_pages": self.max_fetches,
            "possible_api_calls": min(query_count, self.max_candidates),
        }


class JobState(BaseModel):
    job_id: str
    mode: str
    status: JobStatus = "queued"
    stage: str = "queued"
    progress_current: int = 0
    progress_total: int = 1
    message: str = "等待后台工作进程"
    pid: int | None = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    started_at: datetime | None = None
    finished_at: datetime | None = None
    cancel_requested: bool = False
    error_type: str | None = None
    error_message: str | None = None
    run_id: str | None = None
    counters: dict[str, int | float] = Field(default_factory=dict)
    heartbeat_at: datetime | None = None
    worker_started_at: datetime | None = None
    last_progress_at: datetime | None = None
    current_url_redacted: str | None = None
    current_source_id: str | None = None
    queued_count: int = 0
    processed_count: int = 0
