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
