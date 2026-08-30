from __future__ import annotations

from datetime import UTC, date, datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

JobStatus = Literal[
    "queued",
    "running",
    "waiting",
    "completed",
    "completed_with_warnings",
    "failed",
    "cancelled",
]

LEGACY_ACTIVE_STATUSES = {
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
}


class CrawlJobRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    mode: Literal[
        "smart",
        "official_update",
        "web_discovery",
        "seed_backtrack",
        "historical_105",
        "historical_episode_930",
        "recover_missing",
        "source_health",
    ]
    episode_id: str | None = None
    episode_run_id: str | None = None
    episode_queue_path: str | None = None
    episode_output_path: str | None = None
    episode_queue_item_ids: list[str] = Field(default_factory=list)
    episode_city_limit: int = Field(default=5, ge=1, le=105)
    episode_max_ai_calls: int = Field(default=10, ge=0, le=1000)
    start_date: date | None = None
    end_date: date | None = None
    cities: list[str] = Field(default_factory=list)
    provinces: list[str] = Field(default_factory=list)
    topics: list[str] = Field(default_factory=list)
    source_ids: list[str] = Field(default_factory=list)
    source_roles: list[str] = Field(default_factory=list)
    source_types: list[str] = Field(default_factory=list)
    missing_types: list[str] = Field(default_factory=list)
    max_candidates: int = Field(default=200, ge=1, le=100000)
    max_candidates_total: int | None = Field(default=None, ge=1, le=100000)
    max_candidates_per_source: int = Field(default=50, ge=1, le=10000)
    max_pages_per_source: int = Field(default=20, ge=1, le=1000)
    batch_size: int = Field(default=50, ge=1, le=1000)
    global_safety_limit: int = Field(default=10000, ge=1, le=1000000)
    resume: bool = True
    max_fetches: int = Field(default=100, ge=1, le=10000)
    # Only network reads overlap. Parsing, checkpointing and database writes
    # remain owned by the single job worker.
    fetch_concurrency: int | None = Field(default=None, ge=1, le=16)
    per_host_concurrency: int = Field(default=1, ge=1, le=2)
    # Bounded selected-candidate rehearsals must be able to drain the selected
    # set to terminal state without silently inheriting a smaller fetch cap.
    # The global candidate/safety limits remain authoritative.
    drain_selected_batch: bool = False
    max_attachment_attempts: int = Field(default=1, ge=0, le=20)
    enabled_only: bool = True
    include_recommended: bool = False
    run_glm: bool = False
    run_verification: bool = True
    rebuild_database: bool = True
    run_validation: bool = True
    official_first: bool = True
    confirmed_recommended_source_ids: list[str] = Field(default_factory=list)
    demo_mode: bool = False
    runtime_mode: Literal["REHEARSAL", "PRODUCTION", "TEST", "DEMO", "UNSPECIFIED"] = "UNSPECIFIED"
    production_write_allowed: bool = False
    processing_mode: Literal["staged_only", "glm", "glm_verify", "full"] = "full"

    def estimate(self, enabled_source_count: int) -> dict[str, int]:
        """Return a UI-only estimate without constructing the crawl pipeline."""
        candidate_limit = min(
            self.max_candidates_total or self.max_candidates,
            self.global_safety_limit,
        )
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
            "query_count": min(query_count, candidate_limit),
            "max_pages": self.max_pages_per_source,
            "possible_api_calls": min(query_count, candidate_limit),
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
    # Progress counters are mostly numeric, but bounded workflows may carry
    # auditable identifiers (for example an episode_id or source_id).  Runtime
    # state must remain readable after such an event is persisted; rejecting a
    # whole worker state because one diagnostic value is textual makes resume
    # impossible and can misclassify a live crawl as failed.
    counters: dict[str, int | float | str | bool | None] = Field(default_factory=dict)
    heartbeat_at: datetime | None = None
    worker_started_at: datetime | None = None
    last_progress_at: datetime | None = None
    current_url_redacted: str | None = None
    current_source_id: str | None = None
    queued_count: int = 0
    processed_count: int = 0

    @field_validator("status", mode="before")
    @classmethod
    def normalize_legacy_stage_status(cls, value: object) -> object:
        """Keep old state files readable without letting stage become lifecycle."""

        return "running" if str(value or "") in LEGACY_ACTIVE_STATUSES else value
