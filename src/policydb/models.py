from __future__ import annotations

from datetime import date, datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, HttpUrl

RecordType = Literal[
    "policy_document",
    "official_statement",
    "meeting_statement",
    "government_report",
    "policy_change",
    "policy_provision",
    "programme_event",
    "financing_event",
    "enterprise_event",
    "media_report",
    "rumour",
    "internal_document",
    "other",
]
OfficialStatus = Literal[
    "official",
    "official_reprint",
    "consultation_draft",
    "authoritative_media",
    "general_media",
    "self_media",
    "rumour",
    "internal_unverified",
    "unknown",
]
PolicyStatus = Literal[
    "planned",
    "consultation",
    "issued",
    "in_force",
    "expired",
    "repealed",
    "replaced",
    "suspended",
    "historical",
    "unknown",
]
Direction = Literal["tightening", "loosening", "supportive", "neutral", "mixed", "unknown"]


class PolicyRecord(BaseModel):
    model_config = ConfigDict(extra="allow")
    record_id: str
    record_type: RecordType = "policy_document"
    title: str | None = None
    title_normalized: str | None = None
    record_date: date | None = None
    publication_date: date | None = None
    issuance_date: date | None = None
    effective_date: date | None = None
    expiry_date: date | None = None
    status: PolicyStatus = "unknown"
    direction: Direction = "unknown"
    summary: str | None = None
    full_text: str | None = None
    language: str = "zh-CN"
    official_level: str | None = None
    official_status: OfficialStatus = "unknown"
    source_quality: int = 0
    primary_source_url: HttpUrl | str | None = None
    source_file: str
    source_sheet: str
    source_row: int
    import_batch_id: str
    created_at: datetime
    updated_at: datetime
    manual_review_status: str = "pending"
