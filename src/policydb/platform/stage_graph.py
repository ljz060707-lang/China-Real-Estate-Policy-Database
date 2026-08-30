"""CRPD resumable stage graph (additive).

Each stage declares input/output contracts and a checkpoint key so the
platform can resume after a crash without re-running finished stages.
"""
from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

STAGES: list[str] = [
    "SOURCE_INVENTORY",
    "CANDIDATE_DISCOVERY",
    "SOURCE_VALIDATION",
    "SOURCE_ENABLEMENT",
    "CRAWL_PLAN",
    "FETCH",
    "ATTACHMENT",
    "PARSE",
    "DATE_EXTRACT",
    "CLASSIFY",
    "ACTION_SPLIT",
    "DEDUP",
    "COVERAGE",
    "GAP_RECOVERY",
    "REVIEW_QUEUE",
    "PROMOTE",
    "RELEASE",
]


@dataclass
class Stage:
    name: str
    fn: Callable[..., Any] | None = None
    input_contract: str = ""
    output_contract: str = ""
    checkpoint_key: str = ""


STAGE_GRAPH: dict[str, Stage] = {
    "SOURCE_INVENTORY": Stage("SOURCE_INVENTORY", input_contract="city_id", output_contract="slots"),
    "CANDIDATE_DISCOVERY": Stage("CANDIDATE_DISCOVERY", output_contract="candidates"),
    "SOURCE_VALIDATION": Stage("SOURCE_VALIDATION", output_contract="validated_sources"),
    "SOURCE_ENABLEMENT": Stage("SOURCE_ENABLEMENT", output_contract="enabled_slots"),
    "CRAWL_PLAN": Stage("CRAWL_PLAN", output_contract="jobs"),
    "FETCH": Stage("FETCH", output_contract="fetch_attempts"),
    "ATTACHMENT": Stage("ATTACHMENT", output_contract="attachments"),
    "PARSE": Stage("PARSE", output_contract="documents"),
    "DATE_EXTRACT": Stage("DATE_EXTRACT", output_contract="dates"),
    "CLASSIFY": Stage("CLASSIFY", output_contract="classifications"),
    "ACTION_SPLIT": Stage("ACTION_SPLIT", output_contract="actions"),
    "DEDUP": Stage("DEDUP", output_contract="dedup_decisions"),
    "COVERAGE": Stage("COVERAGE", output_contract="coverage"),
    "GAP_RECOVERY": Stage("GAP_RECOVERY", output_contract="recovery_attempts"),
    "REVIEW_QUEUE": Stage("REVIEW_QUEUE", output_contract="review_items"),
    "PROMOTE": Stage("PROMOTE", output_contract="promoted_rows"),
    "RELEASE": Stage("RELEASE", output_contract="release_manifest"),
}

# stage → checkpoint key prefix (persisted under CRPD_CACHE_ROOT/checkpoints)
CHECKPOINT_PREFIX = "crpd_checkpoint_"


def checkpoint_key(run_id: str, stage: str) -> str:
    return f"{CHECKPOINT_PREFIX}{run_id}_{stage}"
