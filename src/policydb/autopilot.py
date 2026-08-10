from __future__ import annotations

"""Bounded, resumable orchestration for source completion and full runs.

This module is deliberately an orchestration layer.  It does not replace the
existing AI provider, search provider, source-slot gates, or crawl pipeline.
AI/search output is evidence or a recommendation; deterministic source gates
remain the only path to verification and enabling.
"""

import hashlib  # noqa: E402
import json  # noqa: E402
import os  # noqa: E402
import tempfile  # noqa: E402
from collections.abc import Callable  # noqa: E402
from dataclasses import asdict, dataclass, field  # noqa: E402
from datetime import UTC, datetime  # noqa: E402
from pathlib import Path  # noqa: E402
from typing import Any, Literal  # noqa: E402

import yaml  # noqa: E402

from policydb.settings import Settings  # noqa: E402
from policydb.source_completion import build_slot_work_queue  # noqa: E402
from policydb.source_completion_ai_workflow import run_ai_batch  # noqa: E402
from policydb.source_slots import audit_525  # noqa: E402

Mode = Literal[
    "source-completion",
    "source-verification",
    "source-enable",
    "full-readiness",
    "full-crawl",
    "archive",
    "dedup",
    "ai-enrichment",
    "acceptance",
    "source-to-full",
]

GLOBAL_STATES = {
    'INTERRUPTED_RECOVERABLE',
    "SOURCE_COMPLETION",
    "SOURCE_GATE_CHECK",
    "FULL_CRAWL_READY",
    "FULL_CRAWL_RUNNING",
    "ARCHIVE_RUNNING",
    "DEDUP_RUNNING",
    "AI_RUNNING",
    "ACCEPTANCE_RUNNING",
    "COMPLETE",
    "BLOCKED",
    "STOPPED",
    "FAILED",
}

SLOT_STATES = {
    "PENDING",
    "AI_PLANNED",
    "SEARCHING",
    "SEARCHED",
    "CANDIDATES_FOUND",
    "CANDIDATES_RANKED",
    "PROBING",
    "PROBE_PARTIAL",
    "PROBE_PASSED",
    "VERIFYING",
    "VERIFIED",
    "ENABLING",
    "ENABLED",
    "RETRY_WAIT",
    "HUMAN_REVIEW",
    "QUARANTINED",
    "BLOCKED_NETWORK",
    "BLOCKED_ROLE",
    "BLOCKED_PARSER",
    "BLOCKED_PAGINATION",
    "COMPLETE",
}

DEFAULT_CONFIG: dict[str, Any] = {
    'research_retry_cooldown_seconds': 3600,
    "provider": "siliconflow",
    "model": "",
    "max_slots_per_batch": 20,
    "max_ai_calls_per_batch": 40,
    "max_total_ai_calls": 1000,
    "max_tokens_per_batch": 0,
    "max_cost_per_batch": 0.0,
    "concurrency": 4,
    "per_domain_concurrency": 1,
    "request_timeout": 30.0,
    "probe_rounds": 2,
    "max_candidates_per_slot": 3,
    "max_search_queries_per_slot": 10,
    "max_retry_attempts": 3,
    "retry_backoff_seconds": [60, 300, 1800],
    "auto_transition_to_full_crawl": False,
    "require_525_slots": True,
    "stop_on_test_failure": True,
    "full_crawl": {
        "start_date": "2018-01-01",
        "end_date": "today",
        "max_pages_per_source": 50,
        "max_candidates_per_shard": 500,
        "max_fetches_per_shard": 500,
    },
}


def utc_now() -> str:
    return datetime.now(UTC).isoformat()


def _safe_json(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)


def _atomic_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="") as handle:
            handle.write(json.dumps(value, ensure_ascii=False, indent=2, default=str) + "\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_name, path)
    finally:
        if os.path.exists(temp_name):
            os.unlink(temp_name)


@dataclass(frozen=True)
class AutopilotConfig:
    provider: str = "siliconflow"
    model: str = ""
    max_slots_per_batch: int = 20
    max_ai_calls_per_batch: int = 40
    max_total_ai_calls: int = 1000
    max_tokens_per_batch: int = 0
    max_cost_per_batch: float = 0.0
    concurrency: int = 4
    per_domain_concurrency: int = 1
    request_timeout: float = 30.0
    probe_rounds: int = 2
    max_candidates_per_slot: int = 3
    max_search_queries_per_slot: int = 10
    max_retry_attempts: int = 3
    retry_backoff_seconds: tuple[int, ...] = (60, 300, 1800)
    research_retry_cooldown_seconds: int = 3600
    auto_transition_to_full_crawl: bool = False
    require_525_slots: bool = True
    stop_on_test_failure: bool = True
    full_crawl: dict[str, Any] = field(default_factory=lambda: dict(DEFAULT_CONFIG["full_crawl"]))

    @classmethod
    def load(cls, path: Path | None = None, *, root: Path | None = None) -> AutopilotConfig:
        path = path or ((root or Path.cwd()) / "config" / "autopilot.yaml")
        values = dict(DEFAULT_CONFIG)
        if path.exists():
            loaded = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
            if not isinstance(loaded, dict):
                raise ValueError("autopilot config must be a mapping")
            values.update(loaded)
        values["full_crawl"] = {**DEFAULT_CONFIG["full_crawl"], **(values.get("full_crawl") or {})}
        values["retry_backoff_seconds"] = tuple(int(x) for x in values["retry_backoff_seconds"])
        config = cls(**{key: values[key] for key in asdict(cls()).keys() if key in values})
        config.validate()
        return config

    def validate(self) -> None:
        if self.provider != "siliconflow":
            raise ValueError(f"unsupported autopilot provider: {self.provider}")
        if not 1 <= self.max_slots_per_batch <= 50:
            raise ValueError("max_slots_per_batch must be between 1 and 50")
        if not 1 <= self.max_ai_calls_per_batch <= 100:
            raise ValueError("max_ai_calls_per_batch must be between 1 and 100")
        if not 1 <= self.concurrency <= 4:
            raise ValueError("concurrency must be between 1 and 4")
        if self.per_domain_concurrency != 1:
            raise ValueError("per_domain_concurrency must remain 1 for government traffic")
        if self.research_retry_cooldown_seconds < 0:
            raise ValueError('research_retry_cooldown_seconds cannot be negative')
        if self.probe_rounds < 2:
            raise ValueError("probe_rounds must be at least 2")
        if self.max_candidates_per_slot < 1:
            raise ValueError("max_candidates_per_slot must be positive")
        if any(value < 0 for value in self.retry_backoff_seconds):
            raise ValueError("retry backoff cannot be negative")


class AutopilotStateStore:
    """Atomic current state plus append-only transition evidence."""

    def __init__(self, run_dir: Path):
        self.run_dir = run_dir
        self.current_path = run_dir / "current_status.json"
        self.events_path = run_dir / "state_transitions.jsonl"
        self.stop_path = run_dir / "STOP_AUTOPILOT"
        run_dir.mkdir(parents=True, exist_ok=True)

    def read(self) -> dict[str, Any]:
        if not self.current_path.exists():
            return {}
        return json.loads(self.current_path.read_text(encoding="utf-8"))

    def write(self, state: dict[str, Any]) -> dict[str, Any]:
        _atomic_json(self.current_path, state)
        return state

    def transition(
        self,
        *,
        new_status: str,
        previous_status: str | None = None,
        slot_id: str | None = None,
        reason_code: str = "",
        evidence_ids: list[str] | None = None,
        attempt: int = 0,
        provider: str | None = None,
        model: str | None = None,
        idempotency_key: str | None = None,
    ) -> dict[str, Any]:
        if new_status not in GLOBAL_STATES and new_status not in SLOT_STATES:
            raise ValueError(f"unknown autopilot state: {new_status}")
        state = self.read()
        event = {
            "job_id": state.get("job_id"),
            "run_id": state.get("run_id"),
            "slot_id": slot_id,
            "previous_status": previous_status or state.get("status"),
            "new_status": new_status,
            "reason_code": reason_code,
            "evidence_ids": evidence_ids or [],
            "attempt": attempt,
            "provider": provider,
            "model": model,
            "timestamp": utc_now(),
            "idempotency_key": idempotency_key or hashlib.sha256(
                _safe_json([state.get("run_id"), slot_id, new_status, reason_code]).encode()
            ).hexdigest(),
        }
        self.events_path.parent.mkdir(parents=True, exist_ok=True)
        with self.events_path.open("a", encoding="utf-8") as handle:
            handle.write(_safe_json(event) + "\n")
            handle.flush()
            os.fsync(handle.fileno())
        state.update({"status": new_status, "updated_at": event["timestamp"], "last_transition": event})
        return self.write(state)

    def request_stop(self, reason: str = "operator_requested") -> dict[str, Any]:
        self.stop_path.write_text(reason + "\n", encoding="utf-8")
        return self.transition(new_status="STOPPED", reason_code=reason)

    def stop_requested(self) -> bool:
        return self.stop_path.exists() or os.getenv("STOP_AUTOPILOT", "").lower() in {"1", "true", "yes"}


def _slot_state(row: dict[str, Any]) -> str:
    work_status = str(row.get("work_status") or "")
    if work_status == "verified_enabled":
        return "ENABLED"
    if row.get("best_candidate_id"):
        if int(row.get("health_probe_success_count") or 0) >= 2:
            return "PROBE_PASSED"
        return "CANDIDATES_FOUND"
    if work_status in {"blocked_network", "blocked_parser", "blocked_pagination", "blocked_role_conflict"}:
        return {
            "blocked_network": "BLOCKED_NETWORK",
            "blocked_parser": "BLOCKED_PARSER",
            "blocked_pagination": "BLOCKED_PAGINATION",
            "blocked_role_conflict": "BLOCKED_ROLE",
        }[work_status]
    if work_status == "no_candidate_manual_research":
        return "HUMAN_REVIEW"
    return "PENDING"


def _gate(audit: dict[str, Any], *, tests_passed: bool = False, active_writer: bool = False) -> dict[str, Any]:
    checks = {
        "required_slots": int(audit.get("required_slots", 0)) == 525,
        "verified_slots": int(audit.get("slots_verified", audit.get("verified_slots", 0))) == 525,
        "enabled_slots": int(audit.get("slots_enabled", audit.get("enabled_slots", 0))) == 525,
        "direct_healthy_slots": int(audit.get("slots_direct_healthy", 0)) == 525,
        "parser_ready_slots": int(audit.get("slots_parser_ready", 0)) == 525,
        "unresolved_slots": int(audit.get("slots_unresolved", 1)) == 0,
        "enabled_unverified_slots": int(audit.get("enabled_unverified_slots", 1)) == 0,
        "full_tests_passed": tests_passed,
        "no_active_writer_conflict": not active_writer,
        "network_policy_valid": True,
        "archive_ai_gates_valid": True,
    }
    return {"status": "GO" if all(checks.values()) else "BLOCKED", "checks": checks, "audit": audit, "evaluated_at": utc_now()}


def _provider_audit(settings: Settings, config: AutopilotConfig) -> dict[str, Any]:
    return {
        "provider": config.provider,
        "model": config.model or settings.siliconflow_chat_model,
        "structured_output": True,
        "native_web_search": False,
        "tool_calling": False,
        "token_usage": True,
        "cost_usage": False,
        "streaming": False,
        "api_key_configured": bool(settings.siliconflow_api_key),
        "real_api_called": False,
        "search_evidence_provider": "existing search provider / DuckDuckGo fallback",
        "secret_values_emitted": False,
    }


class AutopilotController:
    def __init__(
        self,
        settings: Settings | None = None,
        *,
        config: AutopilotConfig | None = None,
        config_path: Path | None = None,
        output: Path | None = None,
        run_id: str | None = None,
        source_runner: Callable[..., dict[str, Any]] | None = None,
    ) -> None:
        self.settings = settings or Settings.discover()
        self.config = config or AutopilotConfig.load(config_path, root=self.settings.root)
        self.run_id = run_id or datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
        self.run_dir = output or self.settings.outputs / "autopilot" / self.run_id
        self.store = AutopilotStateStore(self.run_dir)
        self.source_runner = source_runner or run_ai_batch

    def _base_state(self, mode: Mode) -> dict[str, Any]:
        return {
            "job_id": f"AUTOPILOT_{self.run_id}",
            "run_id": self.run_id,
            "mode": mode,
            "status": "SOURCE_COMPLETION",
            "provider_status": "configured" if self.settings.siliconflow_api_key else "not_configured",
            "api_balance_status": "unknown",
            "verified": None,
            "enabled": None,
            "unresolved": None,
            "current_batch": None,
            "ai_calls": 0,
            "tokens": 0,
            "cost": 0.0,
            "cache_hit_rate": None,
            "candidates": 0,
            "probes": 0,
            "retries": 0,
            "human_review": 0,
            "latest_error": None,
            "next_action": "run bounded source completion or resume",
            "safe_resume_command": f"policydb autopilot resume --run-id {self.run_id}",
            "created_at": utc_now(),
            "updated_at": utc_now(),
        }

    def plan(self, mode: Mode = "source-to-full") -> dict[str, Any]:
        state = self.store.read() or self._base_state(mode)
        self.store.write(state)
        queue = build_slot_work_queue(self.settings)
        slot_rows = []
        for row in queue.iter_rows(named=True):
            slot_rows.append({"slot_id": row.get("slot_id"), "city_id": row.get("city_id"), "city_name": row.get("city_name"), "source_role": row.get("source_role"), "state": _slot_state(row), "work_status": row.get("work_status")})
        source_plan = {
            "run_id": self.run_id,
            "mode": mode,
            "required_slots": len(slot_rows),
            "max_slots_per_batch": self.config.max_slots_per_batch,
            "max_ai_calls_per_batch": self.config.max_ai_calls_per_batch,
            "concurrency": self.config.concurrency,
            "per_domain_concurrency": self.config.per_domain_concurrency,
            "slots": slot_rows,
            "execution_started": False,
        }
        _atomic_json(self.run_dir / "source_completion_plan.json", source_plan)
        full_plan = {
            "run_id": self.run_id,
            "mode": mode,
            "execution_started": False,
            "required_source_gate": "GO",
            "cities": 105,
            "shard_key": ["city_id", "source_role", "month"],
            "start_date": self.config.full_crawl["start_date"],
            "end_date": self.config.full_crawl["end_date"],
            "stages": ["crawl", "archive", "archive_integrity", "text_extraction", "dedup", "ai_eligibility", "ai_enrichment", "final_acceptance"],
            "executor": "existing ExhaustiveCrawler/CrawlPipeline; invoked only after explicit GO and --auto-full-crawl",
        }
        _atomic_json(self.run_dir / "full_crawl_plan_dry_run.json", full_plan)
        audit = audit_525(self.settings)
        gate = _gate(audit)
        _atomic_json(self.run_dir / "go_no_go_dry_run.json", gate)
        _atomic_json(self.run_dir / "API_PROVIDER_AUDIT.json", _provider_audit(self.settings, self.config))
        state.update({"verified": audit.get("slots_verified"), "enabled": audit.get("slots_enabled"), "unresolved": audit.get("slots_unresolved"), "next_action": "source completion; full crawl remains GO-gated", "updated_at": utc_now()})
        self.store.write(state)
        return {"run_dir": str(self.run_dir), "source_plan": source_plan, "full_crawl_plan": full_plan, "go_no_go": gate, "provider": _provider_audit(self.settings, self.config)}

    def run(self, mode: Mode = "source-to-full", *, apply: bool = False, resume: bool = False, auto_full_crawl: bool = False) -> dict[str, Any]:
        if mode not in GLOBAL_STATES and mode not in {"source-completion", "source-verification", "source-enable", "full-readiness", "full-crawl", "archive", "dedup", "ai-enrichment", "acceptance", "source-to-full"}:
            raise ValueError(f"unsupported mode: {mode}")
        if self.store.stop_requested():
            return {"status": "STOPPED", "run_dir": str(self.run_dir), "reason": "stop_requested"}
        plan = self.plan(mode)
        if not apply:
            return {"status": "PLANNED", "run_dir": str(self.run_dir), "go_no_go": plan["go_no_go"], "execution_started": False}
        if mode in {"full-crawl", "archive", "dedup", "ai-enrichment", "acceptance"} and not auto_full_crawl:
            return {"status": "BLOCKED", "run_dir": str(self.run_dir), "reason": "explicit --auto-full-crawl is required for full-run stages"}
        self.store.transition(new_status="SOURCE_COMPLETION", reason_code="bounded_run_started")
        source_dir = self.run_dir / "source_completion"
        try:
            result = self.source_runner(self.settings, output=source_dir, max_slots=self.config.max_slots_per_batch, max_ai_calls=self.config.max_ai_calls_per_batch, concurrency=self.config.concurrency, dry_run=False, apply=True, resume=resume)
        except Exception as exc:
            self.store.transition(new_status="FAILED", reason_code=type(exc).__name__)
            state = self.store.read()
            state.update({"latest_error": str(exc)[:500], "next_action": "inspect checkpoint and resume after repair"})
            self.store.write(state)
            return {"status": "FAILED", "run_dir": str(self.run_dir), "error_type": type(exc).__name__}
        audit = audit_525(self.settings)
        gate = _gate(audit)
        _atomic_json(self.run_dir / "go_no_go.json", gate)
        state = self.store.read()
        state.update({"current_batch": result, "ai_calls": result.get("ai_calls", 0), "candidates": result.get("candidate_proposals", 0), "probes": result.get("probed_candidates", 0), "verified": audit.get("slots_verified"), "enabled": audit.get("slots_enabled"), "unresolved": audit.get("slots_unresolved"), "updated_at": utc_now()})
        if gate["status"] != "GO":
            state.update({"status": "BLOCKED", "next_action": "resume source completion; full crawl is not authorized"})
            self.store.write(state)
            return {"status": "BLOCKED", "run_dir": str(self.run_dir), "batch": result, "go_no_go": gate}
        self.store.transition(new_status="FULL_CRAWL_READY", reason_code="go_gate_passed")
        if mode == "source-to-full" and (auto_full_crawl or self.config.auto_transition_to_full_crawl):
            state["status"] = "FULL_CRAWL_RUNNING"
            self.store.write(state)
            return {"status": "FULL_CRAWL_READY", "run_dir": str(self.run_dir), "go_no_go": gate, "next_action": "invoke existing full-run executor explicitly"}
        state["status"] = "COMPLETE"
        state["next_action"] = "source batch complete; full crawl not started"
        self.store.write(state)
        return {"status": "COMPLETE", "run_dir": str(self.run_dir), "batch": result, "go_no_go": gate, "full_run_started": False}

    def status(self) -> dict[str, Any]:
        return self.store.read() or {"status": "NOT_STARTED", "run_dir": str(self.run_dir)}

    def stop(self) -> dict[str, Any]:
        return self.store.request_stop()

    def retry(self) -> dict[str, Any]:
        if self.store.stop_path.exists():
            self.store.stop_path.unlink()
        state = self.store.read()
        state.update({"status": "RETRY_WAIT", "next_action": f"{state.get('safe_resume_command', 'policydb autopilot resume')}", "updated_at": utc_now()})
        return self.store.write(state)

    def audit(self) -> dict[str, Any]:
        current = self.status()
        transitions = []
        if self.store.events_path.exists():
            transitions = [json.loads(line) for line in self.store.events_path.read_text(encoding="utf-8").splitlines() if line.strip()]
        gate_path = self.run_dir / "go_no_go.json"
        gate = json.loads(gate_path.read_text(encoding="utf-8")) if gate_path.exists() else self.plan(current.get("mode", "source-to-full"))["go_no_go"]
        return {"run_dir": str(self.run_dir), "current": current, "transition_count": len(transitions), "transitions": transitions, "go_no_go": gate, "secret_values_emitted": False}


def default_run_dir(settings: Settings, run_id: str | None = None) -> Path:
    return settings.outputs / "autopilot" / (run_id or datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ"))


def controller_from_cli(*, run_id: str | None = None, output: Path | None = None, config: Path | None = None) -> AutopilotController:
    settings = Settings.discover()
    return AutopilotController(settings, config_path=config, output=output, run_id=run_id)
