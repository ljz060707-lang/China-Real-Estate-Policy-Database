from __future__ import annotations

import json
import shutil
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urlsplit

import polars as pl

from policydb.autopilot_checkpoints import STATUS_PRIORITY, GlobalSlotCheckpointStore
from policydb.crawl.registry import load_registry
from policydb.parquet_store import atomic_write_parquet, read_parquet_snapshot
from policydb.settings import Settings
from policydb.source_discovery import REQUIRED_ROLES, is_reusable_source_entry
from policydb.source_slots import audit_525, slot_paths

ALLOWED_DECISIONS = (
    "approve_primary",
    "approve_alternative",
    "reject_all",
    "change_role",
    "accept_municipal_substitute",
    "defer",
    "quarantine",
)

_PRIORITY = {
    "verified_enabled": 0,
    "candidate_ready_for_probe": 1,
    "candidate_failed_fixable": 2,
    "candidate_failed_ambiguous": 3,
    "blocked_role_conflict": 3,
    "no_candidate_discoverable": 4,
    "no_candidate_manual_research": 5,
    "blocked_network": 6,
    "blocked_parser": 7,
    "blocked_pagination": 8,
    "enabled": 0,
    "verified": 0,
    "human_review": 3,
    "completed": 2,
    "retry_wait": 1,
    "failed_recoverable": 1,
    "claimed": 0,
}
_PARSER_OK = {"ok", "verified", "list_detected", "pagination_detected"}
_DIRECT_ROUTES = {"direct", "direct_ok", "curl_fallback_ok"}


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _official_domain(url: str | None) -> bool:
    host = (urlsplit(str(url or "")).hostname or "").lower().rstrip(".")
    return host == "gov.cn" or host.endswith(".gov.cn")


def _value(row: dict, key: str, default=None):
    value = row.get(key, default)
    return default if value is None else value


def _bool(row: dict, key: str) -> bool:
    return bool(_value(row, key, False))


def _empty_frame(columns: list[str]) -> pl.DataFrame:
    return pl.DataFrame({column: pl.Series(column, [], dtype=pl.String) for column in columns})


def _registry_by_slot(settings: Settings) -> dict[tuple[str, str], list[object]]:
    result: dict[tuple[str, str], list[object]] = defaultdict(list)
    for source in load_registry(settings):
        role = source.agency_type if source.agency_type in REQUIRED_ROLES else source.source_role
        if role not in REQUIRED_ROLES:
            continue
        for city_id in source.city_ids:
            result[(str(city_id), role)].append(source)
    return result


def _candidate_score(row: dict) -> tuple:
    return (
        int(_bool(row, "is_verified")),
        int(_bool(row, "entry_eligible")),
        int(_value(row, "health_probe_success_count", 0) or 0),
        int(str(_value(row, "parser_status", "")).lower() in _PARSER_OK),
        float(_value(row, "overall_confidence", 0.0) or 0.0),
        -int(_value(row, "conflict_count", 0) or 0),
        str(_value(row, "candidate_id", "")),
    )


def _failure_gates(row: dict | None, registered: list[object]) -> list[str]:
    if row is None:
        return ["no_candidate"]
    gates: list[str] = []
    url = str(_value(row, "canonical_url", ""))
    route = str(_value(row, "network_route", "")).lower()
    status = int(_value(row, "http_status", 0) or 0)
    health_success = int(_value(row, "health_probe_success_count", 0) or 0)
    parser_status = str(_value(row, "parser_status", "")).lower()
    pagination = str(_value(row, "pagination_strategy", "")).lower()
    if not _official_domain(url) and not _bool(row, "is_official"):
        gates.append("official_domain_unverified")
    if not _bool(row, "entry_eligible") or not is_reusable_source_entry(url):
        gates.append("entry_ineligible")
    if _value(row, "candidate_kind", "") == "policy_content_evidence":
        gates.append("content_evidence_only")
    if route not in _DIRECT_ROUTES:
        gates.append("network_route_not_direct")
    if status != 200:
        gates.append("http_not_200")
    if health_success < 2:
        gates.append("missing_two_probe_evidence")
    if parser_status not in _PARSER_OK:
        gates.append("parser_not_verified")
    if pagination not in {"next_link", "natural_single_page", "sitemap", "bounded_cursor"}:
        gates.append("pagination_not_verified")
    if row.get("city_confidence") is not None and float(row.get("city_confidence") or 0.0) < 0.8:
        gates.append("city_evidence_ambiguous")
    if row.get("role_confidence") is not None and float(row.get("role_confidence") or 0.0) < 0.8:
        gates.append("role_evidence_ambiguous")
    if int(_value(row, "conflict_count", 0) or 0) > 0 or _bool(row, "has_cross_jurisdiction_conflict"):
        gates.append("conflicting_evidence")
    if not registered:
        gates.append("source_not_registered")
    return list(dict.fromkeys(gates))


def _classify(row: dict | None, registered: list[object]) -> str:
    if row is None:
        return "no_candidate_discoverable" if registered else "no_candidate_manual_research"
    if _bool(row, "is_verified") and registered and any(
        source.crawl_enabled and source.official_domain_verified for source in registered
    ):
        return "verified_enabled"
    gates = _failure_gates(row, registered)
    if "role_evidence_ambiguous" in gates or "conflicting_evidence" in gates:
        return "blocked_role_conflict"
    # An unprobed, otherwise reusable candidate is ready for the controlled probe.
    if (
        "missing_two_probe_evidence" in gates
        and "entry_ineligible" not in gates
        and int(_value(row, "http_status", 0) or 0) == 0
        and str(_value(row, "network_route", "")).lower() in {"", "unknown"}
    ):
        return "candidate_ready_for_probe"
    if "network_route_not_direct" in gates or "http_not_200" in gates:
        return "blocked_network"
    if "pagination_not_verified" in gates and "parser_not_verified" not in gates:
        return "blocked_pagination"
    if "parser_not_verified" in gates:
        return "blocked_parser"
    if "entry_ineligible" in gates or "official_domain_unverified" in gates:
        return "candidate_failed_ambiguous" if not registered else "candidate_failed_fixable"
    return "candidate_failed_fixable"


def build_slot_work_queue(settings: Settings | None = None) -> pl.DataFrame:
    """Build exactly one auditable row per required slot from current Parquet and registry."""
    settings = settings or Settings.discover()
    slot_path, candidate_path = slot_paths(settings)
    slots = read_parquet_snapshot(slot_path)
    candidates = read_parquet_snapshot(candidate_path) if candidate_path.exists() else pl.DataFrame()
    registry = _registry_by_slot(settings)
    candidates_by_slot: dict[str, list[dict]] = defaultdict(list)
    if candidates.height:
        for row in candidates.iter_rows(named=True):
            candidates_by_slot[str(row["slot_id"])].append(row)
    rows: list[dict] = []
    for slot in slots.iter_rows(named=True):
        slot_id = str(slot["slot_id"])
        city_id = str(slot["city_id"])
        role = str(slot["source_role"])
        registered = registry.get((city_id, role), [])
        slot_candidates = candidates_by_slot.get(slot_id, [])
        best = max(slot_candidates, key=_candidate_score) if slot_candidates else None
        status = _classify(best, registered)
        gates = _failure_gates(best, registered)
        verified_count = sum(_bool(item, "is_verified") for item in slot_candidates)
        enabled_count = sum(bool(source.crawl_enabled) for source in registered)
        verified_domains = {
            str(_value(item, "domain", "")).lower()
            for item in slot_candidates
            if _bool(item, "is_verified")
        }
        verified_enabled_count = sum(
            bool(source.crawl_enabled)
            and bool(source.official_domain_verified)
            and str(source.domain).lower() in verified_domains
            for source in registered
        )
        if status == "verified_enabled" and verified_enabled_count == 0:
            status = "candidate_failed_ambiguous"
            gates.append("enabled_source_without_verified_domain_match")
        rows.append(
            {
                "slot_id": slot_id,
                "city_id": city_id,
                "city_name": str(slot["city_name"]),
                "province_name": str(slot["province_name"]),
                "source_role": role,
                "registered_source_count": len(registered),
                "candidate_count": len(slot_candidates),
                "verified_candidate_count": verified_count,
                "enabled_source_count": enabled_count,
                "best_candidate_id": _value(best or {}, "candidate_id"),
                "best_candidate_url": _value(best or {}, "canonical_url"),
                "domain": _value(best or {}, "domain"),
                "candidate_kind": _value(best or {}, "candidate_kind"),
                "entry_eligible": _value(best or {}, "entry_eligible"),
                "official_status": "official" if best and (_bool(best, "is_official") or _official_domain(_value(best, "canonical_url"))) else "unknown",
                "official_domain_verified": any(
                    bool(source.official_domain_verified)
                    and (not best or str(source.domain).lower() == str(_value(best, "domain", "")).lower())
                    for source in registered
                ),
                "city_confidence": _value(best or {}, "city_confidence"),
                "role_confidence": _value(best or {}, "role_confidence"),
                "network_route": _value(best or {}, "network_route"),
                "http_status": _value(best or {}, "http_status"),
                "health_probe_success_count": _value(best or {}, "health_probe_success_count", 0),
                "parser_status": _value(best or {}, "parser_status"),
                "pagination_strategy": _value(best or {}, "pagination_strategy"),
                "failure_gates": json.dumps(list(dict.fromkeys(gates)), ensure_ascii=False),
                "recommended_action": {
                    "verified_enabled": "hold_until_next_milestone_approval",
                    "candidate_ready_for_probe": "probe_twice_then_verify",
                    "candidate_failed_fixable": "repair_candidate_evidence_then_probe",
                    "candidate_failed_ambiguous": "human_review",
                    "no_candidate_discoverable": "discover_official_entry_from_registry_or_portal",
                    "no_candidate_manual_research": "human_research_official_source",
                    "blocked_network": "retry_direct_route_with_browser_compatible_headers",
                    "blocked_role_conflict": "human_review_role_and_jurisdiction",
                    "blocked_parser": "inspect_list_parser_and_preserve_failure",
                    "blocked_pagination": "human_review_or_confirm_real_pagination",
                }[status],
                "requires_human_review": status in {"candidate_failed_ambiguous", "blocked_role_conflict", "no_candidate_manual_research", "blocked_pagination"},
                "priority": _PRIORITY[status],
                "updated_at": _now(),
                "work_status": status,
            }
        )
    checkpoint_store = GlobalSlotCheckpointStore(settings.outputs / "autopilot")
    checkpoint_records = checkpoint_store.snapshot()
    legacy = checkpoint_store.backfill_from_run_dirs(settings.outputs / "autopilot", apply=False)
    for item in legacy.get("proposals", []):
        slot_id = str(item.get("slot_id") or "")
        current = checkpoint_records.get(slot_id)
        if current is None or STATUS_PRIORITY.get(str(item.get("status") or "").upper(), 0) > STATUS_PRIORITY.get(str(current.get("status") or "").upper(), 0):
            checkpoint_records[slot_id] = item
    checkpoint_labels = {
        "CLAIMED": "claimed",
        "COMPLETED": "completed",
        "HUMAN_REVIEW": "human_review",
        "RETRY_WAIT": "retry_wait",
        "FAILED_RECOVERABLE": "failed_recoverable",
        "VERIFIED": "verified",
        "ENABLED": "enabled",
    }
    checkpoint_actions = {
        "CLAIMED": "slot_claimed_by_active_batch",
        "COMPLETED": "hold_completed_checkpoint",
        "HUMAN_REVIEW": "retain_human_review_until_decision",
        "RETRY_WAIT": "wait_until_next_retry_at",
        "FAILED_RECOVERABLE": "retry_recoverable_failure",
        "VERIFIED": "hold_verified_checkpoint",
        "ENABLED": "hold_enabled_checkpoint",
    }
    for row in rows:
        slot_id = str(row["slot_id"])
        checkpoint = checkpoint_records.get(slot_id)
        checkpoint_status = str((checkpoint or {}).get("status") or "").upper()
        row["checkpoint_status"] = checkpoint_status or None
        row["checkpoint_run_id"] = (checkpoint or {}).get("run_id")
        row["checkpoint_terminal_outcome"] = (checkpoint or {}).get("terminal_outcome")
        if not checkpoint_status:
            continue
        base_status = (
            "ENABLED"
            if row["work_status"] == "verified_enabled"
            else "VERIFIED"
            if int(row.get("verified_candidate_count") or 0) > 0
            else "UNRESOLVED"
        )
        if checkpoint_status == "CLAIMED":
            row["failure_gates"] = json.dumps(
                list(dict.fromkeys(json.loads(row["failure_gates"]) + ["checkpoint_claimed"])),
                ensure_ascii=False,
            )
            continue
        if STATUS_PRIORITY.get(checkpoint_status, 0) >= STATUS_PRIORITY.get(base_status, 0):
            label = checkpoint_labels.get(checkpoint_status)
            if label:
                row["work_status"] = label
                row["recommended_action"] = checkpoint_actions.get(checkpoint_status, row["recommended_action"])
                row["requires_human_review"] = checkpoint_status == "HUMAN_REVIEW"
                row["priority"] = _PRIORITY.get(label, row["priority"])
                row["failure_gates"] = json.dumps(
                    list(dict.fromkeys(json.loads(row["failure_gates"]) + [f"checkpoint_{label}"])),
                    ensure_ascii=False,
                )
    frame = pl.DataFrame(rows, infer_schema_length=None).sort(["priority", "province_name", "city_name", "source_role", "slot_id"])
    if frame.height != 525 or frame["city_id"].n_unique() != 105 or frame["slot_id"].n_unique() != 525:
        raise ValueError(f"invalid source completion queue shape: rows={frame.height}, cities={frame['city_id'].n_unique()}")
    return frame


def select_batch(queue: pl.DataFrame, *, max_slots: int = 20, max_cities: int = 10) -> pl.DataFrame:
    if max_slots < 1 or max_slots > 20 or max_cities < 1 or max_cities > 10:
        raise ValueError("batch caps must be at most 20 slots and 10 cities")
    eligible = queue.filter(
        pl.col("best_candidate_id").is_not_null()
        & pl.col("work_status").is_in(["candidate_ready_for_probe", "candidate_failed_fixable"])
    )
    selected: list[dict] = []
    cities: set[str] = set()
    for row in eligible.iter_rows(named=True):
        city_id = str(row["city_id"])
        if city_id not in cities and len(cities) >= max_cities:
            continue
        selected.append(row)
        cities.add(city_id)
        if len(selected) >= max_slots:
            break
    return pl.from_dicts(selected, schema=queue.schema) if selected else queue.head(0)


def _human_review_frame(queue: pl.DataFrame) -> pl.DataFrame:
    columns = ["review_id", "city", "role", "candidate_url", "candidate_title", "official_evidence", "conflicting_evidence", "machine_recommendation", "alternative_candidates", "exact_question_for_human", "allowed_decisions", "impact", "priority"]
    rows: list[dict] = []
    for row in queue.iter_rows(named=True):
        if not bool(row["requires_human_review"]):
            continue
        status = str(row["work_status"])
        question = {
            "blocked_role_conflict": "该候选是否确实属于该城市和必需机构角色？若不是，是否应更换角色或隔离？",
            "no_candidate_manual_research": "是否存在人工规则允许的官方入口或明确授权替代来源？",
            "blocked_pagination": "该栏目是否有真实可复现分页或停止条件？请提供证据。",
        }.get(status, "该候选是否可作为正式持续采集入口？若不可，请选择替代候选或隔离。")
        rows.append({
            "review_id": f"REVIEW_{row['slot_id']}",
            "city": row["city_name"],
            "role": row["source_role"],
            "candidate_url": row["best_candidate_url"],
            "candidate_title": row["candidate_kind"],
            "official_evidence": row["official_status"],
            "conflicting_evidence": row["failure_gates"],
            "machine_recommendation": row["recommended_action"],
            "alternative_candidates": "",
            "exact_question_for_human": question,
            "allowed_decisions": json.dumps(ALLOWED_DECISIONS, ensure_ascii=False),
            "impact": f"影响{row['city_name']}的{row['source_role']}必需来源槽位",
            "priority": row["priority"],
        })
    if not rows:
        return pl.DataFrame({column: pl.Series(column, [], dtype=pl.Int64 if column == "priority" else pl.String) for column in columns})
    return pl.DataFrame(rows)


def _write_json(path: Path, value: object) -> None:
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")


def _queue_stats(queue: pl.DataFrame) -> dict:
    return {
        "required_slots": queue.height,
        "cities": queue["city_id"].n_unique(),
        "roles_per_city": sorted(set(queue.group_by("city_id").len()["len"].to_list())),
        "status_counts": {str(row["work_status"]): int(row["len"]) for row in queue.group_by("work_status").len().iter_rows(named=True)},
        "verified_slots": int(queue.filter(pl.col("work_status") == "verified_enabled").height),
        "enabled_slots": int(queue.filter(pl.col("enabled_source_count") > 0).height),
        "enabled_unverified_slots": int(queue.filter((pl.col("enabled_source_count") > 0) & (pl.col("work_status") != "verified_enabled")).height),
    }


def create_source_completion_run(settings: Settings | None = None, *, run_id: str | None = None, max_slots: int = 20, max_cities: int = 10) -> dict:
    settings = settings or Settings.discover()
    run_id = run_id or datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    run_dir = settings.outputs / "source_completion" / run_id
    run_dir.mkdir(parents=True, exist_ok=False)
    queue = build_slot_work_queue(settings)
    batch = select_batch(queue, max_slots=max_slots, max_cities=max_cities)
    human = _human_review_frame(queue)
    atomic_write_parquet(queue, run_dir / "slot_work_queue.parquet", {"run_id": run_id, "job_id": "source-completion-queue"})
    queue.write_excel(run_dir / "slot_work_queue.xlsx", autofit=True)
    _write_json(run_dir / "slot_work_queue.json", queue.to_dicts())
    human.write_excel(run_dir / "HUMAN_REVIEW_QUEUE.xlsx", autofit=True)
    (run_dir / "HUMAN_REVIEW_GUIDE.md").write_text(
        "# CRPD 人工复核指南\n\n只在本表填写结构化决策，不直接修改Parquet或YAML。填写 `review_id`、`decision`，可选填写 `selected_candidate_id`、`reviewer`、`reason`。\n\n"
        f"允许决策：{', '.join(ALLOWED_DECISIONS)}。\n\n"
        "提交后使用 `policydb sources completion-import-decisions --input <文件>` 导入；导入流程只追加审计记录，不自动启用来源。\n",
        encoding="utf-8",
    )
    stats = _queue_stats(queue)
    _write_json(run_dir / "source_525_audit_before.json", {"generated_at": _now(), **stats, "source_audit": audit_525(settings)})
    _write_json(run_dir / "source_525_audit_after.json", {"status": "pending_batch", **stats})
    _write_json(run_dir / "milestone_status.json", {"milestone": 50, "status": "in_progress", "verified": stats["verified_slots"], "enabled": stats["enabled_slots"], "target": 50})
    _write_json(run_dir / "archive_parallel_report.json", {"status": "not_run", "remaining_ai_ineligible": "unknown_until_archive_audit"})
    _write_json(run_dir / "pytest_summary.json", {"status": "not_run"})
    _write_json(run_dir / "blockers.json", {"go_no_go": "BLOCKED", "blockers": ["full_run_forbidden", "real_ai_forbidden", "manual_acceptance_rules_pending"]})
    _write_json(run_dir / "rollback_manifest.json", {"status": "pending_backup", "run_dir": str(run_dir)})
    _write_json(run_dir / "batch_plan.json", {"run_id": run_id, "max_slots": max_slots, "max_cities": max_cities, "slots": batch.to_dicts(), "selected_slots": batch.height, "selected_cities": sorted(set(batch["city_id"].to_list()))})
    (run_dir / "NEXT_BATCH_COMMAND.ps1").write_text(
        "$ErrorActionPreference = 'Stop'\n# 仅在人工批准后执行；本文件由source_completion生成，不自动执行。\n"
        "$env:CRPD_DATA_ROOT = 'D:\\Data Set\\CRPD'\n"
        f"Write-Host 'source completion run: {run_id}'\n",
        encoding="utf-8",
    )
    return {"run_id": run_id, "run_dir": str(run_dir), "queue": queue, "batch": batch, "human": human, "stats": stats}


def create_immutable_backup(settings: Settings, run_dir: Path) -> dict:
    backup = settings.data_root / "backups" / f"source_completion_{run_dir.name}"
    backup.mkdir(parents=True, exist_ok=False)
    copied: list[str] = []
    for path in (settings.curated / "source_candidates.parquet", settings.curated / "source_requirement_slots.parquet", settings.curated / "source_registry.parquet", settings.root / "data" / "reference" / "source_registry.yaml"):
        if path.exists():
            target = backup / path.name
            shutil.copy2(path, target)
            copied.append(str(target))
    manifest = {"created_at": _now(), "backup_dir": str(backup), "files": copied}
    _write_json(run_dir / "rollback_manifest.json", manifest)
    return manifest


def import_human_decisions(settings: Settings, input_path: Path, *, run_id: str | None = None) -> dict:
    frame = pl.read_excel(input_path) if input_path.suffix.lower() in {".xlsx", ".xls"} else pl.read_csv(input_path)
    required = {"review_id", "decision"}
    missing = required - set(frame.columns)
    if missing:
        raise ValueError(f"human decision file missing columns: {sorted(missing)}")
    decisions = set(frame["decision"].drop_nulls().cast(pl.String).to_list())
    invalid = sorted(decisions - set(ALLOWED_DECISIONS))
    if invalid:
        raise ValueError(f"unsupported human decisions: {invalid}")
    run_id = run_id or datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    audit_path = settings.data_root / "manual_review" / "source_completion_decisions.jsonl"
    audit_path.parent.mkdir(parents=True, exist_ok=True)
    with audit_path.open("a", encoding="utf-8") as handle:
        for row in frame.to_dicts():
            handle.write(json.dumps({"decision_id": f"DECISION_{run_id}_{row['review_id']}", "run_id": run_id, "imported_at": _now(), **row}, ensure_ascii=False, default=str) + "\n")
    return {"imported": frame.height, "audit_path": str(audit_path), "decisions": sorted(decisions)}
