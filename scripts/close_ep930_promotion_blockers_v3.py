"""Build the EP930 V3 root-object closure from persisted rehearsal evidence.

The command is intentionally offline and isolated.  It does not start a
crawler, perform a web search, call an AI provider, or write the production
database/curated/raw roots.  The official controller is started separately
only after this audit has proven that no live writer or runner owns the work.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import polars as pl

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from policydb.episode_930_final_closure import (  # noqa: E402
    EPISODE_ID,
    SCOPE_HASH,
    SCOPE_VERSION,
    api_certification_summary,
    build_date_closure,
    build_direction_closure,
    build_recovery_disposition,
    build_root_objects,
    build_stale_lock_audit,
    file_sha256,
    root_counts,
)

DATA_ROOT = Path(r"E:\Data Set\CRPD")
OUTPUT_ROOT = DATA_ROOT / "outputs" / "special_projects" / "2016_930"
SCOPE_PATH = OUTPUT_ROOT / "930_ANALYSIS_READY_SCOPE.json"
OLD_RUN_ROOT = DATA_ROOT / "promotion_rehearsal" / "CRPD_PROMOTION_20260820T165634Z_BLOCKER_CLOSURE"


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def _atomic_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")
    os.replace(temporary, path)


def _atomic_csv(frame: pl.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    frame.write_csv(temporary)
    os.replace(temporary, path)


def _read_parquet(path: Path) -> pl.DataFrame:
    if not path.exists():
        return pl.DataFrame()
    if path.suffix.lower() == ".csv":
        return pl.read_csv(path)
    return pl.read_parquet(path)


def _latest_v2() -> Path:
    roots = sorted(
        (path for path in (DATA_ROOT / "promotion_rehearsal").glob("*_CLOSURE_V2") if path.is_dir()),
        key=lambda path: path.name,
        reverse=True,
    )
    if not roots:
        raise FileNotFoundError("no completed CLOSURE_V2 rehearsal exists")
    return roots[0]


def _new_run_root() -> Path:
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    base = DATA_ROOT / "promotion_rehearsal"
    root = base / f"CRPD_PROMOTION_{stamp}_CLOSURE_V3"
    suffix = 1
    while root.exists():
        root = base / f"CRPD_PROMOTION_{stamp}_CLOSURE_V3_{suffix}"
        suffix += 1
    root.mkdir(parents=True)
    return root


def _scope_or_fail() -> dict[str, Any]:
    scope = _json(SCOPE_PATH)
    if scope.get("scope_version") != SCOPE_VERSION or scope.get("scope_hash") != SCOPE_HASH or scope.get("frozen") is not True:
        raise RuntimeError("frozen Analysis-ready scope/version/hash is not the registered V3 value")
    return scope


def _inputs(v2: Path) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame, pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    curated = v2 / "data" / "curated"
    release = v2 / "release"
    actions = _read_parquet(curated / "policy_episode_actions.parquet")
    documents = _read_parquet(curated / "policy_episode_documents.parquet")
    parameters = _read_parquet(curated / "policy_episode_parameters.parquet")
    gaps = _read_parquet(curated / "policy_episode_gaps.parquet")
    matrix = _read_parquet(curated / "policy_episode_city_policy_matrix.parquet")
    gate = pl.read_csv(release / "EP930_ACTION_GATE_STATE.csv")
    if actions.is_empty() or documents.is_empty() or gate.is_empty():
        raise RuntimeError("V2 curated action/document/gate evidence is incomplete")
    selected_actions = _select_gate_scope_actions(actions, gate)
    selected_document_ids = selected_actions.select("document_id").drop_nulls().unique()
    selected_documents = documents.join(selected_document_ids, on="document_id", how="semi")
    return selected_actions, selected_documents, parameters, gaps, matrix, gate


def _select_gate_scope_actions(actions: pl.DataFrame, gate: pl.DataFrame) -> pl.DataFrame:
    """Keep every gate-selected action, including unmatched evidence roots."""

    selected = gate.select("action_id", "document_id").unique()
    matched = actions.join(selected.select("action_id"), on="action_id", how="semi")
    missing = selected.join(actions.select("action_id").unique(), on="action_id", how="anti")
    if missing.is_empty():
        return matched
    for column in actions.columns:
        if column in missing.columns:
            continue
        missing = missing.with_columns(pl.lit(None, dtype=actions.schema[column]).alias(column))
    return pl.concat([matched, missing.select(actions.columns)], how="vertical_relaxed")


def _copy_preserved_curated(v2: Path, run_root: Path) -> int:
    source = v2 / "data" / "curated"
    target = run_root / "data" / "curated"
    target.mkdir(parents=True, exist_ok=True)
    count = 0
    for path in source.glob("policy_episode_*.parquet"):
        shutil.copy2(path, target / path.name)
        count += 1
    summary = _json(v2 / "V2_RUN_SUMMARY.json")
    new_rows = (summary.get("formal_import") or {}).get("new_rows") or {}
    try:
        return int(new_rows.get("actions") or 0)
    except (TypeError, ValueError):
        return 0


def _scope_split(scope: dict[str, Any], roots: dict[str, int], metrics: dict[str, Any]) -> pl.DataFrame:
    return pl.DataFrame(
        [
            {"scope": "EP930_FROZEN_CORE", "metric": "scope_version", "value": scope.get("scope_version"), "blocking": False, "source": "frozen scope artifact"},
            {"scope": "EP930_FROZEN_CORE", "metric": "scope_hash", "value": scope.get("scope_hash"), "blocking": False, "source": "frozen scope artifact"},
            {"scope": "EP930_FROZEN_CORE", "metric": "scope_city_count", "value": scope.get("city_count"), "blocking": False, "source": "frozen scope artifact"},
            {"scope": "EP930_FROZEN_CORE", "metric": "scope_queue_item_count", "value": len(scope.get("queue_item_ids") or []), "blocking": False, "source": "frozen scope artifact"},
            {"scope": "EP930_SELECTED_ACTIONS", "metric": "independent_actions", "value": roots["actions"], "blocking": False, "source": "root-object join"},
            {"scope": "EP930_SELECTED_ACTIONS", "metric": "independent_evidence_units", "value": roots["evidence_units"], "blocking": False, "source": "root-object join"},
            {"scope": "EP930_SELECTED_ACTIONS", "metric": "independent_documents", "value": roots["documents"], "blocking": False, "source": "root-object join"},
            {"scope": "EP930_SELECTED_ACTIONS", "metric": "direction_unresolved", "value": metrics["direction_unresolved"], "blocking": True, "source": "V3 deterministic closure"},
            {"scope": "EP930_SELECTED_ACTIONS", "metric": "date_unresolved", "value": metrics["date_unresolved"], "blocking": True, "source": "V3 date closure"},
            {"scope": "EP930_SELECTED_ACTIONS", "metric": "episode_membership_conflicts", "value": metrics["episode_membership_conflicts"], "blocking": True, "source": "frozen 2016 episode window"},
            {"scope": "EP930_SELECTED_ACTIONS", "metric": "recovery_open", "value": metrics["recovery_open"], "blocking": True, "source": "targeted recovery disposition"},
            {"scope": "EP930_SELECTED_ACTIONS", "metric": "api_only_blockers", "value": metrics["api_only_blockers"], "blocking": True, "source": "official controller artifact"},
        ]
    )


def _promotion_actions(actions: pl.DataFrame) -> pl.DataFrame:
    if actions.is_empty():
        return pl.DataFrame()
    columns = [column for column in ("action_id", "document_id", "city", "policy_type", "action_text", "episode_direction", "action_direction", "effective_date", "effective_date_basis", "dedup_status") if column in actions.columns]
    return actions.select(columns).with_columns(
        pl.lit("PRESERVED_FROM_V2_ISOLATED_IMPORT").alias("v3_status"),
        pl.lit("NOT_PROMOTED_API_OR_EPISODE_GATE_OPEN").alias("v3_promotion_state"),
        pl.lit(False).alias("production_promotion"),
    )


def main() -> int:
    global DATA_ROOT, OUTPUT_ROOT, SCOPE_PATH, OLD_RUN_ROOT
    parser = argparse.ArgumentParser(description="Build an offline EP930 V3 final gate closure")
    parser.add_argument("--v2-run-root", type=Path, default=None)
    parser.add_argument("--data-root", type=Path, default=DATA_ROOT)
    args = parser.parse_args()

    DATA_ROOT = args.data_root.resolve()
    OUTPUT_ROOT = DATA_ROOT / "outputs" / "special_projects" / "2016_930"
    SCOPE_PATH = OUTPUT_ROOT / "930_ANALYSIS_READY_SCOPE.json"
    OLD_RUN_ROOT = DATA_ROOT / "promotion_rehearsal" / "CRPD_PROMOTION_20260820T165634Z_BLOCKER_CLOSURE"

    scope = _scope_or_fail()
    v2 = args.v2_run_root.resolve() if args.v2_run_root else _latest_v2()
    run_root = _new_run_root()
    release = run_root / "release"
    release.mkdir(parents=True)
    actions, documents, parameters, gaps, matrix, gate = _inputs(v2)
    recovery = _read_parquet(v2 / "release" / "EP930_RECOVERY_13_DISPOSITION.csv")
    direction = build_direction_closure(actions, gate)
    dates = build_date_closure(actions, documents, gate)
    recovery_v3 = build_recovery_disposition(recovery)
    root_objects = build_root_objects(actions, documents, gate, direction, dates, recovery_v3, api_blocked=True)
    roots = root_counts(root_objects)

    recovery_state = _json(OUTPUT_ROOT / "930_API_RECOVERY_STATE.json")
    provider_state = _json(OUTPUT_ROOT / "930_API_PROVIDER_STATUS.json")
    monitor = _json(OUTPUT_ROOT / "930_MONITOR_SNAPSHOT.json")
    api_pass1 = int((monitor.get("api_health") or {}).get("pass1_success") or 0)
    api_pass2 = int((monitor.get("api_health") or {}).get("pass2_success") or 0)
    api = api_certification_summary(recovery_state, provider_state, pass1_success=api_pass1, pass2_success=api_pass2, api_calls_this_run=0)

    direction_unresolved = int(direction.filter(pl.col("direction_state") != "PASS").height)
    date_unresolved = int(dates.filter(pl.col("date_state") != "PASS").height)
    membership_conflicts = int(
        dates.filter(pl.col("episode_membership_state") == "OUTSIDE_FROZEN_EPISODE_WINDOW").get_column("action_id").n_unique()
        if not dates.is_empty()
        else 0
    )
    recovery_open = int(
        recovery_v3.filter(pl.col("v3_state") == "OPEN").height
        if not recovery_v3.is_empty()
        else 0
    )
    action_root = root_objects.filter(pl.col("root_object_type") == "ACTION")
    if action_root.is_empty():
        api_only_blockers = 0
        non_api_root_blockers = 0
    else:
        has_api_blocker = pl.col("root_blockers").fill_null("").str.contains(
            "API_CERTIFICATION_BLOCKED", literal=True
        )
        has_non_api_blocker = (
            pl.col("root_blockers")
            .fill_null("")
            .str.replace("API_CERTIFICATION_BLOCKED", "")
            .str.strip_chars(";")
            .str.len_chars()
            > 0
        )
        api_only_blockers = int(action_root.filter(has_api_blocker & ~has_non_api_blocker).height)
        non_api_root_blockers = int(action_root.filter(has_non_api_blocker).height)
    preserved_formal_actions = _copy_preserved_curated(v2, run_root)
    promotion_actions = _promotion_actions(actions)
    gates = {
        "isolation_runtime": "PASS" if (v2 / "release" / "EP930_ISOLATION_RUNTIME_VALIDATION.json").exists() else "FAIL",
        "geography": f"{int(gate.filter(pl.col('post_geography_gate') == 'PASS').height)}/{gate.height}",
        "direction_unresolved": direction_unresolved,
        "date_unresolved": date_unresolved,
        "episode_membership_conflicts": membership_conflicts,
        "recovery_open": recovery_open,
        "selected_treatment_critical_completeness": non_api_root_blockers,
        "official_evidence": "PASS" if documents.height and documents.get_column("is_formal_eligible").all() else "FAIL",
        "critical_duplicates": int(actions.get_column("action_id").n_unique() != actions.height) if "action_id" in actions.columns else 1,
        "api_single": "PASS" if api.get("recovery_phase") in {"MICRO_5", "MICRO_20", "BACKLOG_CONSUMPTION"} and api.get("schema_valid") else "BLOCKED",
        "api_micro_5": "PASS" if api.get("recovery_phase") in {"MICRO_20", "BACKLOG_CONSUMPTION"} else "BLOCKED",
        "api_micro_20": "PASS" if api.get("recovery_phase") == "BACKLOG_CONSUMPTION" else "BLOCKED",
        "relevant_pass1": "PASS" if api_pass1 > 0 else "BLOCKED",
        "relevant_pass2": "PASS" if api_pass2 > 0 else "BLOCKED",
        "formal_actions_v3_new": 0,
        "preserved_isolated_formal_actions": preserved_formal_actions,
        "release_validator": "NOT_RUN_UPSTREAM_GATE_OPEN",
        "frozen_scope_hash_unchanged": scope.get("scope_hash") == SCOPE_HASH,
    }
    release_ready = (
        gates["isolation_runtime"] == "PASS"
        and gates["geography"] == "77/77"
        and gates["direction_unresolved"] == 0
        and gates["date_unresolved"] == 0
        and gates["episode_membership_conflicts"] == 0
        and gates["recovery_open"] == 0
        and gates["selected_treatment_critical_completeness"] == 0
        and gates["official_evidence"] == "PASS"
        and gates["critical_duplicates"] == 0
        and gates["api_single"] == "PASS"
        and gates["api_micro_5"] == "PASS"
        and gates["api_micro_20"] == "PASS"
        and gates["relevant_pass1"] == "PASS"
        and gates["relevant_pass2"] == "PASS"
        and preserved_formal_actions > 0
        and gates["frozen_scope_hash_unchanged"] is True
    )
    release_status = "PASS" if release_ready else "FAIL"
    validation = {
        "generated_at": _now(),
        "status": release_status,
        "scope": {"episode_id": EPISODE_ID, "scope_version": scope.get("scope_version"), "scope_hash": scope.get("scope_hash"), "scope_unit": "queue_item", "scope_city_count": scope.get("city_count"), "scope_queue_item_count": len(scope.get("queue_item_ids") or [])},
        "root_counts": roots,
        "gates": gates,
        "api": api,
        "network_or_api_calls_this_v3": 0,
        "production_write_allowed": False,
        "blocking_reasons": [
            reason for reason, condition in (
                ("EPISODE_MEMBERSHIP_CONFLICT", membership_conflicts > 0),
                ("DIRECTION_REVIEW_OPEN", direction_unresolved > 0),
                ("DATE_REVIEW_OPEN", date_unresolved > 0),
                ("RECOVERY_13_OPEN", recovery_open > 0),
                ("API_CERTIFICATION_BLOCKED", api["certification"] != "CERTIFIED"),
            ) if condition
        ],
    }
    decision = {
        "generated_at": _now(),
        "decision": "PROMOTE" if release_status == "PASS" else "DO_NOT_PROMOTE",
        "run_root": str(run_root),
        "scope": {"scope_version": scope.get("scope_version"), "scope_hash": scope.get("scope_hash"), "scope_unit": "queue_item", "scope_city_count": scope.get("city_count"), "scope_queue_item_count": len(scope.get("queue_item_ids") or [])},
        "independent_root_objects": roots,
        "formal_actions_promoted_v3": 0,
        "preserved_isolated_formal_actions": preserved_formal_actions,
        "gates": gates,
        "statement": "Diagnostic V3 closure only; no production promotion authorization exists unless every mandatory gate is PASS.",
    }

    lock_paths = [
        DATA_ROOT / "automation" / "AUTOMATION.lock",
        DATA_ROOT / "automation" / "ROLLING_24M_WRITER.lock",
        OUTPUT_ROOT / "930_AUTORUN.lock",
        DATA_ROOT / "logs" / "policydb-write.lock",
    ]
    stale_lock = build_stale_lock_audit(lock_paths, exclude_pid=os.getpid())
    api["stale_lock_audit_status"] = "PASS" if not any(item.get("status") == "LIVE_OWNER" for item in stale_lock["locks"]) else "LIVE_OWNER_PRESENT"
    writer_capable = stale_lock.get("writer_capable_process_count", 0)
    controller_audit = {
        "audited_at": _now(),
        "controller_started": False,
        "status": "BLOCKED_BY_WRITER_CAPABLE_PROCESS" if writer_capable else "READY_FOR_OFFICIAL_START",
        "blocking_pids": [
            item.get("pid")
            for item in stale_lock.get("live_relevant_processes", [])
            if item.get("process_kind") == "CRPD_WRITER_CAPABLE"
        ],
        "active_ep930_runner_count": stale_lock.get("active_ep930_runner_count", 0),
        "production_writer_count": stale_lock.get("production_writer_count", 0),
        "lock_cleanup_performed": False,
        "reason": "An existing CRPD backfill process can acquire the single writer; do not start EP930 concurrently.",
        "required_next_step": "Wait for the existing writer-capable process to terminate naturally, re-audit, then use official EP930 controller startup only if no writer-capable process remains.",
    }

    _atomic_csv(root_objects, release / "EP930_FINAL_ROOT_OBJECTS.csv")
    _atomic_csv(direction, release / "EP930_DIRECTION_CLOSURE.csv")
    _atomic_csv(dates, release / "EP930_DATE_CLOSURE.csv")
    _atomic_csv(recovery_v3, release / "EP930_RECOVERY_FINAL_DISPOSITION.csv")
    _atomic_csv(promotion_actions, release / "EP930_FINAL_PROMOTION_ACTIONS.csv")
    _atomic_csv(_scope_split(scope, roots, {"direction_unresolved": direction_unresolved, "date_unresolved": date_unresolved, "episode_membership_conflicts": membership_conflicts, "recovery_open": recovery_open, "api_only_blockers": api_only_blockers}), release / "EP930_COMPLETENESS_SCOPE_SPLIT_V3.csv")
    _atomic_json(release / "EP930_STALE_LOCK_AUDIT.json", stale_lock)
    _atomic_json(release / "EP930_CONTROLLER_START_AUDIT.json", controller_audit)
    _atomic_json(release / "EP930_API_CERTIFICATION_FINAL.json", api)
    _atomic_json(release / "EP930_RELEASE_VALIDATION_FINAL.json", validation)
    _atomic_json(release / "EP930_PROMOTION_DECISION_V3.json", decision)
    _atomic_json(release / "EP930_SCOPE_DEFINITION.json", scope)

    report = "\n".join(
        [
            "# EP930 Final Gate Closure V3",
            "",
            f"- decision: **{decision['decision']}**",
            f"- run_root: `{run_root}`",
            f"- scope: `{scope.get('scope_version')}` / `{scope.get('scope_hash')}` / `queue_item` / `{scope.get('city_count')} cities` / `{len(scope.get('queue_item_ids') or [])} queue items`",
            "",
            "## Independent roots",
            "",
            f"- actions: `{roots['actions']}`; evidence units: `{roots['evidence_units']}`; documents: `{roots['documents']}`; recovery items: `{roots['recovery_items']}`.",
            f"- Direction unresolved after V3 deterministic closure: `{direction_unresolved}`.",
            f"- Date unresolved after V3 date closure: `{date_unresolved}`; frozen-window membership conflicts: `{membership_conflicts}`.",
            f"- Recovery open: `{recovery_open}`; no V3 network requests were made.",
            "",
            "## API and promotion",
            "",
            f"- provider/model: `{api.get('provider')}` / `{api.get('model')}`; phase: `{api.get('recovery_phase')}`; certification: `{api.get('certification')}`.",
            f"- Pass1/Pass2 success observed by the existing artifact: `{api_pass1}/{api_pass2}`; V3 API calls: `0`; tokens/cost: `null/null`.",
            f"- preserved isolated formal actions: `{preserved_formal_actions}`; V3 production promotions: `0`.",
            f"- release validator: `{gates['release_validator']}`; final decision: **{decision['decision']}**.",
            f"- official controller: `{controller_audit['status']}`; started: `{controller_audit['controller_started']}`; blocking writer-capable PIDs: `{controller_audit['blocking_pids']}`.",
            "",
            "## Safety",
            "",
            "- Geography was not recomputed; the frozen scope/hash was checked unchanged.",
            "- V3 is isolated and offline; it did not modify production DuckDB, curated Parquet, raw/PDF, the 1575 queue, or historical locks.",
            "- The selected documents contain 2026 evidence rather than the frozen 2016 episode window; this is recorded as an episode-membership blocker, not silently promoted.",
        ]
    )
    (release / "EP930_FINAL_GATE_CLOSURE_V3_REPORT.md").write_text(report + "\n", encoding="utf-8")

    manifest_files = []
    for path in sorted(release.iterdir()):
        if path.name == "SHA256_MANIFEST.json" or not path.is_file():
            continue
        manifest_files.append({"path": path.name, "size_bytes": path.stat().st_size, "sha256": file_sha256(path)})
    manifest = {
        "generated_at": _now(),
        "manifest_self_excluded": True,
        "run_root": str(run_root),
        "scope_version": SCOPE_VERSION,
        "scope_hash": SCOPE_HASH,
        "files": manifest_files,
        "network_requests": 0,
        "api_requests": 0,
        "production_mutations": 0,
    }
    _atomic_json(release / "SHA256_MANIFEST.json", manifest)
    print(json.dumps({"run_root": str(run_root), "release": str(release), "decision": decision["decision"], "root_counts": roots, "direction_unresolved": direction_unresolved, "date_unresolved": date_unresolved, "episode_membership_conflicts": membership_conflicts, "recovery_open": recovery_open, "api_certification": api["certification"], "preserved_isolated_formal_actions": preserved_formal_actions, "controller_status": controller_audit["status"], "controller_started": controller_audit["controller_started"]}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
