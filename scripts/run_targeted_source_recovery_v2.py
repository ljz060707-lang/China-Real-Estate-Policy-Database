from __future__ import annotations

import argparse
import hashlib
import inspect
import json
import os
import re
import shutil
import subprocess
import sys
import time
from collections import defaultdict
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import polars as pl

from policydb.crawl.dedup import canonicalize_url
from policydb.crawl.registry import load_registry
from policydb.settings import Settings
from policydb.source_discovery import REQUIRED_ROLES
from policydb.source_slots import (
    build_requirement_slots,
    enable_source_strict,
    list_candidates,
    probe_candidates,
    promote_candidate,
    slot_paths,
    verify_candidates,
)

SHA256_RE = re.compile(r"^[0-9a-fA-F]{64}$")
GENERIC_TITLES = {
    "",
    "policies",
    "policy",
    "home",
    "index",
    "government",
}
ENTRY_KIND_PRIORITY = {
    "department_entry_candidate": 4,
    "official_entry_candidate": 3,
    "municipal_portal_substitute_candidate": 2,
    "policy_content_evidence": 0,
}
PARSER_PRIORITY = {
    "pagination_detected": 4,
    "list_detected": 3,
    "parser_verified": 3,
    "parser_ready": 2,
    "pending_evaluation": 0,
}
HISTORICAL_STATUSES = {
    "rejected_by_gate",
    "quarantined_invalid_probe_evidence",
}


def utcnow() -> datetime:
    return datetime.now(UTC)


def iso_now() -> str:
    return utcnow().isoformat()


def parse_datetime(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def parse_json(value: Any, default: Any = None) -> Any:
    if isinstance(value, (list, dict)):
        return value
    if value is None:
        return default
    try:
        return json.loads(str(value))
    except (TypeError, json.JSONDecodeError):
        return default


def boolish(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {
        "1",
        "true",
        "yes",
        "y",
        "on",
    }


def atomic_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + ".tmp")
    temp.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )
    os.replace(temp, path)


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def safe_filename(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", value)


def candidate_frame(
    *,
    settings: Settings,
    candidate_ids: list[str] | None = None,
) -> pl.DataFrame:
    frame = list_candidates(settings=settings)
    if candidate_ids is not None:
        frame = frame.filter(
            pl.col("candidate_id").is_in(candidate_ids)
        )
    return frame


def one_candidate(
    candidate_id: str,
    *,
    settings: Settings,
) -> dict[str, Any]:
    frame = list_candidates(
        candidate_id=candidate_id,
        settings=settings,
    )
    if frame.height != 1:
        raise RuntimeError(
            f"{candidate_id}: expected one candidate, found {frame.height}"
        )
    return frame.to_dicts()[0]


def probe_integrity(
    row: dict[str, Any],
    *,
    expected_rounds: int,
    started_at: datetime,
) -> dict[str, Any]:
    probes = parse_json(row.get("probe_evidence_json"), [])
    if not isinstance(probes, list):
        probes = []

    grace_start = started_at - timedelta(seconds=30)
    fresh: list[dict[str, Any]] = []

    for item in probes:
        if not isinstance(item, dict):
            continue
        checked_at = parse_datetime(
            item.get("checked_at")
            or item.get("last_checked_at")
        )
        if checked_at is not None and checked_at >= grace_start:
            fresh.append(item)

    errors: list[str] = []
    warnings: list[str] = []

    if len(fresh) < expected_rounds:
        errors.append(
            f"fresh_probe_rounds={len(fresh)}<{expected_rounds}"
        )

    selected = fresh[-expected_rounds:] if fresh else []
    valid_hashes: list[str] = []
    invalid_hashes: list[str] = []
    titles: list[str] = []
    status_codes: list[int | None] = []
    network_routes: list[str] = []

    for item in selected:
        response_hash = str(
            item.get("response_sha256") or ""
        ).strip()
        title = str(item.get("page_title") or "").strip()
        route = str(item.get("network_route") or "").strip()
        status_raw = item.get("status_code")

        try:
            status_code = int(status_raw)
        except (TypeError, ValueError):
            status_code = None

        titles.append(title)
        status_codes.append(status_code)
        network_routes.append(route)

        if (
            SHA256_RE.fullmatch(response_hash)
            and len(set(response_hash.lower())) >= 8
        ):
            valid_hashes.append(response_hash.lower())
        else:
            invalid_hashes.append(response_hash)

    if len(valid_hashes) < expected_rounds:
        errors.append(
            f"valid_sha256={len(valid_hashes)}<{expected_rounds}"
        )
    if invalid_hashes:
        errors.append(
            "invalid_sha256=" + "|".join(invalid_hashes)
        )
    if any(code is None for code in status_codes):
        errors.append("missing_or_invalid_http_status")
    if any(not route for route in network_routes):
        errors.append("missing_network_route")
    if selected and all(
        title.lower() in GENERIC_TITLES
        for title in titles
    ):
        warnings.append(
            "all_page_titles_are_generic"
        )

    return {
        "ok": not errors,
        "fresh_probe_rounds": len(fresh),
        "selected_probe_rounds": len(selected),
        "valid_sha256_count": len(valid_hashes),
        "invalid_hashes": invalid_hashes,
        "status_codes": status_codes,
        "network_routes": network_routes,
        "page_titles": titles,
        "errors": errors,
        "warnings": warnings,
    }


def global_verified_integrity(
    *,
    settings: Settings,
) -> dict[str, Any]:
    frame = candidate_frame(settings=settings)
    if (
        frame.height
        and "manual_review_status" in frame.columns
    ):
        frame = frame.filter(
            ~pl.col("manual_review_status")
            .fill_null("")
            .cast(pl.String)
            .str.starts_with("excluded_")
        )

    verified = frame.filter(
        pl.col("is_verified").fill_null(False)
    )

    invalid: list[dict[str, Any]] = []

    for row in verified.to_dicts():
        probes = parse_json(
            row.get("probe_evidence_json"),
            [],
        )
        if not isinstance(probes, list):
            probes = []

        valid_count = 0
        invalid_values: list[str] = []

        for item in probes:
            if not isinstance(item, dict):
                continue
            value = str(
                item.get("response_sha256") or ""
            ).strip()
            if (
                SHA256_RE.fullmatch(value)
                and len(set(value.lower())) >= 8
            ):
                valid_count += 1
            elif value:
                invalid_values.append(value)

        if valid_count == 0:
            invalid.append(
                {
                    "candidate_id": row.get("candidate_id"),
                    "slot_id": row.get("slot_id"),
                    "city_id": row.get("city_id"),
                    "source_role": row.get("source_role"),
                    "candidate_url": row.get("candidate_url"),
                    "invalid_hash_values": invalid_values,
                }
            )

    return {
        "verified_candidate_count": verified.height,
        "invalid_verified_candidate_count": len(invalid),
        "invalid_candidates": invalid,
    }


def source_role(source: Any) -> str:
    agency = str(getattr(source, "agency_type", "") or "")
    role = str(getattr(source, "source_role", "") or "")
    if agency in REQUIRED_ROLES:
        return agency
    if role in REQUIRED_ROLES:
        return role
    return ""


def enabled_registry_by_slot(
    *,
    settings: Settings,
) -> dict[tuple[str, str], list[str]]:
    result: dict[tuple[str, str], list[str]] = defaultdict(list)
    for source in load_registry(settings):
        if not bool(getattr(source, "crawl_enabled", False)):
            continue
        role = source_role(source)
        if not role:
            continue
        for city_id in getattr(source, "city_ids", []) or []:
            result[(str(city_id), role)].append(
                str(source.source_id)
            )
    return dict(result)


def canonical(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    try:
        return canonicalize_url(text)
    except Exception:
        return text.lower().rstrip("/")


def source_urls(source: Any) -> set[str]:
    values = [
        getattr(source, "homepage_url", None),
        *list(getattr(source, "list_page_urls", []) or []),
    ]
    return {
        canonical(value)
        for value in values
        if value
    }


def enabled_registry_records_by_slot(
    *,
    settings: Settings,
) -> dict[tuple[str, str], list[Any]]:
    result: dict[tuple[str, str], list[Any]] = defaultdict(list)
    for source in load_registry(settings):
        if not bool(getattr(source, "crawl_enabled", False)):
            continue
        role = source_role(source)
        if not role:
            continue
        for city_id in getattr(source, "city_ids", []) or []:
            result[(str(city_id), role)].append(source)
    return dict(result)


def candidate_rank(row: dict[str, Any]) -> tuple[Any, ...]:
    return (
        ENTRY_KIND_PRIORITY.get(
            str(row.get("candidate_kind") or ""),
            -1,
        ),
        int(boolish(row.get("entry_eligible"))),
        int(
            str(row.get("page_type") or "")
            == "site_or_column_entry"
        ),
        PARSER_PRIORITY.get(
            str(row.get("parser_status") or ""),
            -1,
        ),
        int(row.get("health_probe_success_count") or 0),
        float(row.get("overall_confidence") or 0.0),
        -len(str(row.get("canonical_url") or "")),
        str(row.get("candidate_id") or ""),
    )


def choose_one_per_slot(
    frame: pl.DataFrame,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    by_slot: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in frame.to_dicts():
        by_slot[str(row["slot_id"])].append(row)

    selected: list[dict[str, Any]] = []
    alternates: list[dict[str, Any]] = []

    for _slot_id, rows in sorted(by_slot.items()):
        rows.sort(key=candidate_rank, reverse=True)
        selected.append(rows[0])
        for row in rows[1:]:
            alternates.append(row)

    return selected, alternates


def extract_source_id(value: Any) -> str | None:
    if isinstance(value, dict):
        direct = value.get("source_id")
        if direct:
            return str(direct)
        for nested in value.values():
            found = extract_source_id(nested)
            if found:
                return found
    elif isinstance(value, list):
        for item in value:
            found = extract_source_id(item)
            if found:
                return found
    return None


def run_command(
    command: list[str],
    *,
    cwd: Path,
    env: dict[str, str],
    log_path: Path,
    timeout: int,
) -> dict[str, Any]:
    started = time.monotonic()
    try:
        result = subprocess.run(
            command,
            cwd=cwd,
            env=env,
            text=True,
            encoding="utf-8",
            errors="replace",
            capture_output=True,
            timeout=timeout,
            check=False,
        )
        status = "ok" if result.returncode == 0 else "failed"
        return_code = result.returncode
        stdout = result.stdout
        stderr = result.stderr
    except subprocess.TimeoutExpired as exc:
        status = "timeout"
        return_code = -9
        stdout = exc.stdout or ""
        stderr = (exc.stderr or "") + "\nTIMEOUT"
    except OSError as exc:
        status = "error"
        return_code = -1
        stdout = ""
        stderr = f"{type(exc).__name__}: {exc}"

    payload = {
        "command": command,
        "status": status,
        "return_code": return_code,
        "elapsed_seconds": round(
            time.monotonic() - started,
            3,
        ),
        "stdout": stdout,
        "stderr": stderr,
    }
    atomic_json(log_path, payload)
    return payload


def ensure_targeted_api() -> None:
    probe_signature = inspect.signature(probe_candidates)
    verify_signature = inspect.signature(verify_candidates)

    missing = []
    if "candidate_ids" not in probe_signature.parameters:
        missing.append(
            "probe_candidates(candidate_ids=...)"
        )
    if "candidate_ids" not in verify_signature.parameters:
        missing.append(
            "verify_candidates(candidate_ids=...)"
        )

    if missing:
        raise RuntimeError(
            "Targeted API is unavailable: "
            + ", ".join(missing)
            + ". Refusing broad city/global probing."
        )


def worker(
    *,
    candidate_id: str,
    rounds: int,
    data_root: str | None,
) -> int:
    if data_root:
        os.environ["CRPD_DATA_ROOT"] = data_root
    os.environ["PYTHONUTF8"] = "1"
    os.environ["PYTHONIOENCODING"] = "utf-8"

    settings = Settings.discover()
    ensure_targeted_api()

    result = probe_candidates(
        candidate_ids=[candidate_id],
        rounds=rounds,
        settings=settings,
    )
    print(
        json.dumps(
            result,
            ensure_ascii=False,
            indent=2,
            default=str,
        )
    )
    return 0


def load_queue(path: Path) -> tuple[pl.DataFrame, list[str]]:
    if not path.exists():
        raise FileNotFoundError(path)

    if path.suffix.lower() == ".parquet":
        frame = pl.read_parquet(path)
    else:
        frame = pl.read_csv(
            path,
            infer_schema_length=None,
        )

    if "candidate_id" not in frame.columns:
        raise ValueError(
            f"Queue has no candidate_id column: {path}"
        )

    ids = [
        str(value).strip()
        for value in frame["candidate_id"].to_list()
        if str(value or "").strip()
    ]
    ids = list(dict.fromkeys(ids))
    if not ids:
        raise ValueError("Queue contains no candidate IDs.")

    return frame, ids


def make_run_id(
    queue_path: Path,
    candidate_ids: list[str],
) -> str:
    digest = hashlib.sha256(
        (
            queue_path.resolve().as_posix()
            + "\n"
            + "\n".join(candidate_ids)
        ).encode("utf-8")
    ).hexdigest()[:12]
    return (
        "targeted_recovery_"
        + queue_path.stem[:35]
        + "_"
        + digest
    )


def backup_once(
    *,
    settings: Settings,
    run_dir: Path,
) -> dict[str, str]:
    backup_dir = run_dir / "backups"
    backup_dir.mkdir(parents=True, exist_ok=True)

    candidate_path = slot_paths(settings)[1]
    registry_yaml = (
        settings.root
        / "data"
        / "reference"
        / "source_registry.yaml"
    )
    registry_parquet = (
        settings.curated
        / "source_registry.parquet"
    )
    slots_path = slot_paths(settings)[0]

    sources = {
        "source_candidates.parquet": candidate_path,
        "source_requirement_slots.parquet": slots_path,
        "source_registry.yaml": registry_yaml,
        "source_registry.parquet": registry_parquet,
    }

    manifest: dict[str, Any] = {
        "created_at": iso_now(),
        "files": [],
    }

    for name, source in sources.items():
        if not source.exists():
            continue
        destination = backup_dir / name
        if not destination.exists():
            shutil.copy2(source, destination)
        manifest["files"].append(
            {
                "name": name,
                "source": str(source),
                "backup": str(destination),
                "sha256": file_sha256(destination),
                "size_bytes": destination.stat().st_size,
            }
        )

    manifest_path = backup_dir / "backup_manifest.json"
    if not manifest_path.exists():
        atomic_json(manifest_path, manifest)

    return {
        item["name"]: item["backup"]
        for item in manifest["files"]
    }


def create_baseline(
    *,
    settings: Settings,
    run_dir: Path,
    report_path: Path,
) -> Path:
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    baseline = (
        settings.outputs
        / "acceptance"
        / f"stable_baseline_{stamp}"
    )
    baseline.mkdir(parents=True, exist_ok=False)

    sources = {
        "source_candidates.parquet":
            slot_paths(settings)[1],
        "source_requirement_slots.parquet":
            slot_paths(settings)[0],
        "source_registry.parquet":
            settings.curated / "source_registry.parquet",
        "source_registry.yaml":
            settings.root
            / "data"
            / "reference"
            / "source_registry.yaml",
        "source_525_audit.csv":
            settings.outputs
            / "acceptance"
            / "source_525_audit.csv",
        "targeted_recovery_report.json":
            report_path,
        "pipeline_state.json":
            run_dir / "state.json",
    }

    manifest = {
        "created_at": iso_now(),
        "files": [],
    }

    for name, source in sources.items():
        if not source.exists():
            continue
        destination = baseline / name
        shutil.copy2(source, destination)
        manifest["files"].append(
            {
                "name": name,
                "source": str(source),
                "size_bytes": destination.stat().st_size,
                "sha256": file_sha256(destination),
            }
        )

    atomic_json(
        baseline / "baseline_manifest.json",
        manifest,
    )
    return baseline


def regenerate_outputs(
    *,
    settings: Settings,
    run_dir: Path,
    timeout: int,
) -> list[dict[str, Any]]:
    env = os.environ.copy()
    env["CRPD_DATA_ROOT"] = str(settings.data_root)
    env["PYTHONUTF8"] = "1"
    env["PYTHONIOENCODING"] = "utf-8"

    policydb = (
        settings.root
        / ".venv"
        / "Scripts"
        / "policydb.exe"
    )

    commands: list[list[str]] = []

    if policydb.exists():
        commands.append(
            [
                str(policydb),
                "sources",
                "export-candidate-audit",
            ]
        )

    for script_name in [
        "build_source_525_action_queue.py",
        "build_department_entry_review.py",
        "build_department_entry_slot_shortlist.py",
        "audit_post_dedupe_conflicts.py",
        "audit_verified_probe_integrity.py",
    ]:
        path = settings.root / "scripts" / script_name
        if path.exists():
            commands.append(
                [sys.executable, str(path)]
            )

    results = []
    log_dir = run_dir / "post_audit_logs"
    log_dir.mkdir(parents=True, exist_ok=True)

    for index, command in enumerate(commands, start=1):
        name = safe_filename(Path(command[-1]).stem)
        result = run_command(
            command,
            cwd=settings.root,
            env=env,
            log_path=log_dir / f"{index:02d}_{name}.json",
            timeout=timeout,
        )
        results.append(result)

    return results


def classify_conflict_report(
    *,
    settings: Settings,
) -> dict[str, Any]:
    """Classify real conflicts by canonical-URL group.

    A single current candidate sharing a URL only with rejected/quarantined
    history is not an active conflict. A group is active only when at least
    two current rows remain and they span multiple slots/roles, or when
    multiple current candidates remain in the same slot.
    """
    path = (
        settings.outputs
        / "acceptance"
        / "post_dedupe_audit"
        / "remaining_cross_slot_conflicts.csv"
    )
    if not path.exists():
        return {
            "path": str(path),
            "rows": 0,
            "historical_rows": 0,
            "active_nonconflicting_rows": 0,
            "active_rows": 0,
            "active_conflict_groups": 0,
            "active_conflicts": [],
        }

    frame = pl.read_csv(
        path,
        infer_schema_length=None,
    )

    historical_rows: list[dict[str, Any]] = []
    current_rows: list[dict[str, Any]] = []

    for row in frame.to_dicts():
        status = str(
            row.get("manual_review_status") or ""
        )
        verified = boolish(row.get("is_verified"))

        is_historical = (
            status in HISTORICAL_STATUSES
            or status.startswith("excluded_")
            or status.startswith("quarantined_")
        ) and not verified

        if is_historical:
            historical_rows.append(row)
        else:
            current_rows.append(row)

    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in current_rows:
        key = canonical(
            row.get("canonical_url")
            or row.get("candidate_url")
        )
        grouped[key].append(row)

    active_conflicts: list[dict[str, Any]] = []
    conflict_candidate_ids: set[str] = set()

    for url, rows in sorted(grouped.items()):
        candidate_ids = {
            str(row.get("candidate_id") or "")
            for row in rows
            if row.get("candidate_id")
        }
        slot_keys = {
            (
                str(row.get("slot_id") or ""),
                str(row.get("city_id") or ""),
                str(row.get("source_role") or ""),
            )
            for row in rows
        }

        is_conflict = (
            len(candidate_ids) >= 2
            or len(slot_keys) >= 2
        )

        if not is_conflict:
            continue

        conflict_candidate_ids.update(candidate_ids)
        active_conflicts.append(
            {
                "canonical_url": url,
                "active_row_count": len(rows),
                "candidate_ids": sorted(candidate_ids),
                "slot_keys": [
                    {
                        "slot_id": slot_id,
                        "city_id": city_id,
                        "source_role": role,
                    }
                    for slot_id, city_id, role
                    in sorted(slot_keys)
                ],
            }
        )

    return {
        "path": str(path),
        "rows": frame.height,
        "historical_rows": len(historical_rows),
        "active_nonconflicting_rows": (
            len(current_rows)
            - len(conflict_candidate_ids)
        ),
        "active_rows": len(conflict_candidate_ids),
        "active_conflict_groups": len(active_conflicts),
        "active_conflicts": active_conflicts,
    }


def main(args: argparse.Namespace) -> int:
    if args.data_root:
        os.environ["CRPD_DATA_ROOT"] = args.data_root
    os.environ["PYTHONUTF8"] = "1"
    os.environ["PYTHONIOENCODING"] = "utf-8"

    settings = Settings.discover()
    ensure_targeted_api()

    queue_path = Path(args.queue).expanduser()
    if not queue_path.is_absolute():
        queue_path = (
            settings.root / queue_path
        ).resolve()

    _, candidate_ids = load_queue(queue_path)

    if (
        args.expected_count > 0
        and len(candidate_ids) != args.expected_count
    ):
        raise RuntimeError(
            f"Expected {args.expected_count} candidates, "
            f"found {len(candidate_ids)}."
        )

    live = candidate_frame(
        settings=settings,
        candidate_ids=candidate_ids,
    )
    live_ids = {
        str(value)
        for value in live["candidate_id"].to_list()
    }
    missing = sorted(set(candidate_ids) - live_ids)
    if missing:
        raise RuntimeError(
            "Queue candidates are missing from live data:\n"
            + "\n".join(missing)
        )

    run_id = args.run_id or make_run_id(
        queue_path,
        candidate_ids,
    )
    run_dir = (
        settings.outputs
        / "acceptance"
        / "targeted_source_recovery"
        / run_id
    )
    run_dir.mkdir(parents=True, exist_ok=True)

    state_path = run_dir / "state.json"
    report_path = run_dir / "final_report.json"

    if state_path.exists():
        state = parse_json(
            state_path.read_text(encoding="utf-8"),
            {},
        )
    else:
        state = {
            "run_id": run_id,
            "created_at": iso_now(),
            "queue_path": str(queue_path),
            "queue_sha256": file_sha256(queue_path),
            "candidate_ids": candidate_ids,
            "rounds": args.rounds,
            "status": "planned",
            "candidates": {},
        }
        atomic_json(state_path, state)

    audit_before = build_requirement_slots(settings)

    print("=" * 78)
    print("TARGETED SOURCE RECOVERY")
    print(f"Mode              : {'APPLY' if args.apply else 'PLAN'}")
    print(f"Run ID            : {run_id}")
    print(f"Queue             : {queue_path}")
    print(f"Candidates        : {len(candidate_ids)}")
    print(f"Probe rounds      : {args.rounds}")
    print(f"Per-item timeout  : {args.candidate_timeout_seconds}s")
    print(f"Run directory     : {run_dir}")
    print(f"Verified/enabled  : "
          f"{audit_before.get('slots_verified')}/"
          f"{audit_before.get('slots_enabled')}")
    print("=" * 78)

    if not args.apply:
        print("Plan only; no network probes or registry changes were made.")
        return 0

    backups = backup_once(
        settings=settings,
        run_dir=run_dir,
    )
    state["status"] = "probing"
    state["started_at"] = state.get("started_at") or iso_now()
    state["backups"] = backups
    atomic_json(state_path, state)

    env = os.environ.copy()
    env["CRPD_DATA_ROOT"] = str(settings.data_root)
    env["PYTHONUTF8"] = "1"
    env["PYTHONIOENCODING"] = "utf-8"

    probe_log_dir = run_dir / "probe_logs"
    probe_log_dir.mkdir(parents=True, exist_ok=True)

    successful_integrity_ids: list[str] = []
    failed_probe_ids: list[str] = []

    if args.finalize_only:
        print("FINALIZE-ONLY: no new network probes will run.")
        for index, candidate_id in enumerate(candidate_ids, start=1):
            prior = state["candidates"].get(candidate_id, {})
            started = parse_datetime(
                prior.get("probe_started_at")
            )
            if started is None:
                failed_probe_ids.append(candidate_id)
                print(
                    f"[{index}/{len(candidate_ids)}] "
                    f"NO STATE {candidate_id}"
                )
                continue

            current_row = one_candidate(
                candidate_id,
                settings=settings,
            )
            integrity = probe_integrity(
                current_row,
                expected_rounds=args.rounds,
                started_at=started,
            )

            if integrity["ok"]:
                successful_integrity_ids.append(candidate_id)
                print(
                    f"[{index}/{len(candidate_ids)}] "
                    f"VALID {candidate_id}"
                )
            else:
                failed_probe_ids.append(candidate_id)
                print(
                    f"[{index}/{len(candidate_ids)}] "
                    f"FAILED {candidate_id}: "
                    + "; ".join(integrity.get("errors", []))
                )
    else:
        for index, candidate_id in enumerate(candidate_ids, start=1):
            prior = state["candidates"].get(candidate_id, {})

            if (
                not args.force
                and prior.get("integrity", {}).get("ok") is True
            ):
                current_row = one_candidate(
                    candidate_id,
                    settings=settings,
                )
                started = parse_datetime(
                    prior.get("probe_started_at")
                )
                if started is not None:
                    current_integrity = probe_integrity(
                        current_row,
                        expected_rounds=args.rounds,
                        started_at=started,
                    )
                    if current_integrity["ok"]:
                        successful_integrity_ids.append(
                            candidate_id
                        )
                        print(
                            f"[{index}/{len(candidate_ids)}] "
                            f"SKIP {candidate_id}: already valid"
                        )
                        continue

            probe_started = utcnow()
            command = [
                sys.executable,
                str(Path(__file__).resolve()),
                "--worker",
                "--candidate-id",
                candidate_id,
                "--rounds",
                str(args.rounds),
                "--data-root",
                str(settings.data_root),
            ]

            print(
                f"[{index}/{len(candidate_ids)}] "
                f"PROBE {candidate_id}"
            )

            result = run_command(
                command,
                cwd=settings.root,
                env=env,
                log_path=(
                    probe_log_dir
                    / f"{index:03d}_{candidate_id}.json"
                ),
                timeout=args.candidate_timeout_seconds,
            )

            entry = {
                "candidate_id": candidate_id,
                "probe_started_at": probe_started.isoformat(),
                "probe_finished_at": iso_now(),
                "worker_status": result["status"],
                "worker_return_code": result["return_code"],
                "worker_elapsed_seconds": result["elapsed_seconds"],
            }

            if result["return_code"] == 0:
                row = one_candidate(
                    candidate_id,
                    settings=settings,
                )
                integrity = probe_integrity(
                    row,
                    expected_rounds=args.rounds,
                    started_at=probe_started,
                )
            else:
                integrity = {
                    "ok": False,
                    "errors": [
                        f"worker_{result['status']}"
                    ],
                    "warnings": [],
                }

            entry["integrity"] = integrity
            state["candidates"][candidate_id] = entry
            atomic_json(state_path, state)

            if integrity.get("ok"):
                successful_integrity_ids.append(
                    candidate_id
                )
                print(
                    f"  OK: {integrity.get('valid_sha256_count')} "
                    "valid fresh hashes"
                )
            else:
                failed_probe_ids.append(candidate_id)
                print(
                    "  FAIL: "
                    + "; ".join(
                        integrity.get("errors", [])
                    )
                )

    state["status"] = "verifying"
    state["successful_integrity_ids"] = successful_integrity_ids
    state["failed_probe_ids"] = failed_probe_ids
    atomic_json(state_path, state)

    verification_result: dict[str, Any] = {
        "checked": 0,
        "verified": 0,
    }

    if successful_integrity_ids:
        verify_kwargs: dict[str, Any] = {
            "candidate_ids": successful_integrity_ids,
            "settings": settings,
        }
        if (
            "run_id"
            in inspect.signature(
                verify_candidates
            ).parameters
        ):
            verify_kwargs["run_id"] = run_id

        verification_result = verify_candidates(
            **verify_kwargs
        )

    verified_targets_all = candidate_frame(
        settings=settings,
        candidate_ids=successful_integrity_ids,
    ).filter(
        pl.col("is_verified").fill_null(False)
    )

    safe_verified_rows = []
    unsafe_verified_rows = []

    for row in verified_targets_all.to_dicts():
        reusable = (
            str(row.get("candidate_kind") or "")
            in {
                "department_entry_candidate",
                "official_entry_candidate",
            }
            and str(row.get("page_type") or "")
            == "site_or_column_entry"
            and boolish(row.get("entry_eligible"))
        )
        if reusable:
            safe_verified_rows.append(row)
        else:
            unsafe_verified_rows.append(row)

    verified_targets = (
        pl.DataFrame(
            safe_verified_rows,
            infer_schema_length=None,
        )
        if safe_verified_rows
        else verified_targets_all.head(0)
    )

    selected, alternates = choose_one_per_slot(
        verified_targets
    )

    state["status"] = "promoting_and_enabling"
    state["verification_result"] = verification_result
    state["verified_target_ids"] = (
        verified_targets["candidate_id"].to_list()
        if verified_targets.height
        else []
    )
    state["selected_candidate_ids"] = [
        row["candidate_id"]
        for row in selected
    ]
    state["verified_alternate_ids"] = [
        row["candidate_id"]
        for row in alternates
    ]
    atomic_json(state_path, state)

    promoted: list[dict[str, Any]] = []
    enabled: list[dict[str, Any]] = []
    already_enabled: list[dict[str, Any]] = []
    promotion_errors: list[dict[str, Any]] = []

    for row in selected:
        candidate_id = str(row["candidate_id"])
        city_id = str(row["city_id"])
        role = str(row["source_role"])

        current_records = enabled_registry_records_by_slot(
            settings=settings
        ).get((city_id, role), [])

        if current_records:
            candidate_url = canonical(
                row.get("canonical_url")
                or row.get("candidate_url")
            )
            exact_matches = [
                source
                for source in current_records
                if candidate_url in source_urls(source)
            ]

            if exact_matches:
                already_enabled.append(
                    {
                        "candidate_id": candidate_id,
                        "slot_id": row["slot_id"],
                        "source_ids": [
                            str(source.source_id)
                            for source in exact_matches
                        ],
                        "status": "already_enabled_exact_url",
                    }
                )
                continue

            promotion_errors.append(
                {
                    "candidate_id": candidate_id,
                    "stage": "pre_enable_guard",
                    "error": (
                        "slot_has_different_enabled_source: "
                        + "|".join(
                            str(source.source_id)
                            for source in current_records
                        )
                    ),
                }
            )
            continue

        try:
            promotion = promote_candidate(
                candidate_id,
                settings=settings,
            )
            source_id = extract_source_id(
                promotion
            )
            if not source_id:
                raise RuntimeError(
                    "promotion_result_has_no_source_id"
                )

            promoted.append(
                {
                    "candidate_id": candidate_id,
                    "slot_id": row["slot_id"],
                    "source_id": source_id,
                    "result": promotion,
                }
            )

            enablement = enable_source_strict(
                source_id,
                settings=settings,
            )
            enabled.append(
                {
                    "candidate_id": candidate_id,
                    "slot_id": row["slot_id"],
                    "source_id": source_id,
                    "result": enablement,
                }
            )

        except Exception as exc:
            promotion_errors.append(
                {
                    "candidate_id": candidate_id,
                    "stage": "promote_or_enable",
                    "error_type": type(exc).__name__,
                    "error": str(exc),
                }
            )

    audit_after = build_requirement_slots(
        settings
    )
    integrity_after = global_verified_integrity(
        settings=settings
    )
    enabled_by_slot = enabled_registry_by_slot(
        settings=settings
    )
    multi_enabled = {
        f"{city_id}|{role}": ids
        for (city_id, role), ids
        in enabled_by_slot.items()
        if len(ids) > 1
    }

    post_audit_commands = regenerate_outputs(
        settings=settings,
        run_dir=run_dir,
        timeout=args.post_audit_timeout_seconds,
    )
    conflict_summary = classify_conflict_report(
        settings=settings
    )

    hard_gates = {
        "enabled_unverified_zero":
            audit_after.get(
                "enabled_unverified_slots"
            ) == 0,
        "verified_enabled_aligned":
            audit_after.get("slots_verified")
            == audit_after.get("slots_enabled"),
        "no_multi_enabled_slots":
            not multi_enabled,
        "no_invalid_verified_probe":
            integrity_after[
                "invalid_verified_candidate_count"
            ] == 0,
        "no_active_cross_slot_conflict":
            conflict_summary["active_conflict_groups"] == 0,
    }
    trusted = all(hard_gates.values())

    report = {
        "run_id": run_id,
        "completed_at": iso_now(),
        "queue_path": str(queue_path),
        "candidate_count": len(candidate_ids),
        "successful_integrity_count":
            len(successful_integrity_ids),
        "failed_probe_count":
            len(failed_probe_ids),
        "failed_probe_ids":
            failed_probe_ids,
        "verification_result":
            verification_result,
        "verified_target_count":
            verified_targets.height,
        "unsafe_verified_not_promoted": [
            {
                "candidate_id": row.get("candidate_id"),
                "slot_id": row.get("slot_id"),
                "candidate_kind": row.get("candidate_kind"),
                "page_type": row.get("page_type"),
                "entry_eligible": row.get("entry_eligible"),
            }
            for row in unsafe_verified_rows
        ],
        "selected_slot_count":
            len(selected),
        "selected_candidate_ids": [
            row["candidate_id"]
            for row in selected
        ],
        "verified_alternate_ids": [
            row["candidate_id"]
            for row in alternates
        ],
        "promoted_count": len(promoted),
        "enabled_count": len(enabled),
        "already_enabled_count": len(already_enabled),
        "promoted": promoted,
        "enabled": enabled,
        "already_enabled": already_enabled,
        "errors": promotion_errors,
        "audit_before": audit_before,
        "audit_after": audit_after,
        "verified_probe_integrity_after":
            integrity_after,
        "multi_enabled_slots":
            multi_enabled,
        "cross_slot_conflicts":
            conflict_summary,
        "post_audit_commands":
            post_audit_commands,
        "hard_gates": hard_gates,
        "trusted_baseline": trusted,
        "full_crawl_started": False,
        "full_crawl_blocked_reason": (
            "This pipeline never starts full crawl. "
            "All 525 slots and an explicit full-crawl "
            "opt-in are still required."
        ),
    }
    atomic_json(report_path, report)

    baseline_path: Path | None = None
    if trusted:
        baseline_path = create_baseline(
            settings=settings,
            run_dir=run_dir,
            report_path=report_path,
        )
        report["stable_baseline"] = str(
            baseline_path
        )
        atomic_json(report_path, report)

    state["status"] = (
        "completed_trusted"
        if trusted
        else "completed_with_gate_failures"
    )
    state["completed_at"] = iso_now()
    state["final_report"] = str(report_path)
    state["hard_gates"] = hard_gates
    state["stable_baseline"] = (
        str(baseline_path)
        if baseline_path
        else None
    )
    atomic_json(state_path, state)

    print()
    print("=" * 78)
    print("TARGETED RECOVERY COMPLETE")
    print(f"Fresh probe OK     : {len(successful_integrity_ids)}")
    print(f"Fresh probe failed : {len(failed_probe_ids)}")
    print(f"Verified targets   : {verified_targets.height}")
    print(f"Selected slots     : {len(selected)}")
    print(f"Promoted           : {len(promoted)}")
    print(f"Enabled            : {len(enabled)}")
    print(f"Already enabled    : {len(already_enabled)}")
    print(f"Errors             : {len(promotion_errors)}")
    print(
        "Verified/enabled  : "
        f"{audit_after.get('slots_verified')}/"
        f"{audit_after.get('slots_enabled')}"
    )
    print(
        "Invalid verified  : "
        f"{integrity_after['invalid_verified_candidate_count']}"
    )
    print(
        "Active conflicts  : "
        f"{conflict_summary['active_conflict_groups']} groups / "
        f"{conflict_summary['active_rows']} rows "
        f"(historical rows: {conflict_summary['historical_rows']}; "
        f"current nonconflicting rows: "
        f"{conflict_summary['active_nonconflicting_rows']})"
    )
    print(f"Trusted baseline   : {trusted}")
    print(f"Report             : {report_path}")
    if baseline_path:
        print(f"Baseline           : {baseline_path}")
    print("=" * 78)

    return 0 if trusted else 2


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Targeted, resumable source candidate "
            "reprobe → verify → promote → enable pipeline."
        )
    )
    parser.add_argument(
        "--queue",
        default=(
            r"D:\Data Set\CRPD\outputs\acceptance"
            r"\real_reprobe_queue"
            r"\quarantined_candidates_real_reprobe.csv"
        ),
    )
    parser.add_argument(
        "--expected-count",
        type=int,
        default=28,
        help="Set 0 to disable the exact-count guard.",
    )
    parser.add_argument(
        "--rounds",
        type=int,
        default=2,
    )
    parser.add_argument(
        "--candidate-timeout-seconds",
        type=int,
        default=300,
    )
    parser.add_argument(
        "--post-audit-timeout-seconds",
        type=int,
        default=600,
    )
    parser.add_argument(
        "--run-id",
        default=None,
    )
    parser.add_argument(
        "--data-root",
        default=None,
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Run real probes and registry changes.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-run candidates already completed in this run state.",
    )
    parser.add_argument(
        "--finalize-only",
        action="store_true",
        help=(
            "Reuse existing per-candidate probe state, "
            "skip network calls, and only verify/audit/finalize."
        ),
    )

    parser.add_argument(
        "--worker",
        action="store_true",
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--candidate-id",
        default=None,
        help=argparse.SUPPRESS,
    )
    return parser


if __name__ == "__main__":
    parsed = build_parser().parse_args()
    if parsed.worker:
        if not parsed.candidate_id:
            raise SystemExit(
                "--worker requires --candidate-id"
            )
        raise SystemExit(
            worker(
                candidate_id=parsed.candidate_id,
                rounds=parsed.rounds,
                data_root=parsed.data_root,
            )
        )
    raise SystemExit(main(parsed))
