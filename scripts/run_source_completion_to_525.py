from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import shutil
import socket
import subprocess
import sys
import time
from collections import defaultdict
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import polars as pl

TARGET_METRICS: dict[str, Any] = {
    "required_slots": 525,
    "slots_resolved": 525,
    "slots_with_verified_candidate": 525,
    "slots_verified": 525,
    "slots_with_enabled_source": 525,
    "slots_enabled": 525,
    "slots_direct_healthy": 525,
    "slots_parser_ready": 525,
    "enabled_unverified_slots": 0,
    "slots_unresolved": 0,
    "verified_coverage_pct": 100.0,
    "enabled_coverage_pct": 100.0,
}

SUCCESS_BATCH_CODES = {0, 10}
PROVIDER_BLOCKED_CODE = 20
STOP_REQUESTED_CODE = 21
BLOCKED_EXIT_CODE = 30
PREFLIGHT_EXIT_CODE = 31
GATE_FAILURE_EXIT_CODE = 32

HISTORICAL_STATUSES = {
    "rejected_by_gate",
    "quarantined_invalid_probe_evidence",
    "excluded_registry_duplicate_retired",
    "excluded_registry_source_invalid",
    "excluded_cross_slot_role_mismatch",
    "excluded_detail_page_not_reusable",
    "excluded_cross_slot_gazette_domain_role_mismatch",
    "excluded_duplicate_nonpreferred",
}

SHA256_HEX = set("0123456789abcdefABCDEF")


@dataclass
class CommandResult:
    command: list[str]
    return_code: int
    status: str
    elapsed_seconds: float
    stdout: str
    stderr: str
    log_path: str


def utc_now() -> datetime:
    return datetime.now(UTC)


def iso_now() -> str:
    return utc_now().isoformat()


def atomic_write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + ".tmp")
    temp.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )
    os.replace(temp, path)


def read_json(path: Path, default: Any = None) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        return default


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def safe_name(value: str) -> str:
    return "".join(
        char if char.isalnum() or char in "-_." else "_"
        for char in value
    )


def truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {
        "1",
        "true",
        "yes",
        "y",
        "on",
    }


def extract_last_json(text: str) -> dict[str, Any] | None:
    """Return the last top-level JSON object embedded in command output.

    A command can print prose plus one or more JSON documents. Nested objects
    inside a document must not replace the enclosing document.
    """
    decoder = json.JSONDecoder()
    candidates: list[tuple[int, int, dict[str, Any]]] = []

    for index, char in enumerate(text):
        if char != "{":
            continue
        try:
            value, consumed = decoder.raw_decode(text[index:])
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict):
            candidates.append((index, index + consumed, value))

    if not candidates:
        return None

    top_level: list[tuple[int, int, dict[str, Any]]] = []
    for candidate in candidates:
        start, end, _ = candidate
        contained = any(
            outer_start <= start
            and end <= outer_end
            and (outer_start, outer_end) != (start, end)
            for outer_start, outer_end, _ in candidates
        )
        if not contained:
            top_level.append(candidate)

    selected = max(top_level or candidates, key=lambda item: item[0])
    return selected[2]


def run_command(
    command: list[str],
    *,
    cwd: Path,
    env: dict[str, str],
    log_path: Path,
    timeout_seconds: int,
) -> CommandResult:
    started = time.monotonic()
    try:
        completed = subprocess.run(
            command,
            cwd=cwd,
            env=env,
            text=True,
            encoding="utf-8",
            errors="replace",
            capture_output=True,
            timeout=timeout_seconds,
            check=False,
        )
        code = completed.returncode
        stdout = completed.stdout
        stderr = completed.stderr
        status = "ok" if code == 0 else "nonzero"
    except subprocess.TimeoutExpired as exc:
        code = -9
        stdout = exc.stdout or ""
        stderr = (exc.stderr or "") + "\nTIMEOUT"
        status = "timeout"
    except OSError as exc:
        code = -1
        stdout = ""
        stderr = f"{type(exc).__name__}: {exc}"
        status = "error"

    result = CommandResult(
        command=command,
        return_code=code,
        status=status,
        elapsed_seconds=round(time.monotonic() - started, 3),
        stdout=stdout,
        stderr=stderr,
        log_path=str(log_path),
    )
    atomic_write_json(log_path, asdict(result))
    return result


def is_valid_response_hash(value: Any) -> bool:
    text = str(value or "").strip()
    return (
        len(text) == 64
        and all(char in SHA256_HEX for char in text)
        and len(set(text.lower())) >= 8
    )


class FileLock:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.acquired = False

    @staticmethod
    def _pid_alive(pid: int) -> bool:
        try:
            os.kill(pid, 0)
            return True
        except OSError:
            return False

    def acquire(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if self.path.exists():
            current = read_json(self.path, {}) or {}
            pid = int(current.get("pid") or 0)
            if pid and self._pid_alive(pid):
                raise RuntimeError(
                    f"Source-completion controller is already running: "
                    f"pid={pid}, lock={self.path}"
                )
            stale = self.path.with_name(
                self.path.name + f".stale_{datetime.now():%Y%m%d_%H%M%S}"
            )
            os.replace(self.path, stale)

        fd = os.open(
            self.path,
            os.O_CREAT | os.O_EXCL | os.O_WRONLY,
        )
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            json.dump(
                {
                    "pid": os.getpid(),
                    "host": socket.gethostname(),
                    "created_at": iso_now(),
                },
                stream,
                ensure_ascii=False,
                indent=2,
            )
        self.acquired = True

    def release(self) -> None:
        if self.acquired:
            try:
                self.path.unlink()
            except FileNotFoundError:
                pass
            self.acquired = False

    def __enter__(self) -> FileLock:
        self.acquire()
        return self

    def __exit__(self, *_: Any) -> None:
        self.release()


class CompletionController:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.repo = Path(args.repo).resolve()
        self.python = self.repo / ".venv" / "Scripts" / "python.exe"
        self.policydb = self.repo / ".venv" / "Scripts" / "policydb.exe"
        self.data_root = Path(args.data_root).resolve()
        self.output_root = (
            self.data_root
            / "outputs"
            / "acceptance"
            / "source_completion_to_525"
        )
        self.control_root = self.data_root / "control"
        self.stop_file = self.control_root / "STOP_SOURCE_COMPLETION_TO_525"
        self.lock_file = self.control_root / "SOURCE_COMPLETION_TO_525.lock"
        self.env = os.environ.copy()
        self.env.update(
            {
                "CRPD_DATA_ROOT": str(self.data_root),
                "PYTHONUTF8": "1",
                "PYTHONIOENCODING": "utf-8",
            }
        )
        self.run_id, self.run_dir, self.state = self._load_or_create_run()
        self.state_path = self.run_dir / "controller_state.json"
        self.summary_path = self.run_dir / "controller_summary.json"
        self.logs_dir = self.run_dir / "logs"
        self.cycles_dir = self.run_dir / "cycles"
        self.logs_dir.mkdir(parents=True, exist_ok=True)
        self.cycles_dir.mkdir(parents=True, exist_ok=True)

    def _load_or_create_run(self) -> tuple[str, Path, dict[str, Any]]:
        self.output_root.mkdir(parents=True, exist_ok=True)
        latest_path = self.output_root / "LATEST.json"

        if self.args.run_id:
            run_id = self.args.run_id
        elif not self.args.new_run:
            latest = read_json(latest_path, {}) or {}
            candidate = str(latest.get("run_id") or "")
            candidate_state = (
                self.output_root
                / candidate
                / "controller_state.json"
            )
            existing = read_json(candidate_state, {}) if candidate else {}
            if existing and existing.get("status") not in {
                "COMPLETED_525",
                "BLOCKED",
                "FAILED",
                "STOPPED",
            }:
                run_id = candidate
            else:
                run_id = "SOURCE525_" + utc_now().strftime("%Y%m%dT%H%M%SZ")
        else:
            run_id = "SOURCE525_" + utc_now().strftime("%Y%m%dT%H%M%SZ")

        run_dir = self.output_root / run_id
        run_dir.mkdir(parents=True, exist_ok=True)
        state_path = run_dir / "controller_state.json"
        state = read_json(state_path, {}) or {}
        if not state:
            state = {
                "run_id": run_id,
                "created_at": iso_now(),
                "status": "PLANNED",
                "cycle": 0,
                "stagnant_cycles": 0,
                "provider_block_count": 0,
                "history": [],
                "settings": {
                    "provider": self.args.provider,
                    "batch_slots": self.args.batch_slots,
                    "max_ai_calls": self.args.max_ai_calls,
                    "concurrency": self.args.concurrency,
                    "max_cycles": self.args.max_cycles,
                    "max_stagnant_cycles": self.args.max_stagnant_cycles,
                },
            }
            atomic_write_json(state_path, state)

        atomic_write_json(
            latest_path,
            {
                "run_id": run_id,
                "run_dir": str(run_dir),
                "updated_at": iso_now(),
            },
        )
        return run_id, run_dir, state

    def save_state(self) -> None:
        self.state["updated_at"] = iso_now()
        atomic_write_json(self.state_path, self.state)
        atomic_write_json(
            self.output_root / "LATEST.json",
            {
                "run_id": self.run_id,
                "run_dir": str(self.run_dir),
                "status": self.state.get("status"),
                "cycle": self.state.get("cycle"),
                "updated_at": iso_now(),
            },
        )

    def preflight(self) -> None:
        failures: list[str] = []
        for path, label in [
            (self.repo, "repository"),
            (self.python, "venv python"),
            (self.policydb, "policydb CLI"),
        ]:
            if not path.exists():
                failures.append(f"missing {label}: {path}")

        if failures:
            raise RuntimeError("; ".join(failures))

        help_result = run_command(
            [
                str(self.python),
                "-m",
                "policydb.autopilot_cli",
                "run",
                "--help",
            ],
            cwd=self.repo,
            env=self.env,
            log_path=self.logs_dir / "preflight_autopilot_help.json",
            timeout_seconds=120,
        )
        help_text = help_result.stdout + help_result.stderr
        required_options = {
            "--mode",
            "--run-id",
            "--provider",
            "--max-slots",
            "--max-ai-calls",
            "--concurrency",
            "--apply",
            "--resume",
        }
        missing_options = sorted(
            option for option in required_options if option not in help_text
        )
        if help_result.return_code != 0 or missing_options:
            raise RuntimeError(
                "autopilot CLI preflight failed; missing options: "
                + ", ".join(missing_options)
            )

        audit = self.audit_525("preflight")
        self.state["initial_audit"] = audit
        self.state["status"] = "READY"
        self.save_state()

    def check_stop(self) -> None:
        if self.stop_file.exists():
            self.state["status"] = "STOPPED"
            self.state["stop_reason"] = str(self.stop_file)
            self.save_state()
            raise KeyboardInterrupt(
                f"Stop file detected: {self.stop_file}"
            )

    def audit_525(self, label: str) -> dict[str, Any]:
        result = run_command(
            [
                str(self.policydb),
                "sources",
                "audit-525",
                "--no-seed-registry",
            ],
            cwd=self.repo,
            env=self.env,
            log_path=self.logs_dir / f"audit_525_{safe_name(label)}.json",
            timeout_seconds=self.args.audit_timeout_seconds,
        )
        payload = extract_last_json(result.stdout)
        if result.return_code != 0 or payload is None:
            raise RuntimeError(
                "audit-525 failed: "
                f"return_code={result.return_code}, log={result.log_path}"
            )
        return payload

    def _project_imports(self) -> tuple[Any, Any, Any, Any]:
        try:
            from policydb.crawl.registry import load_registry
            from policydb.settings import Settings
            from policydb.source_discovery import REQUIRED_ROLES
            from policydb.source_slots import list_candidates
        except ImportError as exc:
            raise RuntimeError(
                "Cannot import installed policydb project modules. "
                "Run this script with the repository .venv Python."
            ) from exc
        return load_registry, Settings, REQUIRED_ROLES, list_candidates

    def strict_integrity(self) -> dict[str, Any]:
        load_registry, Settings, REQUIRED_ROLES, list_candidates = (
            self._project_imports()
        )
        settings = Settings.discover()
        candidates = list_candidates(settings=settings)

        if (
            candidates.height
            and "manual_review_status" in candidates.columns
        ):
            candidates = candidates.filter(
                ~pl.col("manual_review_status")
                .fill_null("")
                .cast(pl.String)
                .str.starts_with("excluded_")
            )

        verified_rows = [
            row
            for row in candidates.to_dicts()
            if truthy(row.get("is_verified"))
        ]
        invalid_verified: list[dict[str, Any]] = []
        for row in verified_rows:
            evidence = row.get("probe_evidence_json")
            if isinstance(evidence, str):
                try:
                    evidence = json.loads(evidence)
                except json.JSONDecodeError:
                    evidence = []
            if not isinstance(evidence, list):
                evidence = []
            valid_hashes = [
                item.get("response_sha256")
                for item in evidence
                if isinstance(item, dict)
                and is_valid_response_hash(item.get("response_sha256"))
            ]
            if not valid_hashes:
                invalid_verified.append(
                    {
                        "candidate_id": row.get("candidate_id"),
                        "slot_id": row.get("slot_id"),
                        "candidate_url": row.get("candidate_url"),
                    }
                )

        enabled_by_slot: dict[tuple[str, str], list[str]] = defaultdict(list)
        for source in load_registry(settings):
            if not truthy(getattr(source, "crawl_enabled", False)):
                continue
            agency = str(getattr(source, "agency_type", "") or "")
            role = str(getattr(source, "source_role", "") or "")
            resolved_role = (
                agency
                if agency in REQUIRED_ROLES
                else role if role in REQUIRED_ROLES else ""
            )
            if not resolved_role:
                continue
            for city_id in getattr(source, "city_ids", []) or []:
                enabled_by_slot[(str(city_id), resolved_role)].append(
                    str(source.source_id)
                )

        multi_enabled = {
            f"{city_id}|{role}": source_ids
            for (city_id, role), source_ids in enabled_by_slot.items()
            if len(source_ids) > 1
        }

        active_by_url: dict[str, list[dict[str, Any]]] = defaultdict(list)
        historical_rows = 0
        current_nonconflicting_rows = 0
        for row in candidates.to_dicts():
            canonical = str(row.get("canonical_url") or "").strip()
            if not canonical:
                continue
            status = str(row.get("manual_review_status") or "").strip()
            is_historical = (
                status in HISTORICAL_STATUSES
                or status.startswith("excluded_")
                or status.startswith("quarantined_")
            ) and not truthy(row.get("is_verified"))
            if is_historical:
                historical_rows += 1
                continue
            active_by_url[canonical].append(row)

        active_conflicts: dict[str, list[dict[str, Any]]] = {}
        for canonical, rows in active_by_url.items():
            unique_slots = {
                (
                    str(row.get("slot_id") or ""),
                    str(row.get("city_id") or ""),
                    str(row.get("source_role") or ""),
                )
                for row in rows
            }
            if len(unique_slots) >= 2:
                active_conflicts[canonical] = [
                    {
                        "candidate_id": row.get("candidate_id"),
                        "slot_id": row.get("slot_id"),
                        "city_id": row.get("city_id"),
                        "source_role": row.get("source_role"),
                        "candidate_url": row.get("candidate_url"),
                        "is_verified": row.get("is_verified"),
                        "manual_review_status": row.get(
                            "manual_review_status"
                        ),
                    }
                    for row in rows
                ]
            else:
                current_nonconflicting_rows += len(rows)

        return {
            "verified_candidate_count": len(verified_rows),
            "invalid_verified_candidate_count": len(invalid_verified),
            "invalid_verified_candidates": invalid_verified,
            "multi_enabled_slot_count": len(multi_enabled),
            "multi_enabled_slots": multi_enabled,
            "active_cross_slot_conflict_group_count": len(active_conflicts),
            "active_cross_slot_conflicts": active_conflicts,
            "historical_candidate_rows": historical_rows,
            "current_nonconflicting_rows": current_nonconflicting_rows,
        }

    def hard_gates(
        self,
        audit: dict[str, Any],
        integrity: dict[str, Any],
    ) -> dict[str, bool]:
        return {
            "required_slots_525": audit.get("required_slots") == 525,
            "verified_enabled_aligned": (
                audit.get("slots_verified") == audit.get("slots_enabled")
            ),
            "direct_healthy_aligned": (
                audit.get("slots_direct_healthy")
                == audit.get("slots_enabled")
            ),
            "parser_ready_aligned": (
                audit.get("slots_parser_ready")
                == audit.get("slots_enabled")
            ),
            "enabled_unverified_zero": (
                audit.get("enabled_unverified_slots") == 0
            ),
            "invalid_verified_probe_zero": (
                integrity.get("invalid_verified_candidate_count") == 0
            ),
            "multi_enabled_zero": (
                integrity.get("multi_enabled_slot_count") == 0
            ),
            "active_cross_slot_conflict_zero": (
                integrity.get(
                    "active_cross_slot_conflict_group_count"
                )
                == 0
            ),
        }

    @staticmethod
    def target_reached(audit: dict[str, Any]) -> bool:
        for key, expected in TARGET_METRICS.items():
            observed = audit.get(key)
            if isinstance(expected, float):
                try:
                    if abs(float(observed) - expected) > 1e-9:
                        return False
                except (TypeError, ValueError):
                    return False
            elif observed != expected:
                return False
        return True

    def autopilot_command(
        self,
        *,
        cycle_run_id: str,
        slots: int,
        resume: bool,
    ) -> list[str]:
        command = [
            str(self.python),
            "-m",
            "policydb.autopilot_cli",
            "run",
            "--mode",
            "source-to-full",
            "--run-id",
            cycle_run_id,
            "--provider",
            self.args.provider,
            "--max-slots",
            str(slots),
            "--max-ai-calls",
            str(min(self.args.max_ai_calls, slots)),
            "--concurrency",
            str(self.args.concurrency),
        ]
        if self.args.apply:
            command.append("--apply")
        else:
            command.append("--dry-run")
        if resume:
            command.append("--resume")
        return command

    def run_autopilot_batch(
        self,
        *,
        cycle: int,
        unresolved: int,
    ) -> tuple[CommandResult, dict[str, Any]]:
        slots = max(1, min(self.args.batch_slots, unresolved))
        cycle_run_id = f"{self.run_id}_C{cycle:04d}"
        cycle_dir = self.cycles_dir / f"cycle_{cycle:04d}"
        cycle_dir.mkdir(parents=True, exist_ok=True)

        command = self.autopilot_command(
            cycle_run_id=cycle_run_id,
            slots=slots,
            resume=False,
        )
        result = run_command(
            command,
            cwd=self.repo,
            env=self.env,
            log_path=cycle_dir / "autopilot_run.json",
            timeout_seconds=self.args.batch_timeout_seconds,
        )
        payload = extract_last_json(result.stdout) or {}

        if result.return_code not in SUCCESS_BATCH_CODES:
            resume_command = self.autopilot_command(
                cycle_run_id=cycle_run_id,
                slots=slots,
                resume=True,
            )
            resume_result = run_command(
                resume_command,
                cwd=self.repo,
                env=self.env,
                log_path=cycle_dir / "autopilot_resume.json",
                timeout_seconds=self.args.batch_timeout_seconds,
            )
            if resume_result.return_code in SUCCESS_BATCH_CODES:
                return resume_result, extract_last_json(
                    resume_result.stdout
                ) or {}
            return resume_result, extract_last_json(
                resume_result.stdout
            ) or payload

        return result, payload

    def repair_latest_cycle(self, cycle: int) -> dict[str, Any]:
        cycle_run_id = f"{self.run_id}_C{cycle:04d}"
        autopilot_run_dir = (
            self.data_root / "outputs" / "autopilot" / cycle_run_id
        )
        if not autopilot_run_dir.exists():
            return {
                "attempted": False,
                "reason": f"run_dir_missing:{autopilot_run_dir}",
            }

        cycle_dir = self.cycles_dir / f"cycle_{cycle:04d}"
        dry = run_command(
            [
                str(self.python),
                "-m",
                "policydb.autopilot_cli",
                "repair-checkpoints",
                "--run-dir",
                str(autopilot_run_dir),
            ],
            cwd=self.repo,
            env=self.env,
            log_path=cycle_dir / "repair_checkpoints_dry_run.json",
            timeout_seconds=self.args.audit_timeout_seconds,
        )
        dry_payload = extract_last_json(dry.stdout) or {}
        if dry.return_code != 0 or int(dry_payload.get("proposed") or 0) <= 0:
            return {
                "attempted": True,
                "applied": False,
                "dry_run": dry_payload,
            }

        applied = run_command(
            [
                str(self.python),
                "-m",
                "policydb.autopilot_cli",
                "repair-checkpoints",
                "--run-dir",
                str(autopilot_run_dir),
                "--apply",
            ],
            cwd=self.repo,
            env=self.env,
            log_path=cycle_dir / "repair_checkpoints_apply.json",
            timeout_seconds=self.args.audit_timeout_seconds,
        )
        return {
            "attempted": True,
            "applied": applied.return_code == 0,
            "dry_run": dry_payload,
            "apply": extract_last_json(applied.stdout) or {},
        }

    def run_reporting_scripts(self, label: str) -> list[dict[str, Any]]:
        scripts = [
            "audit_post_dedupe_conflicts.py",
            "audit_verified_probe_integrity.py",
            "build_source_525_action_queue.py",
            "build_department_entry_review.py",
            "build_department_entry_slot_shortlist.py",
            "export_source_completion_review.py",
        ]
        results: list[dict[str, Any]] = []
        report_dir = self.logs_dir / f"reporting_{safe_name(label)}"
        report_dir.mkdir(parents=True, exist_ok=True)
        for index, name in enumerate(scripts, start=1):
            path = self.repo / "scripts" / name
            if not path.exists():
                continue
            result = run_command(
                [str(self.python), str(path)],
                cwd=self.repo,
                env=self.env,
                log_path=report_dir / f"{index:02d}_{safe_name(name)}.json",
                timeout_seconds=self.args.reporting_timeout_seconds,
            )
            results.append(asdict(result))
        return results

    def blocker_report(
        self,
        *,
        audit: dict[str, Any],
        integrity: dict[str, Any],
        reason: str,
    ) -> Path:
        action_queue = (
            self.data_root
            / "outputs"
            / "acceptance"
            / "source_525_action_queue.csv"
        )
        counts: dict[str, int] = defaultdict(int)
        samples: list[dict[str, Any]] = []
        if action_queue.exists():
            with action_queue.open(
                "r", encoding="utf-8-sig", newline=""
            ) as stream:
                rows = list(csv.DictReader(stream))
            for row in rows:
                key = str(row.get("coverage_status") or "unknown")
                counts[key] += 1
            samples = rows[:100]

        payload = {
            "created_at": iso_now(),
            "reason": reason,
            "audit": audit,
            "integrity": integrity,
            "action_queue_counts": dict(counts),
            "action_queue_samples": samples,
            "stop_conditions": {
                "no_gate_bypass": True,
                "no_global_promote_or_enable": True,
                "full_crawl_started": False,
                "manual_or_external_blockers_may_remain": True,
            },
        }
        path = self.run_dir / "BLOCKERS.json"
        atomic_write_json(path, payload)
        return path

    def final_tests(self) -> dict[str, Any]:
        if self.args.skip_final_tests:
            return {
                "skipped": True,
                "all_passed": False,
                "reason": "--skip-final-tests",
            }

        test_dir = self.run_dir / "final_tests"
        test_dir.mkdir(parents=True, exist_ok=True)
        commands = [
            [str(self.python), "-m", "compileall", "-q", "src", "tests"],
            [str(self.python), "-m", "ruff", "check", "src", "tests"],
            [str(self.python), "-m", "pytest", "-q"],
            ["git", "diff", "--check"],
        ]
        results = []
        all_passed = True
        for index, command in enumerate(commands, start=1):
            result = run_command(
                command,
                cwd=self.repo,
                env=self.env,
                log_path=test_dir / f"{index:02d}_{safe_name(command[-1])}.json",
                timeout_seconds=self.args.final_test_timeout_seconds,
            )
            results.append(asdict(result))
            all_passed = all_passed and result.return_code == 0
        return {
            "skipped": False,
            "all_passed": all_passed,
            "results": results,
        }

    def create_final_snapshot(
        self,
        *,
        audit: dict[str, Any],
        integrity: dict[str, Any],
        tests: dict[str, Any],
    ) -> Path:
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        snapshot = (
            self.data_root
            / "outputs"
            / "acceptance"
            / f"stable_baseline_525_{stamp}"
        )
        snapshot.mkdir(parents=True, exist_ok=False)

        sources = {
            "source_candidates.parquet": (
                self.data_root / "curated" / "source_candidates.parquet"
            ),
            "source_requirement_slots.parquet": (
                self.data_root
                / "curated"
                / "source_requirement_slots.parquet"
            ),
            "source_registry.parquet": (
                self.data_root / "curated" / "source_registry.parquet"
            ),
            "source_registry.yaml": (
                self.repo / "data" / "reference" / "source_registry.yaml"
            ),
            "source_525_audit.csv": (
                self.data_root
                / "outputs"
                / "acceptance"
                / "source_525_audit.csv"
            ),
            "source_525_action_queue.csv": (
                self.data_root
                / "outputs"
                / "acceptance"
                / "source_525_action_queue.csv"
            ),
            "controller_state.json": self.state_path,
        }
        manifest: dict[str, Any] = {
            "created_at": iso_now(),
            "run_id": self.run_id,
            "audit": audit,
            "integrity": integrity,
            "tests": tests,
            "files": [],
        }
        for name, source in sources.items():
            if not source.exists():
                continue
            destination = snapshot / name
            shutil.copy2(source, destination)
            manifest["files"].append(
                {
                    "name": name,
                    "source": str(source),
                    "size_bytes": destination.stat().st_size,
                    "sha256": sha256_file(destination),
                }
            )
        atomic_write_json(snapshot / "baseline_manifest.json", manifest)
        return snapshot

    def print_status(
        self,
        *,
        audit: dict[str, Any],
        integrity: dict[str, Any],
        gates: dict[str, bool],
    ) -> None:
        print("=" * 78)
        print(f"Controller run       : {self.run_id}")
        print(f"Controller cycle     : {self.state.get('cycle', 0)}")
        print(f"Status               : {self.state.get('status')}")
        print(
            "Resolved/verified     : "
            f"{audit.get('slots_resolved')}/"
            f"{audit.get('slots_verified')}"
        )
        print(
            "Enabled/direct/parser: "
            f"{audit.get('slots_enabled')}/"
            f"{audit.get('slots_direct_healthy')}/"
            f"{audit.get('slots_parser_ready')}"
        )
        print(f"Unresolved           : {audit.get('slots_unresolved')}")
        print(
            "Coverage             : "
            f"{audit.get('verified_coverage_pct')}% / "
            f"{audit.get('enabled_coverage_pct')}%"
        )
        print(
            "Integrity            : "
            f"invalid_verified={integrity.get('invalid_verified_candidate_count')}, "
            f"multi_enabled={integrity.get('multi_enabled_slot_count')}, "
            "active_conflicts="
            f"{integrity.get('active_cross_slot_conflict_group_count')}"
        )
        print(
            "Hard gates           : "
            + ("PASS" if all(gates.values()) else "FAIL")
        )
        print(f"Run directory        : {self.run_dir}")
        print("=" * 78)

    def run(self) -> int:
        self.preflight()
        audit = self.state.get("initial_audit") or self.audit_525("initial")
        integrity = self.strict_integrity()
        gates = self.hard_gates(audit, integrity)
        self.print_status(audit=audit, integrity=integrity, gates=gates)

        if not all(gates.values()):
            path = self.blocker_report(
                audit=audit,
                integrity=integrity,
                reason="initial_hard_gate_failure",
            )
            self.state["status"] = "FAILED"
            self.state["blocker_report"] = str(path)
            self.save_state()
            return GATE_FAILURE_EXIT_CODE

        if self.target_reached(audit):
            return self.finish(audit, integrity)

        if not self.args.apply:
            self.state["status"] = "PLAN_COMPLETE"
            self.save_state()
            print("Plan mode only. Re-run with --apply for writes and paid calls.")
            return 0

        self.state["status"] = "RUNNING"
        self.save_state()
        previous_verified = int(audit.get("slots_verified") or 0)

        while True:
            self.check_stop()
            cycle = int(self.state.get("cycle") or 0) + 1
            if self.args.max_cycles > 0 and cycle > self.args.max_cycles:
                path = self.blocker_report(
                    audit=audit,
                    integrity=integrity,
                    reason="max_cycles_reached",
                )
                self.state["status"] = "BLOCKED"
                self.state["blocker_report"] = str(path)
                self.save_state()
                return BLOCKED_EXIT_CODE

            unresolved = int(audit.get("slots_unresolved") or 0)
            self.state["cycle"] = cycle
            self.state["current_step"] = "AUTOPILOT_BATCH"
            self.save_state()

            print()
            print("#" * 78)
            print(
                f"Cycle {cycle}: unresolved={unresolved}, "
                f"verified={previous_verified}"
            )
            print("#" * 78)

            batch_result, batch_payload = self.run_autopilot_batch(
                cycle=cycle,
                unresolved=unresolved,
            )

            if batch_result.return_code == STOP_REQUESTED_CODE:
                self.state["status"] = "STOPPED"
                self.state["last_batch"] = asdict(batch_result)
                self.save_state()
                return STOP_REQUESTED_CODE

            if batch_result.return_code == PROVIDER_BLOCKED_CODE:
                blocks = int(self.state.get("provider_block_count") or 0) + 1
                self.state["provider_block_count"] = blocks
                self.state["last_batch"] = asdict(batch_result)
                self.save_state()
                if blocks > self.args.max_provider_retries:
                    path = self.blocker_report(
                        audit=audit,
                        integrity=integrity,
                        reason="provider_block_retry_limit",
                    )
                    self.state["status"] = "BLOCKED"
                    self.state["blocker_report"] = str(path)
                    self.save_state()
                    return PROVIDER_BLOCKED_CODE
                wait = min(
                    self.args.provider_retry_seconds * (2 ** (blocks - 1)),
                    self.args.max_provider_retry_seconds,
                )
                print(f"Provider blocked. Retrying after {wait}s.")
                time.sleep(wait)
                continue

            if batch_result.return_code not in SUCCESS_BATCH_CODES:
                path = self.blocker_report(
                    audit=audit,
                    integrity=integrity,
                    reason=(
                        "autopilot_batch_failed:"
                        f"{batch_result.return_code}"
                    ),
                )
                self.state["status"] = "FAILED"
                self.state["blocker_report"] = str(path)
                self.state["last_batch"] = asdict(batch_result)
                self.save_state()
                return batch_result.return_code or 1

            self.state["provider_block_count"] = 0
            self.state["current_step"] = "AUDIT"
            self.save_state()

            audit_after = self.audit_525(f"cycle_{cycle:04d}")
            integrity_after = self.strict_integrity()
            gates_after = self.hard_gates(audit_after, integrity_after)
            self.run_reporting_scripts(f"cycle_{cycle:04d}")

            current_verified = int(audit_after.get("slots_verified") or 0)
            delta = current_verified - previous_verified
            if delta > 0:
                stagnant = 0
            else:
                stagnant = int(self.state.get("stagnant_cycles") or 0) + 1
                repair = self.repair_latest_cycle(cycle)
                self.state["last_repair"] = repair

            history_item = {
                "cycle": cycle,
                "completed_at": iso_now(),
                "autopilot_run_id": f"{self.run_id}_C{cycle:04d}",
                "batch_return_code": batch_result.return_code,
                "batch_payload": batch_payload,
                "verified_before": previous_verified,
                "verified_after": current_verified,
                "verified_delta": delta,
                "audit": audit_after,
                "integrity": integrity_after,
                "hard_gates": gates_after,
            }
            self.state.setdefault("history", []).append(history_item)
            self.state["last_audit"] = audit_after
            self.state["last_integrity"] = integrity_after
            self.state["last_hard_gates"] = gates_after
            self.state["stagnant_cycles"] = stagnant
            self.save_state()
            self.print_status(
                audit=audit_after,
                integrity=integrity_after,
                gates=gates_after,
            )

            if not all(gates_after.values()):
                path = self.blocker_report(
                    audit=audit_after,
                    integrity=integrity_after,
                    reason="post_batch_hard_gate_failure",
                )
                self.state["status"] = "FAILED"
                self.state["blocker_report"] = str(path)
                self.save_state()
                return GATE_FAILURE_EXIT_CODE

            if self.target_reached(audit_after):
                return self.finish(audit_after, integrity_after)

            if stagnant >= self.args.max_stagnant_cycles:
                path = self.blocker_report(
                    audit=audit_after,
                    integrity=integrity_after,
                    reason=(
                        "stagnation_limit_reached:"
                        f"{stagnant}"
                    ),
                )
                self.state["status"] = "BLOCKED"
                self.state["blocker_report"] = str(path)
                self.save_state()
                print(
                    "Automation reached a deterministic/manual/external "
                    f"blocker. Report: {path}"
                )
                return BLOCKED_EXIT_CODE

            previous_verified = current_verified
            audit = audit_after
            integrity = integrity_after
            time.sleep(self.args.sleep_seconds)

    def finish(
        self,
        audit: dict[str, Any],
        integrity: dict[str, Any],
    ) -> int:
        gates = self.hard_gates(audit, integrity)
        if not self.target_reached(audit) or not all(gates.values()):
            raise RuntimeError("finish() called before all 525 gates passed")

        self.state["status"] = "FINAL_TESTS"
        self.save_state()
        reporting = self.run_reporting_scripts("final_525")
        tests = self.final_tests()
        if not tests.get("all_passed"):
            path = self.blocker_report(
                audit=audit,
                integrity=integrity,
                reason="final_tests_failed_or_skipped",
            )
            self.state["status"] = "FAILED"
            self.state["blocker_report"] = str(path)
            self.state["final_tests"] = tests
            self.save_state()
            return GATE_FAILURE_EXIT_CODE

        snapshot = self.create_final_snapshot(
            audit=audit,
            integrity=integrity,
            tests=tests,
        )
        summary = {
            "status": "COMPLETED_525",
            "completed_at": iso_now(),
            "run_id": self.run_id,
            "audit": audit,
            "integrity": integrity,
            "hard_gates": gates,
            "final_tests": tests,
            "reporting": reporting,
            "stable_baseline": str(snapshot),
            "full_crawl_started": False,
            "note": (
                "All 525 source slots passed strict source gates. "
                "This controller intentionally does not start full crawl."
            ),
        }
        atomic_write_json(self.summary_path, summary)
        self.state["status"] = "COMPLETED_525"
        self.state["completed_at"] = iso_now()
        self.state["stable_baseline"] = str(snapshot)
        self.state["summary"] = str(self.summary_path)
        self.save_state()

        print()
        print("=" * 78)
        print("SOURCE COMPLETION 525/525 ACHIEVED")
        print(json.dumps(audit, ensure_ascii=False, indent=2))
        print(f"Stable baseline: {snapshot}")
        print(f"Summary        : {self.summary_path}")
        print("No full crawl was started.")
        print("=" * 78)
        return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Bounded, resumable CRPD source-completion controller. "
            "It repeatedly runs the existing strict Autopilot until all "
            "525 source slots pass, or stops with a machine-readable "
            "blocker report."
        )
    )
    parser.add_argument(
        "--repo",
        default=(
            r"D:\Codex\projects\Documents-Codex\2026-07-13"
            r"\text-20260705-xlsx-text-data-raw\policy-database"
        ),
    )
    parser.add_argument(
        "--data-root",
        default=r"E:\Data Set\CRPD",
    )
    parser.add_argument("--provider", default="siliconflow")
    parser.add_argument("--batch-slots", type=int, default=20)
    parser.add_argument("--max-ai-calls", type=int, default=20)
    parser.add_argument("--concurrency", type=int, default=4)
    parser.add_argument(
        "--max-cycles",
        type=int,
        default=200,
        help="0 means unlimited cycles; 200 is a high but bounded default.",
    )
    parser.add_argument(
        "--max-stagnant-cycles",
        type=int,
        default=6,
        help=(
            "Stop with BLOCKERS.json after this many successful batches "
            "without any increase in verified slots."
        ),
    )
    parser.add_argument("--max-provider-retries", type=int, default=6)
    parser.add_argument("--provider-retry-seconds", type=int, default=300)
    parser.add_argument(
        "--max-provider-retry-seconds",
        type=int,
        default=3600,
    )
    parser.add_argument("--sleep-seconds", type=int, default=20)
    parser.add_argument("--batch-timeout-seconds", type=int, default=10800)
    parser.add_argument("--audit-timeout-seconds", type=int, default=600)
    parser.add_argument("--reporting-timeout-seconds", type=int, default=1200)
    parser.add_argument("--final-test-timeout-seconds", type=int, default=3600)
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--new-run", action="store_true")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument(
        "--skip-final-tests",
        action="store_true",
        help=(
            "Not compatible with final completion: the controller will "
            "refuse to create a 525 baseline when final tests are skipped."
        ),
    )
    parser.add_argument(
        "--status-only",
        action="store_true",
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    controller = CompletionController(args)

    if args.status_only:
        audit = controller.audit_525("status_only")
        integrity = controller.strict_integrity()
        gates = controller.hard_gates(audit, integrity)
        controller.print_status(
            audit=audit,
            integrity=integrity,
            gates=gates,
        )
        return 0

    try:
        with FileLock(controller.lock_file):
            return controller.run()
    except KeyboardInterrupt as exc:
        print(str(exc), file=sys.stderr)
        return STOP_REQUESTED_CODE
    except Exception as exc:
        controller.state["status"] = "FAILED"
        controller.state["fatal_error"] = {
            "type": type(exc).__name__,
            "message": str(exc),
            "at": iso_now(),
        }
        controller.save_state()
        print(f"FATAL: {type(exc).__name__}: {exc}", file=sys.stderr)
        return PREFLIGHT_EXIT_CODE


if __name__ == "__main__":
    raise SystemExit(main())
