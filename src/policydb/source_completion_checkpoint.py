"""Crash-safe, deterministic artifacts for source-completion checkpoints.

The source-completion runners write their own run directory.  This module is
the small, reusable boundary used to publish a human-readable checkpoint for a
completed safe boundary.  It intentionally keeps ``None`` as JSON ``null``
and renders it as the literal word ``null`` in Markdown; PowerShell template
variables must never be interpolated by the renderer.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import subprocess
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

_SECRET_KEY_RE = re.compile(
    r"(?:api[_-]?key|authorization|password|secret|credential|access[_-]?token)",
    re.IGNORECASE,
)


def _safe_value(value: Any, *, key: str = "") -> Any:
    """Return a JSON-safe value without copying credentials into artifacts."""

    if _SECRET_KEY_RE.search(key):
        return "[REDACTED]"
    if isinstance(value, Mapping):
        return {str(k): _safe_value(v, key=str(k)) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_safe_value(item, key=key) for item in value]
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


def _display(value: Any) -> str:
    """Render a scalar without PowerShell interpolation or control bytes."""

    if value is None:
        return "null"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (dict, list, tuple)):
        return json.dumps(_safe_value(value), ensure_ascii=False, sort_keys=True, default=str)
    text = str(value)
    text = "".join(char if char in "\n\r\t" or ord(char) >= 32 else " " for char in text)
    return text.replace("$", "\\$")


def _json_text(value: Any) -> str:
    return json.dumps(_safe_value(value), ensure_ascii=False, indent=2, default=str) + "\n"


def _atomic_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(text, encoding="utf-8", newline="\n")
    os.replace(temporary, path)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def git_metadata(repo: Path) -> dict[str, Any]:
    """Read branch, HEAD and status without failing a checkpoint."""

    def run(*args: str) -> str | None:
        try:
            result = subprocess.run(
                ["git", *args],
                cwd=repo,
                check=True,
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
            )
        except (OSError, subprocess.CalledProcessError):
            return None
        return result.stdout.strip()

    status = run("status", "--short")
    return {
        "branch": run("branch", "--show-current"),
        "head": run("rev-parse", "HEAD"),
        "git_status": status.splitlines() if status else [],
    }


def render_checkpoint_markdown(state: Mapping[str, Any]) -> str:
    """Render the stable checkpoint summary used by operators and auditors."""

    current = state.get("current_status") if isinstance(state.get("current_status"), Mapping) else {}
    batch = current.get("current_batch") if isinstance(current.get("current_batch"), Mapping) else {}
    audit = state.get("slot_audit") if isinstance(state.get("slot_audit"), Mapping) else {}
    stop_reason = state.get("stop_reason") if isinstance(state.get("stop_reason"), Mapping) else {}
    lines = [
        "# CRPD source-completion checkpoint",
        "",
        f"- checkpoint_id: {_display(state.get('checkpoint_id'))}",
        f"- created_at: {_display(state.get('created_at'))}",
        f"- branch: {_display(state.get('branch'))}",
        f"- HEAD: {_display(state.get('head'))}",
        f"- current command: {_display(state.get('current_command'))}",
        f"- latest run id: {_display(state.get('latest_run_id'))}",
        f"- latest run directory: {_display(state.get('latest_run_dir'))}",
        f"- current status: {_display(current.get('status', state.get('status')))}",
        f"- current batch: {_display(batch.get('run_id', state.get('latest_run_id')))}",
        f"- current step: {_display(current.get('current_step', batch.get('current_step')))}",
        f"- planned slots: {_display(batch.get('planned_slots'))}",
        f"- AI calls: {_display(current.get('ai_calls', batch.get('ai_calls')))}",
        f"- AI attempts: {_display(current.get('ai_attempts', batch.get('ai_attempts')))}",
        f"- candidates: {_display(current.get('candidates', batch.get('candidates')))}",
        f"- probes: {_display(current.get('probes', batch.get('probes')))}",
        f"- human review: {_display(current.get('human_review', batch.get('human_review')))}",
        f"- verified: {_display(current.get('verified', audit.get('slots_verified')))}",
        f"- enabled: {_display(current.get('enabled', audit.get('slots_enabled')))}",
        f"- unresolved: {_display(current.get('unresolved', audit.get('slots_unresolved')))}",
        f"- tokens: {_display(current.get('tokens', batch.get('tokens')))}",
        f"- estimated cost: {_display(current.get('cost', batch.get('cost')))}",
        f"- usage status: {_display(current.get('usage_status', batch.get('usage_status')))}",
        f"- provider status: {_display(current.get('provider_status', state.get('provider_status')))}",
        f"- API balance status: {_display(current.get('api_balance_status', state.get('api_balance_status')))}",
        f"- active source worker: {_display(state.get('active_source_worker', False))}",
        f"- relevant locks: {_display(len(state.get('relevant_locks') or []))}",
        f"- data write state: {_display(state.get('data_write_state'))}",
        f"- full crawl started: {_display(state.get('full_crawl_started', False))}",
        f"- full AI started: {_display(state.get('full_ai_started', False))}",
        f"- stop reason: {_display(stop_reason.get('code'))}",
        f"- stop detail: {_display(stop_reason)}",
        f"- recovery command: {_display(state.get('recovery_command'))}",
        "",
        "## Git status",
        "",
    ]
    git_status = state.get("git_status") or []
    lines.extend(f"- {_display(item)}" for item in git_status)
    if not git_status:
        lines.append("- clean or unavailable")
    lines.extend(["", "All source admission remains subject to deterministic verification, parser, pagination and independent probe gates.", ""])
    return "\n".join(lines)


def render_report_markdown(report: Mapping[str, Any]) -> str:
    """Render a report without accidental template interpolation."""

    payload = _json_text(report)
    return "# AI source-completion batch report\n\n```json\n" + payload + "```\n\nAI output is discovery evidence only; it cannot write verified or enabled.\n"


def render_next_batch_command(
    *,
    python_path: str = ".venv\\Scripts\\python.exe",
    data_root: str = r"E:\Data Set\CRPD",
    max_slots: int = 20,
    max_ai_calls: int = 20,
    concurrency: int = 2,
) -> str:
    """Return an operator-run PowerShell command with bounded defaults."""

    def quote(value: str) -> str:
        return "'" + str(value).replace("'", "''") + "'"

    return (
        "$ErrorActionPreference = 'Stop'\n"
        f"$env:CRPD_DATA_ROOT = {quote(data_root)}\n"
        f"& {quote(python_path)} -m policydb.autopilot_cli source research "
        f"--max-slots {int(max_slots)} --max-ai-calls {int(max_ai_calls)} "
        f"--concurrency {int(concurrency)} --apply --resume\n"
        "exit $LASTEXITCODE\n"
    )


def write_checkpoint_artifacts(
    checkpoint_dir: Path,
    *,
    state: Mapping[str, Any],
    report: Mapping[str, Any] | None = None,
    next_command: str | None = None,
) -> dict[str, Any]:
    """Atomically publish a new checkpoint directory and its manifest."""

    checkpoint_dir = Path(checkpoint_dir)
    if checkpoint_dir.exists() and any(checkpoint_dir.iterdir()):
        raise FileExistsError(f"checkpoint directory is not empty: {checkpoint_dir}")
    checkpoint_dir.mkdir(parents=True, exist_ok=True)
    safe_state = _safe_value(dict(state))
    safe_report = _safe_value(dict(report or {}))
    _atomic_text(checkpoint_dir / "CURRENT_STATE.json", _json_text(safe_state))
    _atomic_text(checkpoint_dir / "CURRENT_STATE.md", render_checkpoint_markdown(safe_state))
    _atomic_text(checkpoint_dir / "AI_SOURCE_COMPLETION_REPORT.md", render_report_markdown(safe_report))
    _atomic_text(
        checkpoint_dir / "NEXT_AI_BATCH_COMMAND.ps1",
        next_command or render_next_batch_command(),
    )
    files = []
    for path in sorted(checkpoint_dir.iterdir()):
        if path.name == "checkpoint_manifest.json" or not path.is_file():
            continue
        files.append({"path": path.name, "sha256": _sha256(path), "bytes": path.stat().st_size})
    manifest = {
        "checkpoint_id": checkpoint_dir.name,
        "created_at": datetime.now(UTC).isoformat(),
        "immutable": True,
        "files": files,
    }
    _atomic_text(checkpoint_dir / "checkpoint_manifest.json", _json_text(manifest))
    return {"checkpoint_dir": str(checkpoint_dir), "manifest": manifest}


def build_checkpoint_state(
    *,
    run_id: str,
    run_dir: Path,
    current_status: Mapping[str, Any],
    slot_audit: Mapping[str, Any] | None = None,
    current_command: str | None = None,
    repo: Path | None = None,
    checkpoint_id: str | None = None,
    stop_reason: Mapping[str, Any] | None = None,
    recovery_command: str | None = None,
) -> dict[str, Any]:
    """Assemble the state payload consumed by :func:`write_checkpoint_artifacts`."""

    metadata = git_metadata(repo) if repo is not None else {"branch": None, "head": None, "git_status": []}
    current = _safe_value(dict(current_status))
    audit = _safe_value(dict(slot_audit or {}))
    return {
        "checkpoint_id": checkpoint_id or datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ"),
        "created_at": datetime.now(UTC).isoformat(),
        **metadata,
        "current_command": current_command,
        "latest_run_id": run_id,
        "latest_run_dir": str(run_dir),
        "current_status": current,
        "slot_audit": audit,
        "active_source_worker": False,
        "relevant_locks": [],
        "data_write_state": "SAFE_STOPPED_AFTER_ATOMIC_BATCH",
        "stop_reason": dict(stop_reason or {"code": "BATCH_COMPLETED_SAFE_BOUNDARY"}),
        "full_crawl_started": bool(current.get("full_run_started", False)),
        "full_ai_started": False,
        "provider_status": current.get("provider_status"),
        "api_balance_status": current.get("api_balance_status"),
        "recovery_command": recovery_command or "review the latest checkpoint and run NEXT_AI_BATCH_COMMAND.ps1",
    }
