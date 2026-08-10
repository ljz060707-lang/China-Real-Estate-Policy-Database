from __future__ import annotations

import hashlib
import json
from pathlib import Path

from policydb.source_completion_checkpoint import (
    render_checkpoint_markdown,
    render_next_batch_command,
    write_checkpoint_artifacts,
)


def _state() -> dict:
    return {
        "checkpoint_id": "20260806T050000Z",
        "created_at": "2026-08-06T05:00:00+00:00",
        "branch": "feat/source-recovery",
        "head": "abc123",
        "git_status": [" M src/policydb/example.py"],
        "current_command": "policydb source research --max-slots 5",
        "latest_run_id": "RUN_1",
        "current_status": {
            "status": "GO_BLOCKED",
            "current_step": "go_gate_blocked",
            "ai_calls": 1,
            "ai_attempts": 1,
            "candidates": 3,
            "probes": 2,
            "human_review": 1,
            "verified": 109,
            "enabled": 109,
            "unresolved": 416,
            "tokens": 32,
            "cost": None,
            "current_batch": {"run_id": "RUN_1", "planned_slots": 5, "human_review": 1, "cost": None},
        },
        "slot_audit": {"slots_verified": 109, "slots_enabled": 109, "slots_unresolved": 416},
        "active_source_worker": False,
        "relevant_locks": [],
        "data_write_state": "SAFE_STOPPED_AFTER_ATOMIC_BATCH",
        "full_crawl_started": False,
        "full_ai_started": False,
        "stop_reason": {"code": "SAFE_BOUNDARY"},
    }


def test_checkpoint_markdown_renders_values_instead_of_power_shell_placeholders() -> None:
    rendered = render_checkpoint_markdown(_state())
    assert "$stamp" not in rendered
    assert "$branch" not in rendered
    assert "$head" not in rendered
    assert "$(" not in rendered
    assert "\x08" not in rendered
    assert "estimated cost: null" in rendered
    assert "active source worker: false" in rendered
    assert "relevant locks: 0" in rendered
    assert "branch: feat/source-recovery" in rendered
    assert "HEAD: abc123" in rendered
    assert "current batch: RUN_1" in rendered


def test_checkpoint_artifacts_are_new_and_manifested(tmp_path: Path) -> None:
    output = tmp_path / "checkpoint"
    result = write_checkpoint_artifacts(output, state=_state(), report={"tokens": 32, "cost": None})
    assert result["checkpoint_dir"] == str(output)
    state = json.loads((output / "CURRENT_STATE.json").read_text(encoding="utf-8"))
    assert state["current_status"]["cost"] is None
    assert "null" in (output / "AI_SOURCE_COMPLETION_REPORT.md").read_text(encoding="utf-8")
    manifest = json.loads((output / "checkpoint_manifest.json").read_text(encoding="utf-8"))
    for item in manifest["files"]:
        path = output / item["path"]
        assert hashlib.sha256(path.read_bytes()).hexdigest() == item["sha256"]
    try:
        write_checkpoint_artifacts(output, state=_state())
    except FileExistsError:
        pass
    else:
        raise AssertionError("checkpoint overwrite must be rejected")


def test_next_batch_command_is_bounded_and_does_not_contain_credentials() -> None:
    command = render_next_batch_command(max_slots=5, max_ai_calls=5, concurrency=1)
    assert "--max-slots 5" in command
    assert "--max-ai-calls 5" in command
    assert "--concurrency 1" in command
    assert "api_key" not in command.lower()
    assert "authorization" not in command.lower()
