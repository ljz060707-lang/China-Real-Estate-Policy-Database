import json
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import policydb.episode_930_v4 as episode_930_v4
from policydb.episode_930_v4 import (
    FINAL_MEMBERSHIP_STATUSES,
    _api_summary,
    classify_membership,
    process_audit,
)


def test_module_has_real_cli_entrypoint():
    source = Path(__file__).parents[1] / "src" / "policydb" / "episode_930_v4.py"
    assert 'if __name__ == "__main__":' in source.read_text(encoding="utf-8")


def test_membership_closure_uses_underlying_date_not_later_reprint_page():
    status, reason, _ = classify_membership(
        {
            "publication_date": "2018-01-02",
            "official_text": "原政策自2016年9月30日起执行。",
        }
    )
    assert status == "CONFIRMED_EP930_REPRINT"
    assert "underlying" in reason


def test_membership_closure_marks_later_policy_outside_episode():
    status, _, dates = classify_membership(
        {
            "publication_date": "2026-08-07",
            "official_text": "本通知自2026年8月7日起施行。",
        }
    )
    assert status == "CONFIRMED_OUTSIDE_EP930"
    assert date(2026, 8, 7) in dates


def test_membership_closure_never_emits_open_or_unknown():
    status, _, _ = classify_membership({"policy_title": "历史政策页面"})
    assert status in FINAL_MEMBERSHIP_STATUSES
    assert status not in {"UNKNOWN", "WINDOW_CONFLICT", "MANUAL_REVIEW_PENDING"}


def test_core_and_extended_windows_are_distinct():
    core, _, _ = classify_membership({"official_text": "2016-10-01发布"})
    extended, _, _ = classify_membership({"official_text": "2016-10-12发布"})
    assert core == "CONFIRMED_EP930_CORE"
    assert extended == "CONFIRMED_EP930_EXTENDED"


def test_api_core_backlog_comes_from_authoritative_monitor_when_state_lacks_it(tmp_path):
    (tmp_path / "930_API_RECOVERY_STATE.json").write_text(
        '{"phase":"BACKOFF_SINGLE_PROBE","schema_valid":true}', encoding="utf-8"
    )
    (tmp_path / "930_API_PROVIDER_STATUS.json").write_text(
        '{"provider":"siliconflow","model":"model","status":"OPERATIONAL"}', encoding="utf-8"
    )
    summary = _api_summary(
        tmp_path,
        {"api_health": {"core_pass1_waiting": 16, "core_pass1_success": 0, "core_pass2_waiting": 0}},
    )
    assert summary["core_pass1_waiting"] == 16
    assert summary["core_pass1_success"] == 0


def test_api_summary_requires_persisted_certification_batches(tmp_path):
    (tmp_path / "930_API_RECOVERY_STATE.json").write_text(
        '{"phase":"MICRO_5","last_success_documents":1,"last_success_rate":1.0,"schema_valid":true}',
        encoding="utf-8",
    )
    (tmp_path / "930_API_PROVIDER_STATUS.json").write_text(
        '{"provider":"siliconflow","model":"model","status":"OPERATIONAL"}',
        encoding="utf-8",
    )
    summary = _api_summary(tmp_path)
    assert summary["certification"] == "BLOCKED_BY_CERTIFICATION_BATCH"
    assert summary["certification_stages"]["MICRO_5"]["passed"] is False


def test_due_retry_relaunches_after_previous_controller_exited(tmp_path, monkeypatch):
    """A dead prior launch must not suppress a due official controller retry."""

    previous_launch_at = (datetime.now(UTC) - timedelta(seconds=60)).isoformat()
    (tmp_path / "EP930_CONVERGENCE_STATUS.json").write_text(
        json.dumps(
            {
                "official_controller": {
                    "last_launch_at": previous_launch_at,
                    "last_launch": {"started": True, "pid": 999999},
                }
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        episode_930_v4,
        "_scope_audit",
        lambda data_root: {
            "scope_hash_unchanged": True,
            "scope_version": "930-analysis-ready-v1",
            "scope_unit": "queue_item",
            "scope_city_count": 20,
            "scope_queue_item_count": 100,
            "scope_hash": episode_930_v4.SCOPE_HASH,
            "frozen": True,
        },
    )
    monkeypatch.setattr(
        episode_930_v4,
        "build_root_document_closure",
        lambda data_root, output: {"membership_counts": {}, "included_documents": 1},
    )
    monkeypatch.setattr(
        episode_930_v4,
        "build_recovery_closure",
        lambda data_root, output: {"items": 0, "status": "PASS"},
    )
    monkeypatch.setattr(
        episode_930_v4,
        "process_audit",
        lambda exclude_pid=None: {
            "writer_capable_processes": [],
            "writer_capable_process_count": 0,
            "writer_chain_count": 0,
            "writer_chain_roots": [],
            "official_controllers": [],
            "official_controller_count": 0,
        },
    )
    monkeypatch.setattr(episode_930_v4, "_lock_audit", lambda data_root, exclude_pid=None: [])
    monkeypatch.setattr(
        episode_930_v4,
        "_queue_metrics",
        lambda monitor, state: {
            "total": 1575,
            "raw_completed": 1575,
            "accounted_total": 1575,
            "consistent": True,
            "accounted_statuses": {"completed": 1575},
        },
    )
    monkeypatch.setattr(
        episode_930_v4,
        "_api_summary",
        lambda output, monitor=None: {
            "provider": "siliconflow",
            "model": "model",
            "provider_status": "OPERATIONAL",
            "phase": "BACKOFF_SINGLE_PROBE",
            "retry_due": True,
            "last_success_rate": 0.5,
            "schema_valid": True,
            "core_pass1_waiting": 1,
            "core_pass1_success": 0,
            "core_pass2_waiting": 0,
            "manual_api_calls": 0,
        },
    )
    launch = {"started": False}

    def fake_start(repo_root, data_root, output):
        launch.update({"started": True, "pid": 12345, "started_at": episode_930_v4._now()})
        return dict(launch)

    monkeypatch.setattr(episode_930_v4, "_start_official_controller", fake_start)

    status = episode_930_v4.run_cycle(tmp_path, tmp_path, tmp_path, own_pid=999)

    assert status["official_controller"]["active_count"] == 1
    assert status["decision"] == "OFFICIAL_CONTROLLER_STARTED"


def test_due_retry_preserves_cooldown_for_live_previous_controller(monkeypatch):
    class LiveController:
        def is_running(self):
            return True

        def cmdline(self):
            return ["python.exe", "-m", "policydb.episode_930_autorun"]

    monkeypatch.setattr(episode_930_v4.psutil, "Process", lambda pid: LiveController())

    assert episode_930_v4._previous_controller_is_live(
        {
            "official_controller": {
                "last_launch": {"started": True, "pid": 12345},
            }
        }
    )


def test_process_audit_counts_controller_interpreter_chain_once(monkeypatch):
    class FakeProcess:
        def __init__(self, pid, parent_pid):
            self.pid = pid
            self.info = {"name": "python.exe"}
            self._parent_pid = parent_pid

        def cmdline(self):
            return ["python.exe", "-m", "policydb.episode_930_autorun"]

        def ppid(self):
            return self._parent_pid

    monkeypatch.setattr(
        episode_930_v4.psutil,
        "process_iter",
        lambda fields: iter([FakeProcess(100, 50), FakeProcess(101, 100)]),
    )

    audit = process_audit()

    assert audit["official_controller_count"] == 1
    assert audit["official_controller_roots"] == [100]
