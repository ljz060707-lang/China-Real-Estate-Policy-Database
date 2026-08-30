"""Render the read-only 930 operational snapshot for PowerShell wrappers."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

from policydb.episode_930_monitor import build_monitor_snapshot  # noqa: E402


def _bar(percent: Any, width: int = 28) -> str:
    if percent is None:
        return "[calibrating]"
    value = max(0.0, min(100.0, float(percent)))
    filled = int(round(width * value / 100.0))
    return f"[{'#' * filled}{'-' * (width - filled)}] {value:5.1f}%"


def _eta(value: Any) -> str:
    if not value:
        return "CALIBRATING"
    if value in {"BLOCKED_PROVIDER", "BLOCKED_BY_API"}:
        return value
    if value in {"COMPLETE", "CALIBRATING"}:
        return value
    try:
        return datetime.fromisoformat(str(value)).astimezone().strftime("%Y-%m-%d %H:%M")
    except ValueError:
        return str(value)


def _process_info() -> dict[str, Any]:
    script = (
        "$p=Get-CimInstance Win32_Process -ErrorAction SilentlyContinue | "
        "Where-Object {$_.ProcessId -ne $PID -and $_.Name -match 'python' -and "
        "$_.CommandLine -match 'episode_930_autorun|policydb.jobs.worker'} | "
        "Select-Object ProcessId,ParentProcessId,Name,CommandLine; "
        "@($p)|ConvertTo-Json -Compress -Depth 3"
    )
    try:
        result = subprocess.run(
            ["powershell.exe", "-NoLogo", "-NoProfile", "-NonInteractive", "-Command", script],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=5,
            check=False,
        )
        value = json.loads(result.stdout or "[]")
    except (OSError, subprocess.SubprocessError, json.JSONDecodeError):
        return {"runner": [], "workers": []}
    rows = value if isinstance(value, list) else [value]
    runner_rows, worker_rows = [], []
    for row in rows:
        if not isinstance(row, dict):
            continue
        command = str(row.get("CommandLine") or "")
        if "episode_930_autorun" in command:
            runner_rows.append(row)
        if "policydb.jobs.worker" in command:
            worker_rows.append(row)

    def leaf_pids(items: list[dict[str, Any]]) -> list[int]:
        pids = {int(row["ProcessId"]) for row in items if row.get("ProcessId") is not None}
        parent_pids = {
            int(row["ParentProcessId"])
            for row in items
            if row.get("ParentProcessId") is not None
        }
        leaves = sorted(pids - parent_pids)
        return leaves or sorted(pids)

    return {"runner": leaf_pids(runner_rows), "workers": leaf_pids(worker_rows)}


def _render(snapshot: dict[str, Any], output: Path) -> str:
    processes = _process_info()
    now = datetime.now().astimezone().strftime("%Y-%m-%d %H:%M:%S")
    last_real = snapshot.get("last_real_progress_at") or "N/A"
    stage = snapshot.get("stage_progress") or {}
    rates = snapshot.get("stage_throughput_per_hour") or {}
    etas = snapshot.get("stage_eta") or {}
    queue_total = int(snapshot.get("queue_total") or 0)
    queue_completed = int(snapshot.get("queue_completed") or 0)
    queue_pending = int(snapshot.get("queue_pending") or 0)
    queue_reconciliation = snapshot.get("queue_reconciliation") or {}
    buckets = queue_reconciliation.get("accounted_statuses") or {}
    api_health = snapshot.get("api_health") or {}
    timeout_fingerprint_state = api_health.get("timeout_fingerprint") or {}
    lines = [
        "=" * 70,
        " 2016年930楼市调控潮 · 自动搜集与结构化生产监控",
        "=" * 70,
        f"状态              {snapshot.get('episode_status', 'UNKNOWN')}",
        f"执行模式          {snapshot.get('execution_mode', 'UNKNOWN')}",
        f"流水线状态        {snapshot.get('pipeline_status', 'UNKNOWN')}",
        f"当前阶段          {snapshot.get('current_stage', 'UNKNOWN')}",
        f"当前 Run           {snapshot.get('run_id', 'N/A')}",
        f"Runner             {','.join(map(str, processes['runner'])) or 'NOT FOUND'}",
        f"Worker             {','.join(map(str, processes['workers'])) or 'NOT FOUND'}",
        f"Writer             {'1 SAFE' if (output.parents[2] / 'logs' / 'policydb-write.lock').exists() else '0 / IDLE'}",
        f"当前时间           {now}",
        f"最后真实进度       {last_real}",
        "",
        f"OVERALL PROGRESS   {_bar(snapshot.get('overall_progress_percent'))}",
        f"队列               {queue_completed}/{queue_total} completed; {queue_pending} pending",
        f"队列账本           accounted={queue_reconciliation.get('accounted_total', 0)}/{queue_total}; {'CONSISTENT' if queue_reconciliation.get('consistent') else 'INCONSISTENT'}",
        f"状态分解           active={buckets.get('active', 0)} inflight={queue_reconciliation.get('inflight', 0)} leased={buckets.get('leased', 0)} retry={buckets.get('retry', 0)}",
        f"Lease引用          {queue_reconciliation.get('lease_reference_count', 0)}; active historical recovery={queue_reconciliation.get('active_terminal_history_references', 0)}; stale completed={queue_reconciliation.get('stale_completed_lease_references', 0)}",
        f"Discovery账本       raw={snapshot.get('raw_queue_completed', 0)}; provenance={snapshot.get('provenance_verified_completed', 0)}; false_candidates={snapshot.get('false_completion_candidates', 0)}; recovery_required={snapshot.get('false_completion_recovery_required', 0)}; recovered={snapshot.get('recovery_completed', 0)}",
        "",
        "STAGE PROGRESS / RATE / ETA",
    ]
    labels = {
        "analysis_ready_discovery": "Analysis discovery",
        "discovery": "Discovery (final)",
        "official_recovery": "Official recovery",
        "action_extraction": "Action extraction",
        "api_pass1": "API Pass1",
        "api_pass2": "API Pass2",
        "date_verification": "Dates",
        "parameter_extraction": "Parameters",
        "attachment": "Attachments",
        "dedup_quality": "Dedup / quality",
        "formal_promotion": "Promotion",
        "gap_audit": "Gap audit",
        "analysis_ready_gap_audit": "Core gap audit",
        "export_dashboard": "Export / dashboard",
    }
    for name, label in labels.items():
        item = stage.get(name) or {}
        rate = rates.get(name)
        rate_text = "CALIBRATING" if rate in (None, 0) else f"{float(rate):.1f}/h"
        gate = item.get("readiness_gate")
        status = item.get("status") or item.get("raw_status") or "UNKNOWN"
        eta_text = _eta(etas.get(name))
        suffix = (f" {status}" if eta_text != status else "") + (f" gate={gate}" if gate else "")
        lines.append(f"{label:<20}{_bar(item.get('percent')):<42} {rate_text:<14} {eta_text}{suffix}")
    crawl = snapshot.get("crawl") or {}
    latest_api_failure = api_health.get("latest_failure") or {}
    lines.extend([
        "",
        "LIVE WEB CRAWL",
        f"Search calls/results  {crawl.get('search_calls', 0)} / {crawl.get('search_results', 0)}",
        f"HTTP requests/200     {crawl.get('http_requests', 0)} / {crawl.get('http_200', 0)}",
        f"Real fetches/bytes    {crawl.get('real_network_fetches', 0)} / {crawl.get('response_bytes', 0)}",
        f"DocumentVersions      {crawl.get('document_versions', 0)}",
        "",
        "BACKLOG",
        f"Search pending        {queue_pending}",
        f"API Pass1 waiting     {api_health.get('pass1_waiting', 0)}",
        f"API Pass2 eligible    {api_health.get('pass2_eligible', 0)}",
        f"API Pass2 not eligible {api_health.get('pass2_not_yet_eligible', 0)}",
        f"API Pass2 waiting     {api_health.get('pass2_waiting', 0)}",
        f"Core API Pass1        eligible={api_health.get('core_pass1_eligible', 0)} waiting={api_health.get('core_pass1_waiting', 0)} success={api_health.get('core_pass1_success', 0)}",
        f"Core API Pass2        not_eligible={api_health.get('core_pass2_not_eligible', 0)} eligible={api_health.get('core_pass2_eligible', 0)} waiting={api_health.get('core_pass2_waiting', 0)} success={api_health.get('core_pass2_success', 0)}",
        f"API health            {api_health.get('status', 'UNKNOWN')} / gate={api_health.get('recovery_gate', 'UNKNOWN')} / next={api_health.get('next_probe', 'UNKNOWN')}",
        f"API schema/gate       schema_valid={api_health.get('schema_valid', False)}; blocked={api_health.get('recovery_gate_blocked', False)}",
        f"API success age       {api_health.get('last_success_age_seconds', 'N/A')}s; no-success-15m={api_health.get('no_success_for_15m', False)}",
        f"API probe policy      {api_health.get('probe_policy', 'UNKNOWN')}",
        f"API latest failure    {latest_api_failure.get('failure_class') or 'UNCLASSIFIED'} http={latest_api_failure.get('http_status')} response={latest_api_failure.get('response_received')} json={latest_api_failure.get('json_parse_ok')} schema={latest_api_failure.get('schema_valid')}",
        f"Timeout fingerprint    {timeout_fingerprint_state.get('CLIENT_READ_TIMEOUT_SUSPECTED')} samples={timeout_fingerprint_state.get('sample_count', 0)} known={timeout_fingerprint_state.get('known_sample_count', 0)}",
        f"Date recovery         {max(0, int(snapshot.get('actions', 0)) - int(snapshot.get('dates', 0)))}",
        f"Attachment retry      {max(0, int(snapshot.get('attachments_found', 0)) - int(snapshot.get('attachments_resolved', 0)))}",
        "",
        "BLOCKERS",
    ])
    blockers = snapshot.get("blockers") or []
    lines.extend([f"{b.get('severity', 'P?')} {b.get('type')} state={b.get('runtime_state', b.get('status', 'UNKNOWN'))} affected={b.get('affected_documents', 0)}" for b in blockers[:5]] or ["NONE"])
    readiness = snapshot.get("csv_readiness") or {}
    lines.extend([
        "",
        "CSV READINESS",
        f"Live crawl           {'PASS' if readiness.get('live_crawl') else 'FAIL'}",
        f"Official evidence    {'PASS' if readiness.get('official_documents') else 'FAIL'}",
        f"Action extraction    {'PASS' if readiness.get('action_extraction') else 'FAIL'}",
        f"API Pass1            {'PASS' if readiness.get('api_pass1') else 'FAIL'}",
        f"API Pass2            {'PASS' if readiness.get('api_pass2') else 'FAIL'}",
        f"Date verification    {'PASS' if readiness.get('date_verification') else 'FAIL'}",
        f"Formal promotion     {'PASS' if readiness.get('formal_promotion') else 'FAIL'}",
        f"Dashboard export     {'PASS' if readiness.get('dashboard_export') else 'FAIL'}",
        f"CORE critical gaps   {readiness.get('critical_gaps', 0)}",
        f"GLOBAL critical gaps {readiness.get('global_critical_gaps', 0)}",
        f"Gate status           {readiness.get('status', 'UNKNOWN')} failed={','.join(readiness.get('failed_gates', [])) or 'NONE'}",
        f"ANALYSIS READY       {'YES' if readiness.get('analysis_ready') else 'NO'}",
        f"Analysis-ready ETA    {_eta(snapshot.get('analysis_ready_eta'))}",
        f"Final complete ETA    {_eta(snapshot.get('final_complete_eta'))}",
        f"ETA confidence        {snapshot.get('eta_confidence', 'UNKNOWN')}",
        "",
        "SEMANTIC SCOPES",
        f"Current batch         {snapshot.get('CURRENT_BATCH_PROGRESS', {}).get('percent', 'N/A')}% / {snapshot.get('CURRENT_BATCH_PROGRESS', {}).get('completed', 0)}/{snapshot.get('CURRENT_BATCH_PROGRESS', {}).get('total', 0)}",
        f"Analysis discovery    {snapshot.get('analysis_ready_discovery_progress', {}).get('core_verified', 0)}/{snapshot.get('analysis_ready_discovery_progress', {}).get('core_eligible_total', 0)}",
        f"Core postprocess      discovery={snapshot.get('core_rolling_postprocess', {}).get('core_discovery_verified', 0)}/{snapshot.get('core_rolling_postprocess', {}).get('core_discovery_total', 0)}; actions={snapshot.get('core_rolling_postprocess', {}).get('core_action_completed', 0)}/{snapshot.get('core_rolling_postprocess', {}).get('core_action_eligible', 0)}; dates={snapshot.get('core_rolling_postprocess', {}).get('core_date_resolved', 0)}; params={snapshot.get('core_rolling_postprocess', {}).get('core_parameters_processed', 0)}",
        f"Core gaps            {((snapshot.get('analysis_ready_core_blocking_gaps') or {}).get('blocking_gap_count', 0))}; global gaps={((snapshot.get('global_final_blocking_gaps') or {}).get('blocking_gap_count', 0))}",
        f"Final discovery       {snapshot.get('final_discovery_progress', {}).get('discovery_credit_completed', 0)}/{snapshot.get('final_discovery_progress', {}).get('scope_total', 0)}",
        f"Global episode        {snapshot.get('GLOBAL_EPISODE_PROGRESS', {}).get('status', 'UNKNOWN')} / {snapshot.get('GLOBAL_EPISODE_PROGRESS', {}).get('overall_progress_percent', 0)}%",
        f"CSV readiness gate    {snapshot.get('CSV_READINESS_GATE', {}).get('status', 'UNKNOWN')}",
        "",
        f"Snapshot: {output / '930_MONITOR_SNAPSHOT.json'}",
        "Ctrl+C closes monitor only; production runner is not stopped.",
    ])
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-root", default=os.environ.get("CRPD_DATA_ROOT", r"E:\Data Set\CRPD"))
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--write", action="store_true", help="refresh the production-owned monitor snapshot")
    args = parser.parse_args()
    output = Path(args.data_root) / "outputs" / "special_projects" / "2016_930"
    snapshot = build_monitor_snapshot(output, write=args.write)
    print(_render(snapshot, output))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
