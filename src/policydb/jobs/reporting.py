from __future__ import annotations

import csv
import json
from pathlib import Path

import polars as pl

from policydb.jobs.models import JobState
from policydb.settings import Settings

REPORT_TABLES = (
    "discovered_candidates",
    "fetched_documents",
    "new_policies",
    "duplicate_documents",
    "recovered_sources",
    "errors",
    "source_health",
)

RESULT_PATH_MAP = {
    "discovered_candidates": "crawl_items",
    "fetched_documents": "policy_document_versions",
    "errors": "fetch_errors",
    "source_health": "source_health",
}


def _write_csv(path: Path, rows: list[dict]) -> None:
    fields = sorted({key for row in rows for key in row}) or ["status"]
    with path.open("w", encoding="utf-8-sig", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _write_result_csv(path: Path, name: str, result: dict) -> None:
    rows = result.get(name)
    if isinstance(rows, list):
        _write_csv(path, rows)
        return
    table_name = RESULT_PATH_MAP.get(name)
    source = result.get("table_paths", {}).get(table_name) if table_name else None
    if source and Path(source).exists():
        pl.scan_parquet(source).sink_csv(path)
        return
    preview = result.get("previews", {}).get(name, [])
    _write_csv(path, preview)


def generate_crawl_report(
    settings: Settings,
    state: JobState,
    result: dict,
) -> Path:
    output = settings.root / "outputs" / "crawl_reports" / state.job_id
    output.mkdir(parents=True, exist_ok=True)
    summary = {
        "job_id": state.job_id,
        "mode": state.mode,
        "status": state.status,
        "run_id": state.run_id,
        **result.get("metrics", {}),
        "recommendations": result.get("recommendations", []),
    }
    (output / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    for name in REPORT_TABLES:
        _write_result_csv(output / f"{name}.csv", name, result)
    recommendations = "\n".join(f"- {item}" for item in summary["recommendations"]) or "- 本次没有额外建议。"
    markdown = f"""# 抓取运行报告

- 任务：{state.job_id}
- 模式：{state.mode}
- 状态：{state.status}
- 来源数：{summary.get('source_count', 0)}
- 候选 URL：{summary.get('candidate_count', 0)}
- 抓取成功：{summary.get('fetched', 0)}
- 抓取失败：{summary.get('failed', 0)}
- 新增文档版本：{summary.get('document_versions', 0)}
- 自动审核通过：{summary.get('auto_verified', 0)}
- 人工审核剩余：{summary.get('manual_review', 0)}

## 可执行建议

{recommendations}
"""
    (output / "report.md").write_text(markdown, encoding="utf-8")
    job_dir = settings.root / "data" / "logs" / "crawl_jobs" / state.job_id
    (job_dir / "report.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    (job_dir / "report.md").write_text(markdown, encoding="utf-8")
    return output
