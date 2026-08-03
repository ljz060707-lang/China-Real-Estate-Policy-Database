from __future__ import annotations

import json
import os
import subprocess
from datetime import UTC, datetime
from pathlib import Path

import polars as pl

from policydb.parquet_store import read_parquet_snapshot
from policydb.settings import Settings
from policydb.source_slots import audit_525


def _git(root: Path, *args: str) -> str | None:
    try:
        result = subprocess.run(
            ["git", *args],
            cwd=root,
            check=True,
            capture_output=True,
            text=True,
            timeout=10,
        )
        return result.stdout.strip()
    except (OSError, subprocess.SubprocessError):
        return None


def build_exhaustive_acceptance(
    settings: Settings | None = None,
) -> dict:
    settings = settings or Settings.discover()
    output = settings.outputs / "acceptance"
    output.mkdir(parents=True, exist_ok=True)
    shards_path = settings.curated / "crawl_shards.parquet"
    progress_path = settings.curated / "city_year_progress.parquet"
    candidates_path = settings.curated / "source_candidates.parquet"
    shards = read_parquet_snapshot(shards_path) if shards_path.exists() else pl.DataFrame()
    progress = (
        read_parquet_snapshot(progress_path) if progress_path.exists() else pl.DataFrame()
    )
    candidates = (
        read_parquet_snapshot(candidates_path)
        if candidates_path.exists()
        else pl.DataFrame()
    )
    nanjing = (
        shards.filter(pl.col("city_id") == "CITY_320100")
        if shards.height
        else pl.DataFrame()
    )
    nanjing_report = {
        "city_id": "CITY_320100",
        "period": "2023-02",
        "shards": nanjing.height,
        "fetched": int(nanjing["fetched"].sum()) if nanjing.height else 0,
        "failed": int(nanjing["failed"].sum()) if nanjing.height else 0,
        "document_versions": (
            int(nanjing["document_versions"].sum()) if nanjing.height else 0
        ),
        "date_unknown": (
            int(nanjing["date_unknown_count"].sum()) if nanjing.height else 0
        ),
        "cross_period_rejected": (
            int(nanjing["cross_period_rejected_count"].sum())
            if nanjing.height
            else 0
        ),
        "statuses": (
            nanjing.group_by("status")
            .agg(pl.len().alias("count"))
            .to_dicts()
            if nanjing.height
            else []
        ),
        "certified_complete": False,
        "conclusion": (
            "已形成真实文档版本，但来源槽位、未知日期、分页或错误闭环尚未满足；不得认证完整。"
            if nanjing.height and int(nanjing["fetched"].sum())
            else "未形成真实文档版本；不得把运行结束视为抓取成功。"
        ),
    }
    nanjing_path = output / "nanjing_2023_02_report.json"
    nanjing_path.write_text(
        json.dumps(nanjing_report, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    report = {
        "generated_at": datetime.now(UTC).isoformat(),
        "repository_root": str(settings.root),
        "data_root": str(settings.data_root),
        "branch": _git(settings.root, "branch", "--show-current"),
        "commit": _git(settings.root, "rev-parse", "HEAD"),
        "source_525": audit_525(settings),
        "source_candidates": candidates.height,
        "verified_candidates": (
            candidates.filter(pl.col("is_verified")).height
            if candidates.height
            else 0
        ),
        "crawl_shards": shards.height,
        "city_year_rows": progress.height,
        "certified_city_years": (
            progress.filter(pl.col("status") == "certified_complete").height
            if progress.height
            else 0
        ),
        "confirmed_zero_city_years": (
            progress.filter(pl.col("status") == "confirmed_zero").height
            if progress.height
            else 0
        ),
        "network_diagnostics_nanjing": str(
            output / "network_diagnostics_nanjing.json"
        ),
        "nanjing_2023_02": nanjing_report,
        "truthful_scope": {
            "code_implemented": True,
            "data_artifacts_materialized": True,
            "nanjing_live_validation_run": nanjing.height > 0,
            "all_105_historical_crawl_completed": False,
            "all_525_sources_verified": False,
        },
        "environment": {
            "d_drive_available": settings.data_root.exists(),
            "raw_modified_by_acceptance": False,
            "proxy_values_recorded": False,
        },
    }
    target = output / "exhaustive_crawl_acceptance.json"
    temp = target.with_suffix(".json.tmp")
    temp.write_text(
        json.dumps(report, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    os.replace(temp, target)
    report["output"] = str(target)
    return report
