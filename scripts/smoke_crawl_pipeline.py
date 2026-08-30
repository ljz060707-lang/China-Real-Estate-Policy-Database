from __future__ import annotations

import argparse
import hashlib
import json
from datetime import UTC, date, datetime
from pathlib import Path
from urllib.parse import urlsplit

import polars as pl
import yaml

from policydb.crawl.fetcher import RespectfulFetcher
from policydb.crawl.pipeline import CrawlPipeline
from policydb.settings import Settings


def _atomic_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    temporary.replace(path)


def main() -> int:
    parser = argparse.ArgumentParser(description="Run a bounded real CRPD pipeline smoke.")
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--limit", type=int, default=12)
    parser.add_argument("--concurrency", type=int, default=6)
    parser.add_argument("--per-host-concurrency", type=int, default=1)
    parser.add_argument("--timeout", type=float, default=8.0)
    parser.add_argument("--connect-timeout", type=float, default=4.0)
    args = parser.parse_args()

    if args.output_root.exists() and any(args.output_root.iterdir()):
        raise FileExistsError(f"smoke output must be new and empty: {args.output_root}")
    reference = args.output_root / "data" / "reference"
    curated = args.output_root / "data" / "curated"
    reference.mkdir(parents=True, exist_ok=True)
    curated.mkdir(parents=True, exist_ok=True)

    manifest = json.loads(args.manifest.read_text(encoding="utf-8"))
    urls = [str(value) for value in manifest["urls"][: args.limit]]
    sources = []
    for ordinal, url in enumerate(urls, start=1):
        host = urlsplit(url).netloc.lower()
        sources.append(
            {
                "source_id": f"SMOKE_{ordinal:03d}",
                "source_name": f"Pipeline smoke {ordinal:03d}",
                "domain": host,
                "source_type": "government",
                "source_role": "canonical_candidate",
                "official_status": "official",
                "seed_urls": [url],
                "crawl_enabled": True,
                "priority": ordinal,
                "rate_limit": 0.2,
            }
        )
    (reference / "source_registry.yaml").write_text(
        yaml.safe_dump({"sources": sources}, allow_unicode=True),
        encoding="utf-8",
    )

    data_root = args.output_root / "data"
    settings = Settings(
        root=args.output_root,
        data_root_path=data_root,
        database_path=data_root / "database" / "policydb.duckdb",
        curated_path=curated,
        raw_path=data_root / "raw",
        archive_path=data_root / "archive",
        research_path=data_root / "research",
        outputs_path=data_root / "outputs",
        logs_path=data_root / "logs",
        automation_path=data_root / "automation",
        control_path=data_root / "control",
        runtime_path=data_root / "runtime",
        cache_path=data_root / "cache",
        temp_path=data_root / "temp",
        test_artifacts_path=data_root / "test_artifacts",
        dashboard_path=data_root / "dashboard",
        backups_path=data_root / "backups",
        dashboard_runtime_path=data_root / "runtime" / "dashboard",
    )
    resolved_paths = {
        settings.curated,
        settings.raw,
        settings.archive_root,
        settings.logs,
        settings.outputs,
        settings.database,
    }
    if any(args.output_root not in path.parents for path in resolved_paths):
        raise RuntimeError(f"smoke storage escaped output root: {resolved_paths}")
    fetcher = RespectfulFetcher(
        timeout=args.timeout,
        connect_timeout=args.connect_timeout,
        retries=1,
        rate_limit=0.2,
        check_robots=True,
    )
    try:
        pipeline = CrawlPipeline(settings, fetcher=fetcher)
        plan = pipeline.plan(
            run_type="bounded_real_pipeline_smoke",
            start_date=date(2018, 1, 1),
            end_date=date.today(),
            max_candidates_total=args.limit,
            global_safety_limit=args.limit,
        )
        first = pipeline.run(
            plan["run_id"],
            max_fetches=args.limit,
            fetch_concurrency=args.concurrency,
            per_host_concurrency=args.per_host_concurrency,
        )
        second = pipeline.run(
            plan["run_id"],
            max_fetches=args.limit,
            fetch_concurrency=args.concurrency,
            per_host_concurrency=args.per_host_concurrency,
        )
    finally:
        fetcher.client.close()

    items = pl.read_parquet(curated / "crawl_items.parquet")
    versions_path = curated / "policy_document_versions.parquet"
    versions = pl.read_parquet(versions_path) if versions_path.exists() else pl.DataFrame()
    terminal = items.filter(pl.col("status") != "pending").height
    unique_versions = (
        versions["document_version_id"].n_unique()
        if versions.height and "document_version_id" in versions.columns
        else 0
    )
    local_paths_exist = bool(
        versions.height
        and all(
            (args.output_root / str(path)).exists() if not Path(str(path)).is_absolute() else Path(str(path)).exists()
            for path in versions["local_path"].to_list()
        )
    )
    passed = bool(
        first.get("fetched", 0) > 0
        and terminal == items.height
        and second.get("fetched") == 0
        and unique_versions == versions.height
        and local_paths_exist
    )
    result = {
        "schema_version": "crpd-bounded-real-pipeline-smoke-v1",
        "generated_at": datetime.now(UTC).isoformat(),
        "manifest_path": str(args.manifest),
        "manifest_sha256": hashlib.sha256(args.manifest.read_bytes()).hexdigest(),
        "url_count": len(urls),
        "first_run": first,
        "resume_run": second,
        "crawl_items": items.height,
        "terminal_items": terminal,
        "document_versions": versions.height,
        "unique_document_versions": unique_versions,
        "raw_or_local_evidence_paths_exist": local_paths_exist,
        "single_writer_design": True,
        "production_database_touched": False,
        "passed": passed,
    }
    _atomic_json(args.output_root / "smoke_result.json", result)
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
