from __future__ import annotations

import argparse
import json
import sys
from dataclasses import replace
from datetime import UTC, date, datetime
from pathlib import Path

import polars as pl

from policydb.autopilot import AutopilotConfig
from policydb.autopilot_checkpoints import GlobalSlotCheckpointStore
from policydb.autopilot_runtime import BoundedAutopilotController
from policydb.dashboard_metrics import city_role_matrix
from policydb.episode_930 import Episode930Pipeline, EpisodeConfig
from policydb.episode_930_production import Episode930ProductionController
from policydb.fast_bulk_ingest import (
    FastBulkConfig,
    FastBulkIngestController,
    load_fast_bulk_config,
)
from policydb.full_sync import FullSyncConfig, FullSyncController
from policydb.pdf_pipeline import PDFPipeline, load_pdf_config
from policydb.recent_priority import (
    RECENT_STATUS_TERMINAL,
    Recent30DConfig,
    build_recent_queue,
    run_recent_30d,
)
from policydb.rolling_24m import (
    Rolling24MConfig,
    build_rolling_queue,
    rolling_audit,
    run_rolling_24m,
)
from policydb.settings import Settings
from policydb.source_completion_checkpoint import (
    build_checkpoint_state,
    write_checkpoint_artifacts,
)
from policydb.source_discovery import REQUIRED_ROLES
from policydb.source_slots import audit_525, rebuild_verification_audit


def _resume_recent_window(
    settings: Settings,
    *,
    start_date: date | None,
    end_date: date | None,
    resume: bool,
) -> tuple[date | None, date | None]:
    """Reuse the persisted queue window when ``--resume`` has no dates.

    A moving default date must never turn a resume into a new queue.  Explicit
    CLI dates still win, and a mixed/corrupt queue remains visible to the
    caller instead of being silently guessed.
    """

    if not resume or start_date is not None or end_date is not None:
        return start_date, end_date
    path = settings.outputs / "recent_30d" / "RECENT_30D_QUEUE.parquet"
    if not path.exists():
        return start_date, end_date
    frame = pl.read_parquet(path)
    if not {"start_date", "end_date"}.issubset(frame.columns) or frame.is_empty():
        return start_date, end_date
    pairs = frame.select(["start_date", "end_date"]).drop_nulls().unique()
    if pairs.height != 1:
        return start_date, end_date
    row = pairs.row(0, named=True)
    try:
        return date.fromisoformat(str(row["start_date"])), date.fromisoformat(str(row["end_date"]))
    except ValueError:
        return start_date, end_date


def _add_full_sync_args(command: argparse.ArgumentParser) -> None:
    command.add_argument("--scope", choices=("all", "city", "slot", "source"), default="all")
    command.add_argument("--city-id")
    command.add_argument("--slot-id")
    command.add_argument("--source-id")
    command.add_argument("--scope-file", type=Path)
    command.add_argument("--discovery-mode", choices=("AUTO", "DISABLED", "SEARCH_ONLY", "AI_ONLY", "SEARCH_AND_AI"), default="AUTO")
    command.add_argument("--all-five-source-roles", action="store_true")
    command.add_argument("--discover-missing", action="store_true")
    command.add_argument("--verify-candidates", action="store_true")
    command.add_argument("--enable-ready", action="store_true")
    command.add_argument("--backfill", action="store_true")
    command.add_argument("--incremental", action="store_true")
    command.add_argument("--repair-gaps", action="store_true")
    command.add_argument("--until-current", action="store_true")
    command.add_argument("--all-remaining", action="store_true")
    command.add_argument("--max-slots", type=int, default=20)
    command.add_argument("--max-sources", type=int, default=20)
    command.add_argument("--max-documents", type=int, default=1000)
    command.add_argument("--max-minutes-per-source", type=int)
    command.add_argument("--max-list-pages-per-source", type=int, default=20)
    command.add_argument("--max-document-retries", type=int, default=2)
    command.add_argument("--max-attachment-attempts", type=int, default=1)
    command.add_argument("--top-k", type=int, default=3)
    command.add_argument("--concurrency", type=int, default=1)
    command.add_argument("--discovery-concurrency", type=int, default=1)
    command.add_argument("--crawl-concurrency", type=int, default=1)
    command.add_argument("--max-ai-calls", type=int, default=0)
    command.add_argument("--max-search-calls", type=int, default=5)
    command.add_argument("--max-http-calls", type=int, default=100)
    command.add_argument("--budget-usd", type=float)
    command.add_argument("--budget-tokens", type=int)
    command.add_argument("--rate-limit-per-minute", type=int, default=20)
    command.add_argument("--lookback-days", type=int, default=30)
    command.add_argument("--checkpoint-every", type=int, default=1)
    command.add_argument("--stop-on-error-rate", type=float, default=0.20)
    command.add_argument("--max-consecutive-failures", type=int, default=3)
    command.add_argument("--daily-call-limit", type=int)
    command.add_argument("--backfill-from", "--date-from", dest="backfill_from", type=lambda value: __import__("datetime").date.fromisoformat(value))
    command.add_argument("--backfill-to", "--date-to", dest="backfill_to", type=lambda value: __import__("datetime").date.fromisoformat(value))
    command.add_argument("--resume", action="store_true")
    command.add_argument("--apply", action="store_true")
    command.add_argument("--dry-run", action="store_true")
    command.add_argument("--confirm-full-sync", action="store_true")
    command.add_argument("--output", type=Path)
    command.add_argument("--run-id")
    command.add_argument("--format", dest="report_formats", default="json,xlsx,parquet")
    command.add_argument("--pdf-enabled", action="store_true", help="integrate bounded PDF discovery/download/parse after HTML crawl")
    command.add_argument("--no-pdf-discover", action="store_false", dest="pdf_discover", default=True)
    command.add_argument("--no-pdf-download", action="store_false", dest="pdf_download", default=True)
    command.add_argument("--no-pdf-parse", action="store_false", dest="pdf_parse", default=True)
    command.add_argument("--pdf-max-downloads-per-source", type=int, default=20)
    command.add_argument("--pdf-max-downloads-per-job", type=int, default=30)


def _add_pdf_scope_args(command: argparse.ArgumentParser, *, apply: bool = True) -> None:
    command.add_argument("--root", type=Path, help="PDF inventory/data root; defaults to CRPD_DATA_ROOT")
    command.add_argument("--limit", type=int)
    command.add_argument("--city-id")
    command.add_argument("--source-id")
    if apply:
        command.add_argument("--apply", action="store_true")
    command.add_argument("--run-id")


def _pdf_pipeline_for_args(settings: Settings, args: argparse.Namespace) -> PDFPipeline:
    config = load_pdf_config(settings)
    if args.root:
        root = Path(args.root).resolve()
        config = replace(config, inventory_root=root, archive_root=root / "raw" / "pdf")
    if getattr(args, "workers", None) is not None:
        if args.pdf_command == "download":
            config = replace(config, download_workers=max(1, int(args.workers)))
        elif args.pdf_command == "parse":
            config = replace(config, parse_workers=max(1, int(args.workers)))
    return PDFPipeline(settings, config=config)


def main() -> None:
    parser = argparse.ArgumentParser(description="CRPD bounded API-driven autopilot")
    sub = parser.add_subparsers(dest="command", required=True)
    for name in ("plan", "run", "resume", "status", "stop", "retry", "audit"):
        command = sub.add_parser(name)
        command.add_argument("--mode", default="source-to-full")
        command.add_argument("--run-id")
        command.add_argument("--output", type=Path)
        command.add_argument("--config", type=Path)
        command.add_argument("--provider")
        command.add_argument("--max-slots", type=int)
        command.add_argument("--max-ai-calls", type=int)
        command.add_argument("--concurrency", type=int)
        command.add_argument("--apply", action="store_true")
        command.add_argument("--dry-run", action="store_true")
        command.add_argument("--resume", action="store_true")
        command.add_argument("--auto-full-crawl", action="store_true")
    full_sync = sub.add_parser("full-sync", help="bounded continuous source/backfill/incremental synchronization")
    full_sync_sub = full_sync.add_subparsers(dest="full_sync_command", required=True)
    for name in ("plan", "run", "resume", "status", "refresh", "repair", "report"):
        command = full_sync_sub.add_parser(name)
        _add_full_sync_args(command)
    repair = sub.add_parser("repair-checkpoints", help="inspect or explicitly backfill global slot checkpoints")
    repair.add_argument("--run-dir", type=Path)
    repair.add_argument("--apply", action="store_true")
    rebuild = sub.add_parser("rebuild-verification-audit", help="read-only rebuild of historical deterministic verification evidence")
    rebuild.add_argument("--run-dir", type=Path, required=True)
    rebuild.add_argument("--output", type=Path)
    fast = sub.add_parser("fast-bulk-ingest", help="breadth-first Bronze ingestion with bounded source budgets")
    fast.add_argument("--config", type=Path)
    fast.add_argument("--city-id", action="append", default=[])
    fast.add_argument("--max-cities", type=int)
    fast.add_argument("--max-sources", type=int)
    fast.add_argument("--max-minutes-per-source", type=int)
    fast.add_argument("--max-list-pages-per-source", type=int)
    fast.add_argument("--max-documents-per-source", type=int)
    fast.add_argument("--max-http-calls", type=int)
    fast.add_argument("--concurrency", type=int)
    fast.add_argument("--apply", action="store_true")
    fast.add_argument("--dry-run", action="store_true")
    fast.add_argument("--resume", action="store_true", default=True)
    fast.add_argument("--no-resume", action="store_false", dest="resume")
    fast.add_argument("--output", type=Path)
    recent = sub.add_parser("recent-30d", help="bounded recent-30-day crawl priority and formal promotion")
    recent_sub = recent.add_subparsers(dest="recent_command", required=True)
    for name in ("plan", "run", "status", "audit"):
        command = recent_sub.add_parser(name)
        command.add_argument("--start-date", type=lambda value: __import__("datetime").date.fromisoformat(value))
        command.add_argument("--end-date", type=lambda value: __import__("datetime").date.fromisoformat(value))
        command.add_argument("--max-items", type=int, default=20)
        command.add_argument("--max-pages-per-source", type=int, default=30)
        command.add_argument("--max-candidates-per-shard", type=int, default=500)
        command.add_argument("--max-fetches-per-shard", type=int, default=500)
        command.add_argument("--apply", action="store_true")
        command.add_argument("--resume", action="store_true", default=True)
        command.add_argument("--no-resume", action="store_false", dest="resume")
    rolling = sub.add_parser("rolling-24m", help="dynamic, resumable 24-month all-city source backfill")
    rolling_sub = rolling.add_subparsers(dest="rolling_command", required=True)
    for name in ("plan", "run", "status", "audit"):
        command = rolling_sub.add_parser(name)
        command.add_argument("--start-date", type=lambda value: __import__("datetime").date.fromisoformat(value))
        command.add_argument("--end-date", type=lambda value: __import__("datetime").date.fromisoformat(value))
        command.add_argument("--max-items", type=int, default=5)
        command.add_argument("--max-pages-per-source", type=int, default=300)
        command.add_argument("--max-candidates-per-shard", type=int, default=5000)
        command.add_argument("--max-fetches-per-shard", type=int, default=5000)
        command.add_argument("--max-attempts", type=int, default=3)
        command.add_argument("--pdf-discovery-limit", type=int, default=30)
        command.add_argument("--apply", action="store_true")
        command.add_argument("--resume", action="store_true", default=True)
        command.add_argument("--no-resume", action="store_false", dest="resume")
        command.add_argument("--run-id")
    pdf = sub.add_parser("pdf", help="bounded PDF inventory/archive/discovery/download/parse workflow")
    pdf_sub = pdf.add_subparsers(dest="pdf_command", required=True)
    inventory = pdf_sub.add_parser("inventory", help="read-only recursive PDF inventory")
    _add_pdf_scope_args(inventory, apply=False)
    archive = pdf_sub.add_parser("archive", help="content-addressed copy of existing PDFs")
    _add_pdf_scope_args(archive)
    discover = pdf_sub.add_parser("discover", help="discover PDF links from existing policy pages")
    _add_pdf_scope_args(discover)
    download = pdf_sub.add_parser("download", help="bounded direct-government PDF downloads")
    _add_pdf_scope_args(download)
    download.add_argument("--workers", type=int)
    parse = pdf_sub.add_parser("parse", help="bounded PyMuPDF text extraction; OCR remains disabled")
    _add_pdf_scope_args(parse)
    parse.add_argument("--workers", type=int)
    match = pdf_sub.add_parser("match", help="deterministically match existing PDF assets")
    _add_pdf_scope_args(match)
    status = pdf_sub.add_parser("status", help="show current PDF metrics")
    _add_pdf_scope_args(status, apply=False)
    report = pdf_sub.add_parser("report", help="write a PDF integrity report")
    _add_pdf_scope_args(report, apply=False)
    report.add_argument("--output", type=Path)
    run = pdf_sub.add_parser("run", help="run one bounded PDF inventory/archive/discover/download/parse cycle")
    _add_pdf_scope_args(run)
    city = sub.add_parser("city", help="validated city-scoped operations")
    city_sub = city.add_subparsers(dest="city_command", required=True)
    for name in ("status", "fast-ingest", "complete", "report"):
        command = city_sub.add_parser(name)
        command.add_argument("--city-id", required=True)
        command.add_argument("--source-role")
        command.add_argument("--apply", action="store_true")
    source = sub.add_parser("source", help="validated source-scoped operations")
    source_sub = source.add_subparsers(dest="source_command", required=True)
    source_resume = source_sub.add_parser("resume")
    source_resume.add_argument("--source-id", required=True)
    source_resume.add_argument("--apply", action="store_true")
    source_research = source_sub.add_parser(
        "research",
        help="bounded programmatic research for no-candidate source slots",
    )
    source_research.add_argument("--run-id")
    source_research.add_argument("--output", type=Path)
    source_research.add_argument("--slot-id", action="append", default=[])
    source_research.add_argument("--max-slots", type=int, default=20)
    source_research.add_argument("--max-ai-calls", type=int, default=20)
    source_research.add_argument("--concurrency", type=int, default=2)
    source_research.add_argument("--apply", action="store_true")
    source_research.add_argument("--resume", action="store_true")
    episode = sub.add_parser("episode-930", help="auditable 2016 930 tightening episode supplement")
    episode_sub = episode.add_subparsers(dest="episode_command", required=True)
    for name in ("run", "status", "discover", "recover", "audit", "extract", "classify"):
        command = episode_sub.add_parser(name)
        command.add_argument("--output", type=Path)
        command.add_argument("--apply", action="store_true")
        command.add_argument("--max-search-queries", type=int, default=120)
        command.add_argument("--search-results-per-query", type=int, default=8)
        command.add_argument("--max-official-fetches", type=int, default=120)
        command.add_argument("--max-ai-calls", type=int, default=50)
        command.add_argument("--no-search", action="store_true")
        command.add_argument("--no-ai", action="store_true")
        command.add_argument("--pass-number", type=int, default=1)
    episode_production = sub.add_parser(
        "episode-930-production",
        help="queue or inspect the 2016 930 episode through the formal crawler",
    )
    episode_production_sub = episode_production.add_subparsers(
        dest="episode_production_command", required=True
    )
    production_plan = episode_production_sub.add_parser("plan")
    production_plan.add_argument("--output", type=Path)
    production_plan.add_argument("--run-id")
    production_start = episode_production_sub.add_parser("start")
    production_start.add_argument("--output", type=Path)
    production_start.add_argument("--run-id")
    production_start.add_argument("--city-id", action="append", default=[])
    production_start.add_argument("--city-limit", type=int, default=5)
    production_start.add_argument("--max-ai-calls", type=int, default=10)
    production_start.add_argument("--max-fetches", type=int, default=30)
    production_status = episode_production_sub.add_parser("status")
    production_status.add_argument("--output", type=Path)
    args = parser.parse_args()
    settings = Settings.discover()
    if args.command == "full-sync":
        config = FullSyncConfig.from_namespace(args, command=args.full_sync_command)
        output = args.output
        if output is None and args.full_sync_command in {"status", "resume", "refresh", "repair", "report"}:
            output = FullSyncController.latest_run_dir(settings)
        effective_run_id = args.run_id or (output.name if output is not None else None)
        controller = FullSyncController(settings, config=config, output=output, run_id=effective_run_id)
        result = controller.execute(args.full_sync_command)
        print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
        raise SystemExit(int(result.get("exit_code", 0)))
    if args.command == "repair-checkpoints":
        root = settings.outputs / "autopilot"
        result = GlobalSlotCheckpointStore(root).backfill_from_run_dirs(root, apply=args.apply, run_dir=args.run_dir)
        result.update({"command": "repair-checkpoints", "mode": "apply" if args.apply else "dry-run", "history_modified": bool(args.apply)})
        print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
        raise SystemExit(0)
    if args.command == "rebuild-verification-audit":
        result = rebuild_verification_audit(args.run_dir, settings=settings, output=args.output)
        print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
        raise SystemExit(0)
    if args.command == "fast-bulk-ingest":
        config_path = args.config or settings.root / "config" / "continuous_sync.yaml"
        config = load_fast_bulk_config(config_path if config_path.exists() else None)
        updates = {name: getattr(args, name) for name in ("max_cities", "max_sources", "max_minutes_per_source", "max_list_pages_per_source", "max_documents_per_source", "max_http_calls") if getattr(args, name) is not None}
        if args.concurrency is not None:
            updates["source_concurrency"] = args.concurrency
        if args.city_id:
            updates["city_ids"] = tuple(args.city_id)
            if args.max_cities is None:
                updates["max_cities"] = len(args.city_id)
        updates["apply"] = bool(args.apply)
        updates["dry_run"] = bool(args.dry_run)
        updates["resume"] = bool(args.resume)
        if args.output is not None:
            updates["output"] = args.output
        config = replace(config, **updates)
        config.validate()
        result = FastBulkIngestController(settings, config=config, output=args.output).run(city_ids=args.city_id)
        print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
        raise SystemExit(int(result.get("exit_code", 0)))
    if args.command == "recent-30d":
        resume_start, resume_end = _resume_recent_window(
            settings,
            start_date=args.start_date,
            end_date=args.end_date,
            resume=bool(args.resume),
        )
        base = Recent30DConfig.default(
            start_date=resume_start,
            end_date=resume_end,
            max_items=args.max_items,
            max_pages_per_source=args.max_pages_per_source,
            max_candidates_per_shard=args.max_candidates_per_shard,
            max_fetches_per_shard=args.max_fetches_per_shard,
            apply=bool(args.apply and args.recent_command == "run"),
            resume=bool(args.resume),
        )
        if args.recent_command == "plan":
            result = build_recent_queue(settings, config=base, resume=args.resume)
            payload = {
                "status": "PLANNED",
                "queue_size": result.height,
                "start_date": base.start_date.isoformat(),
                "end_date": base.end_date.isoformat(),
                "output": str(settings.outputs / "recent_30d"),
                "exit_code": 0,
            }
        elif args.recent_command == "status":
            path = settings.automation / "RECENT_30D_STATE.json"
            payload = json.loads(path.read_text(encoding="utf-8")) if path.exists() else {"status": "NOT_STARTED"}
            payload["exit_code"] = 0
        elif args.recent_command == "audit":
            queue_path = settings.outputs / "recent_30d" / "RECENT_30D_QUEUE.parquet"
            queue = pl.read_parquet(queue_path) if queue_path.exists() else pl.DataFrame()
            complete = bool(queue.height) and queue.get_column("status").is_in(list(RECENT_STATUS_TERMINAL)).all()
            payload = {
                "status": "COMPLETE" if complete else "PARTIAL",
                "queue_size": queue.height,
                "completed_items": int(queue.get_column("status").is_in(list(RECENT_STATUS_TERMINAL)).sum()) if queue.height else 0,
                "pending_items": int((~queue.get_column("status").is_in(list(RECENT_STATUS_TERMINAL))).sum()) if queue.height else 0,
                "cities_checked": int(queue.filter(pl.col("attempts") > 0).get_column("city_id").n_unique()) if queue.height and "attempts" in queue.columns else 0,
                "audit": "PASS" if complete else "PENDING",
                "exit_code": 0,
            }
        else:
            payload = run_recent_30d(settings, config=base)
        print(json.dumps(payload, ensure_ascii=False, indent=2, default=str))
        raise SystemExit(int(payload.get("exit_code", 0)))
    if args.command == "rolling-24m":
        config = Rolling24MConfig(
            start_date=args.start_date,
            end_date=args.end_date,
            max_items=args.max_items,
            max_pages_per_source=args.max_pages_per_source,
            max_candidates_per_shard=args.max_candidates_per_shard,
            max_fetches_per_shard=args.max_fetches_per_shard,
            max_attempts=args.max_attempts,
            pdf_discovery_limit=args.pdf_discovery_limit,
            apply=bool(args.apply and args.rolling_command == "run"),
            resume=bool(args.resume),
        )
        if args.rolling_command == "plan":
            queue = build_rolling_queue(settings, config=config, resume=args.resume)
            start, end = config.resolved_window()
            payload = {
                "status": "PLANNED",
                "queue_size": queue.height,
                "window_start": start.isoformat(),
                "window_end": end.isoformat(),
                "target_months": json.loads(queue[0, "target_months"]) if queue.height else [],
                "output": str(settings.outputs / "rolling_24m"),
                "exit_code": 0,
            }
        elif args.rolling_command == "status":
            path = settings.automation / "ROLLING_24M_STATE.json"
            payload = json.loads(path.read_text(encoding="utf-8")) if path.exists() else {"status": "NOT_STARTED"}
            payload["exit_code"] = 0
        elif args.rolling_command == "audit":
            payload = rolling_audit(settings, config=config)
            payload["exit_code"] = 0
        else:
            payload = run_rolling_24m(settings, config=config, run_id=args.run_id)
        print(json.dumps(payload, ensure_ascii=False, indent=2, default=str))
        raise SystemExit(int(payload.get("exit_code", 0)))
    if args.command == "pdf":
        write_commands = {"archive", "discover", "download", "parse", "match", "run"}
        if args.pdf_command in write_commands and not args.apply:
            raise SystemExit(f"pdf {args.pdf_command} requires --apply; inventory/status/report are read-only")
        pipeline = _pdf_pipeline_for_args(settings, args)
        run_id = args.run_id
        if args.pdf_command == "inventory":
            result = pipeline.inventory(limit=args.limit, run_id=run_id)
        elif args.pdf_command == "archive":
            result = pipeline.archive(limit=args.limit, run_id=run_id)
        elif args.pdf_command == "discover":
            result = pipeline.discover(limit=args.limit, city_id=args.city_id, source_id=args.source_id, run_id=run_id)
        elif args.pdf_command == "download":
            result = pipeline.download(limit=args.limit, city_id=args.city_id, source_id=args.source_id, run_id=run_id)
        elif args.pdf_command == "parse":
            result = pipeline.parse(limit=args.limit, city_id=args.city_id, source_id=args.source_id, run_id=run_id)
        elif args.pdf_command == "match":
            result = pipeline.match(limit=args.limit, run_id=run_id)
        elif args.pdf_command == "status":
            result = pipeline.summary()
        elif args.pdf_command == "report":
            result = pipeline.report(output=args.output)
        else:
            result = pipeline.run(limit=args.limit, city_id=args.city_id, source_id=args.source_id, run_id=run_id)
        print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
        raise SystemExit(0)
    if args.command == "city":
        matrix = city_role_matrix(settings)
        selected = matrix.filter(pl.col("city_id").cast(pl.String) == args.city_id) if not matrix.is_empty() else matrix
        if args.city_command == "status":
            print(json.dumps({"city_id": args.city_id, "rows": selected.to_dicts()}, ensure_ascii=False, indent=2, default=str))
            raise SystemExit(0)
        if args.city_command == "report":
            print(selected.write_json(row_oriented=True) if not selected.is_empty() else json.dumps({"city_id": args.city_id, "rows": []}, ensure_ascii=False))
            raise SystemExit(0)
        if not args.apply:
            raise SystemExit("city write operations require --apply")
        if args.city_command == "fast-ingest":
            config = FastBulkConfig(apply=True, resume=True, max_cities=1, city_ids=(args.city_id,))
            result = FastBulkIngestController(settings, config=config).run(city_ids=[args.city_id])
        else:
            if args.source_role:
                if args.source_role not in REQUIRED_ROLES:
                    raise SystemExit(f"unsupported source role: {args.source_role}")
                config = FastBulkConfig(apply=True, resume=True, max_cities=1, city_ids=(args.city_id,), source_roles=(args.source_role,))
                result = FastBulkIngestController(settings, config=config).run(city_ids=[args.city_id])
            else:
                config = FullSyncConfig(scope="city", city_id=args.city_id, source_id=None, all_five_source_roles=True, backfill=True, incremental=True, resume=True, apply=True, max_slots=5, max_sources=5)
                result = FullSyncController(settings, config=config).run(command="run")
        print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
        raise SystemExit(int(result.get("exit_code", 0)))
    if args.command == "source":
        if not args.apply:
            raise SystemExit("source write operations require --apply")
        if args.source_command == "research":
            config = AutopilotConfig.load(root=settings.root)
            config = replace(
                config,
                max_slots_per_batch=args.max_slots,
                max_ai_calls_per_batch=args.max_ai_calls,
                concurrency=args.concurrency,
            )
            config.validate()
            controller = BoundedAutopilotController(
                settings,
                config=config,
                output=args.output,
                run_id=args.run_id,
                research_mode=True,
                slot_ids=set(args.slot_id or []),
            )
            result = controller.run(apply=True, resume=args.resume)
            result["mode"] = "programmatic_manual_research"
            # Publish a unique, crash-safe operator checkpoint after the batch
            # has reached its own atomic boundary.  The historical checkpoint
            # directories remain immutable; this never overwrites a prior run.
            run_dir = Path(str(result.get("run_dir") or controller.run_dir))
            checkpoint_id = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
            checkpoint_dir = settings.outputs / "source_completion_ai" / checkpoint_id
            if checkpoint_dir.exists():
                checkpoint_dir = settings.outputs / "source_completion_ai" / f"{checkpoint_id}_{args.run_id or run_dir.name}"
            checkpoint_state = build_checkpoint_state(
                run_id=str(result.get("run_id") or args.run_id or run_dir.name),
                run_dir=run_dir,
                current_status=controller._state(),
                slot_audit=audit_525(settings),
                current_command=" ".join(sys.argv),
                repo=settings.root,
                checkpoint_id=checkpoint_dir.name,
                stop_reason={
                    "code": "BATCH_COMPLETED_SAFE_BOUNDARY",
                    "go_gate": result.get("go_gate"),
                    "exit_code": result.get("exit_code"),
                },
            )
            checkpoint = write_checkpoint_artifacts(
                checkpoint_dir,
                state=checkpoint_state,
                report=result,
            )
            result["checkpoint_dir"] = checkpoint["checkpoint_dir"]
            print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
            raise SystemExit(int(result.get("exit_code", 0)))
        config = FullSyncConfig(scope="source", source_id=args.source_id, backfill=True, incremental=True, resume=True, apply=True, max_sources=1)
        result = FullSyncController(settings, config=config).run(command="run")
        print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
        raise SystemExit(int(result.get("exit_code", 0)))
    if args.command == "episode-930":
        config = EpisodeConfig(
            max_search_queries=max(0, args.max_search_queries),
            search_results_per_query=max(1, args.search_results_per_query),
            max_official_fetches=max(1, args.max_official_fetches),
            max_ai_calls=max(0, args.max_ai_calls),
            run_search=not args.no_search,
            run_ai=not args.no_ai,
            apply=bool(args.apply),
        )
        pipeline = Episode930Pipeline(settings, config=config, output=args.output)
        if args.episode_command == "run":
            result = pipeline.run()
        elif args.episode_command == "status":
            result = pipeline.status()
        elif args.episode_command == "discover":
            pipeline.scope()
            result = pipeline.discover()
        else:
            candidate_path = pipeline.phase_dirs["01_DISCOVERY"] / "930_DISCOVERY_CANDIDATES.parquet"
            city_path = pipeline.phase_dirs["01_DISCOVERY"] / "2016_930_CITY_DISCOVERY.parquet"
            if not candidate_path.exists() or not city_path.exists():
                pipeline.scope()
                pipeline.discover()
            cities = pl.read_parquet(city_path)
            candidates = pl.read_parquet(candidate_path).to_dicts()
            recovery_path = pipeline.phase_dirs["02_OFFICIAL_RECOVERY"] / "930_OFFICIAL_RECOVERY.parquet"
            if recovery_path.exists() and args.episode_command in {"audit", "extract", "classify"}:
                documents = pl.read_parquet(recovery_path)
                recovery = {"status": "REUSED_OFFICIAL_RECOVERY", "documents": documents.height}
            else:
                documents, recovery = pipeline.official_recovery(candidates=candidates, cities_frame=cities)
            if args.episode_command == "recover":
                result = recovery
            elif args.episode_command == "audit":
                _, gaps, result = pipeline.gap_audit(documents, pass_number=args.pass_number)
                result["gap_rows"] = gaps.height
            elif args.episode_command == "extract":
                actions, params, result = pipeline.extract_actions(documents)
                result["parameter_rows"] = params.height
            else:
                actions, _, _ = pipeline.extract_actions(documents)
                _, result = pipeline.classify_actions(documents, actions)
        print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
        raise SystemExit(0)
    if args.command == "episode-930-production":
        controller = Episode930ProductionController(
            settings,
            output=args.output,
            run_id=getattr(args, "run_id", None),
            city_limit=getattr(args, "city_limit", 5),
            max_ai_calls=getattr(args, "max_ai_calls", 10),
        )
        if args.episode_production_command == "plan":
            result = controller.build_plan()
        elif args.episode_production_command == "status":
            result = {
                "episode_id": controller.output.name,
                "run_id": controller.run_id,
                "progress": controller.snapshot_path.read_text(encoding="utf-8")
                if controller.snapshot_path.exists()
                else None,
                "checkpoint": controller.checkpoint_path.read_text(encoding="utf-8")
                if controller.checkpoint_path.exists()
                else None,
            }
        else:
            from policydb.jobs.manager import JobManager
            from policydb.jobs.models import CrawlJobRequest

            manager = JobManager(settings)
            request = CrawlJobRequest(
                mode="historical_episode_930",
                episode_id="EP_2016_930_TIGHTENING",
                cities=list(args.city_id),
                episode_city_limit=args.city_limit,
                episode_max_ai_calls=args.max_ai_calls,
                max_fetches=args.max_fetches,
                processing_mode="full",
                rebuild_database=False,
                run_validation=False,
            )
            state = manager.create(request)
            started = manager.start(state.job_id)
            result = {
                "status": "STARTED",
                "job_id": state.job_id,
                "pid": started.pid,
                "request": request.model_dump(mode="json"),
            }
        print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
        raise SystemExit(0)
    config = AutopilotConfig.load(args.config, root=settings.root)
    overrides = {}
    if args.provider is not None:
        overrides["provider"] = args.provider
    if args.max_slots is not None:
        overrides["max_slots_per_batch"] = args.max_slots
    if args.max_ai_calls is not None:
        overrides["max_ai_calls_per_batch"] = args.max_ai_calls
    if args.concurrency is not None:
        overrides["concurrency"] = args.concurrency
    if overrides:
        config = replace(config, **overrides)
        config.validate()
    controller = BoundedAutopilotController(settings, config=config, output=args.output, run_id=args.run_id)
    if args.command == "plan":
        result = controller.run(apply=False, resume=args.resume)
    elif args.command in {"run", "resume"}:
        result = controller.run(apply=True if args.command == "resume" else args.apply, resume=args.resume or args.command == "resume")
    elif args.command == "status":
        result = controller._state()
    elif args.command == "stop":
        result = controller.store.request_stop()
    elif args.command == "retry":
        controller.store.stop_path.unlink(missing_ok=True)
        result = controller._state()
    else:
        result = {"run_dir": str(controller.run_dir), "current": controller._state()}
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    raise SystemExit(int(result.get("exit_code", 0)))


if __name__ == "__main__":
    main()
