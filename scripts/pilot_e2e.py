"""CRPD pilot city end-to-end driver — real chain, isolated pilot DB, resumable.

Proves the full pipeline for one or more real cities (default Beijing
CITY_110000) from source inventory → live crawl (discovery/fetch/parse/dedup)
→ classification → promotion → database materialization → coverage → review
queue → release, entirely inside an isolated pilot root (separate duckdb +
curated/parquet + releases). Production data root is never written.

Stages are checkpointed under <pilot_root>/checkpoints; a re-run with
--resume skips completed stages (idempotent). Live network only with --apply.

Usage:
  python scripts/pilot_e2e.py                                # dry run (no changes)
  python scripts/pilot_e2e.py --apply                        # execute all stages
  python scripts/pilot_e2e.py --apply --resume               # resume unfinished stages
  python scripts/pilot_e2e.py --apply --cities CITY_120000,CITY_210200   # multi-city
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import sys
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

from policydb.coverage_audit import run_coverage_audit
from policydb.crawl.registry import load_registry
from policydb.crawl.service import CrawlService
from policydb.export.release import create_release
from policydb.ingest.promote_versions import promote_document_versions
from policydb.jobs.manager import PolicyWriteLock
from policydb.jobs.models import CrawlJobRequest
from policydb.settings import Settings

REPO_ROOT = Path(__file__).resolve().parents[1]
PRODUCTION_ROOT = Path(r"E:\Data Set\CRPD")

# Run configuration (set by configure_run in main()).
CITIES: list[str] = ["CITY_110000"]
PILOT_ROOT = PRODUCTION_ROOT / "pilot" / "CITY_110000_pilot1"
RELEASE_VERSION = "pilot-1.0.0"


def configure_run(cities: list[str]) -> None:
    """Pin run-wide globals from the requested city list (idempotent)."""
    global CITIES, PILOT_ROOT, RELEASE_VERSION
    CITIES = [str(city).strip() for city in cities if str(city).strip()]
    if not CITIES:
        raise ValueError("--cities must not be empty")
    if len(CITIES) == 1:
        PILOT_ROOT = PRODUCTION_ROOT / "pilot" / f"{CITIES[0]}_pilot1"
        RELEASE_VERSION = "pilot-1.0.0"
    else:
        PILOT_ROOT = PRODUCTION_ROOT / "pilot" / f"multi_{len(CITIES)}"
        RELEASE_VERSION = f"pilot-multi-{len(CITIES)}-1.0.0"

STAGES = [
    "SOURCE_INVENTORY",
    "CRAWL",
    "CLASSIFY",
    "PROMOTE",
    "DATABASE",
    "COVERAGE",
    "REVIEW_QUEUE",
    "RELEASE",
]


def checkpoints_dir() -> Path:
    """Checkpoint dir for the CURRENT run root (configure_run may change it)."""
    return PILOT_ROOT / "checkpoints"


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def checkpoint_path(stage: str) -> Path:
    return checkpoints_dir() / f"pilot_e2e_{stage}.json"


def load_checkpoint(stage: str) -> dict | None:
    path = checkpoint_path(stage)
    if path.exists():
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return None
    return None


def write_checkpoint(stage: str, payload: dict) -> None:
    checkpoints_dir().mkdir(parents=True, exist_ok=True)
    payload = {"stage": stage, "at": datetime.now(UTC).isoformat(), **payload}
    path = checkpoint_path(stage)
    temp = path.with_suffix(".json.tmp")
    temp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    temp.replace(path)


def stage_done(stage: str, resume: bool) -> bool:
    if not resume:
        return False
    return load_checkpoint(stage) is not None


def pilot_settings() -> Settings:
    """Pilot settings with EVERY storage path pinned to the pilot root.

    The production machine environment exports CRPD_DATA_ROOT / POLICYDB_DATABASE /
    POLICYDB_CURATED_ROOT / POLICYDB_OUTPUT_ROOT, which would otherwise override
    the data_root_path field. Explicit fields always win, so pin all of them.
    """
    return Settings(
        root=REPO_ROOT,
        data_root_path=PILOT_ROOT,
        database_path=PILOT_ROOT / "database" / "policydb.duckdb",
        curated_path=PILOT_ROOT / "curated",
        raw_path=PILOT_ROOT / "raw",
        research_path=PILOT_ROOT / "research",
        outputs_path=PILOT_ROOT / "outputs",
        logs_path=PILOT_ROOT / "logs",
        automation_path=PILOT_ROOT / "automation",
        control_path=PILOT_ROOT / "control",
        runtime_path=PILOT_ROOT / "runtime",
        cache_path=PILOT_ROOT / "cache",
        temp_path=PILOT_ROOT / "temp",
        test_artifacts_path=PILOT_ROOT / "test_artifacts",
        dashboard_path=PILOT_ROOT / "dashboard",
        backups_path=PILOT_ROOT / "backups",
        dashboard_runtime_path=PILOT_ROOT / "runtime" / "dashboard",
    )


def assert_isolation(settings: Settings) -> None:
    """Abort unless every write target resolves inside the pilot root."""
    resolved = {
        "database": settings.database,
        "curated": settings.curated,
        "outputs": settings.outputs,
        "logs": settings.logs,
        "jobs": settings.jobs,
        "manifests": settings.manifests,
        "raw": settings.raw,
        "temp": settings.temp,
        "cache": settings.cache,
    }
    leaked = {name: str(path) for name, path in resolved.items() if PILOT_ROOT not in path.parents}
    if leaked:
        raise RuntimeError(f"pilot isolation violated — paths outside pilot root: {leaked}")


def stage_source_inventory() -> dict:
    settings = pilot_settings()
    sources = load_registry(settings)
    requested = set(CITIES)
    city_sources = [
        s for s in sources
        if requested & (set(s.city_ids or []) | set(s.coverage_city_ids or []))
        or s.scope_type == "national"
    ]
    rows = [
        {
            "source_id": s.source_id,
            "source_name": s.source_name,
            "domain": s.domain,
            "official_status": s.official_status,
            "scope_type": s.scope_type,
            "crawl_enabled": s.crawl_enabled,
            "required_level": s.required_level,
            "seed_urls": len(s.seed_urls or []),
            "list_page_urls": len(s.list_page_urls or []),
            "search_url_template": bool(s.search_url_template),
        }
        for s in city_sources
    ]
    enabled = sum(1 for r in rows if r["crawl_enabled"])
    csv_path = PILOT_ROOT / "evidence" / "pilot_source_inventory.csv"
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    with open(csv_path, "w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]) if rows else ["source_id"])
        writer.writeheader()
        writer.writerows(rows)
    return {
        "status": "COMPLETED",
        "sources_total": len(rows),
        "sources_enabled": enabled,
        "evidence": str(csv_path),
    }


def stage_crawl(apply: bool, window_days: int, max_fetches: int) -> dict:
    settings = pilot_settings()
    today = date.today()
    request = CrawlJobRequest(
        mode="official_update",
        cities=CITIES,
        start_date=today - timedelta(days=window_days),
        end_date=today,
        max_fetches=max_fetches,
        max_candidates_per_source=20,
        max_pages_per_source=10,
        max_attachment_attempts=2,
        run_glm=False,
        run_verification=False,
        enabled_only=True,
        official_first=True,
        resume=True,
    )
    if not apply:
        service = CrawlService(settings)
        estimate = service.estimate(request)
        return {"status": "DRY_RUN", "estimate": estimate, "run_id": None}
    service = CrawlService(settings)
    result = service.execute(request)
    return {
        "status": "COMPLETED",
        "run_id": result["run_id"],
        "metrics": result["metrics"],
        "table_paths": result["table_paths"],
    }


def stage_classify() -> dict:
    """Deterministic document-level classification over pilot versions."""
    import polars as pl

    from policydb.classify.rules import classify as classify_text

    settings = pilot_settings()
    versions_path = settings.curated / "policy_document_versions.parquet"
    if not versions_path.exists():
        return {"status": "SKIPPED_NO_VERSIONS", "rows": 0}
    frame = pl.read_parquet(versions_path)
    rows = []
    for row in frame.iter_rows(named=True):
        text = str(row.get("extracted_text") or "")[:4000]
        hits = classify_text(text)
        rows.append(
            {
                "document_version_id": row.get("document_version_id"),
                "record_id": row.get("record_id"),
                "title": row.get("title"),
                "classification_hits": len(hits),
                "hits_json": json.dumps(hits, ensure_ascii=False),
            }
        )
    out = PILOT_ROOT / "evidence" / "pilot_classifications.csv"
    out.parent.mkdir(parents=True, exist_ok=True)
    with open(out, "w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]) if rows else ["document_version_id"])
        writer.writeheader()
        writer.writerows(rows)
    return {"status": "COMPLETED", "rows": len(rows), "evidence": str(out)}


def stage_promote(apply: bool) -> dict:
    settings = pilot_settings()
    checkpoint = load_checkpoint("CRAWL")
    run_id = checkpoint.get("run_id") if checkpoint else None
    if not run_id:
        return {"status": "SKIPPED_NO_RUN_ID", "run_id": None}
    if not apply:
        return {"status": "DRY_RUN", "run_id": run_id}
    with PolicyWriteLock(settings, f"PILOT_PROMOTE_{run_id}"):
        result = promote_document_versions(settings, run_id=run_id, apply=True)
    return {"status": "COMPLETED", "run_id": run_id, "promote": result}


# Canonical record terms required by the database views (v_policy_master,
# v_policy_topic_long, dashboard counts). Derived deterministically from
# document text; same vocabulary the views query.
CANONICAL_TERMS = [
    "限购", "限售", "商业住房贷款", "限贷", "公积金", "购房补贴",
    "人才住房", "城市更新", "城中村改造", "老旧小区改造", "危旧房改造",
]


def stage_database(apply: bool) -> dict:
    """Materialize the pilot duckdb from pilot curated parquet (atomic).

    record_terms is normally produced by the Excel-import path; for web-crawl
    pilots it is derived deterministically from extracted document text using
    the exact vocabulary the database views query (no invented semantics).
    """
    if not apply:
        return {"status": "DRY_RUN"}
    import shutil

    import polars as pl

    from policydb.query.database import build_database_atomic

    settings = pilot_settings()
    # Shared reference/governance layer (global, not pilot-derived): the 525-slot
    # source universe, the 105-city universe, jurisdiction and city mappings.
    # Copied read-only from production curated so the DB migrations and coverage
    # views are structurally complete; pilot record-level data stays pilot-only.
    shared_reference = [
        "source_registry.parquet",
        "cities_105.parquet",
        "jurisdictions.parquet",
        "policy_applicable_cities.parquet",
    ]
    copied_reference = []
    for name in shared_reference:
        source = PRODUCTION_ROOT / "curated" / name
        if source.exists():
            shutil.copy2(source, settings.curated / name)
            copied_reference.append(name)

    # Schema-only stubs (zero rows) for Excel-derived optional tables the
    # deferred migrations reference; the schema comes from the production
    # parquet, no rows are invented.
    def _schema_stub(name: str) -> bool:
        source = PRODUCTION_ROOT / "curated" / f"{name}.parquet"
        target = settings.curated / f"{name}.parquet"
        if target.exists() or not source.exists():
            return False
        schema = pl.read_parquet_schema(source)
        pl.DataFrame(schema=dict(schema)).write_parquet(target)
        return True

    stubbed_tables = [name for name in ("policy_files",) if _schema_stub(name)]

    versions_path = settings.curated / "policy_document_versions.parquet"
    terms: list[dict] = []
    if versions_path.exists():
        from policydb.transform.normalization import stable_id

        frame = pl.read_parquet(versions_path)
        for row in frame.iter_rows(named=True):
            text = " ".join(
                str(row.get(column) or "") for column in ("title", "extracted_text")
            )
            for term in CANONICAL_TERMS:
                if term in text:
                    start = max(0, text.find(term) - 20)
                    excerpt = text[start : start + 80].replace("\n", " ")
                    terms.append(
                        {
                            "record_id": row.get("record_id"),
                            "term_id": stable_id(row.get("record_id"), term, prefix="TERM"),
                            "taxonomy_name": "topic",
                            "term_name": term,
                            "classification_source": "rule",
                            "confidence": 0.9,
                            "evidence_excerpt": excerpt,
                            "review_status": "unreviewed",
                        }
                    )
        if terms:
            frame_terms = pl.DataFrame(terms).unique(
                subset=["record_id", "term_name"], keep="first"
            )
            frame_terms.write_parquet(settings.curated / "record_terms.parquet")
    path = build_database_atomic(settings, job_id=f"PILOT_{CITIES[0]}")
    return {
        "status": "COMPLETED",
        "database": str(path),
        "terms_derived": len(terms),
        "shared_reference_copied": copied_reference,
        "schema_stubs": stubbed_tables,
    }


def stage_coverage(apply: bool) -> dict:
    if not apply:
        return {"status": "DRY_RUN"}
    settings = pilot_settings()
    report = run_coverage_audit(settings, sample_size=10)
    out = PILOT_ROOT / "evidence" / "pilot_coverage_audit.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    return {"status": "COMPLETED", "evidence": str(out), "report": report}


def stage_review_queue() -> dict:
    import polars as pl

    settings = pilot_settings()
    versions_path = settings.curated / "policy_document_versions.parquet"
    rows = []
    if versions_path.exists():
        frame = pl.read_parquet(versions_path)
        for row in frame.iter_rows(named=True):
            if row.get("parse_status") == "partial" or row.get("http_status") not in (200, None):
                rows.append(
                    {
                        "document_version_id": row.get("document_version_id"),
                        "record_id": row.get("record_id"),
                        "title": row.get("title"),
                        "parse_status": row.get("parse_status"),
                        "http_status": row.get("http_status"),
                        "reason": "needs_review",
                    }
                )
    out = PILOT_ROOT / "evidence" / "pilot_review_queue.csv"
    out.parent.mkdir(parents=True, exist_ok=True)
    with open(out, "w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["document_version_id", "record_id", "title", "parse_status", "http_status", "reason"])
        writer.writeheader()
        writer.writerows(rows)
    return {"status": "COMPLETED", "review_items": len(rows), "evidence": str(out)}


def stage_release(apply: bool) -> dict:
    if not apply:
        return {"status": "DRY_RUN", "version": RELEASE_VERSION}
    # Release is root-based (settings.root/data/releases); give the pilot its
    # own root with reference files + docs so nothing lands in the repo.
    import shutil

    release_settings = Settings(
        root=PILOT_ROOT,
        data_root_path=PILOT_ROOT,
        database_path=PILOT_ROOT / "database" / "policydb.duckdb",
        curated_path=PILOT_ROOT / "curated",
        outputs_path=PILOT_ROOT / "outputs",
        logs_path=PILOT_ROOT / "logs",
    )
    reference = PILOT_ROOT / "data" / "reference"
    reference.mkdir(parents=True, exist_ok=True)
    for name in ("cities_105.csv", "source_registry.yaml", "crawl_keywords.yaml"):
        source = REPO_ROOT / "data" / "reference" / name
        if source.exists():
            shutil.copy2(source, reference / name)
    docs = PILOT_ROOT / "docs"
    docs.mkdir(parents=True, exist_ok=True)
    for name in ("data_dictionary.md", "methodology.md"):
        source = REPO_ROOT / "docs" / name
        if source.exists():
            shutil.copy2(source, docs / name)
    path = create_release(RELEASE_VERSION, release_settings)
    return {"status": "COMPLETED", "release_path": str(path)}


STAGE_FUNCS = {
    "SOURCE_INVENTORY": stage_source_inventory,
    "CLASSIFY": stage_classify,
    "COVERAGE": stage_coverage,
    "REVIEW_QUEUE": stage_review_queue,
}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--cities", default="CITY_110000", help="comma-separated city ids")
    parser.add_argument("--window-days", type=int, default=90)
    parser.add_argument("--max-fetches", type=int, default=60)
    args = parser.parse_args()

    # Windows consoles default to cp936; summaries contain characters it cannot
    # encode. Write UTF-8 to stdout instead of crashing after a completed run.
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except (AttributeError, ValueError):
        pass

    configure_run([part for part in args.cities.split(",") if part.strip()])

    if PILOT_ROOT == PRODUCTION_ROOT:
        print("ABORT: pilot root must not equal production root", file=sys.stderr)
        return 3

    settings = pilot_settings()
    try:
        assert_isolation(settings)
    except RuntimeError as exc:
        print(f"ABORT: {exc}", file=sys.stderr)
        return 3
    if args.apply:
        PILOT_ROOT.mkdir(parents=True, exist_ok=True)
        (PILOT_ROOT / "evidence").mkdir(parents=True, exist_ok=True)

    production_hash_before = sha256(PRODUCTION_ROOT / "database" / "policydb.duckdb")
    summary: dict = {"cities": CITIES, "apply": args.apply, "resume": args.resume, "stages": {}}
    for stage in STAGES:
        if stage_done(stage, args.resume and args.apply):
            summary["stages"][stage] = {"status": "SKIPPED_RESUMED"}
            continue
        if stage == "CRAWL":
            result = stage_crawl(args.apply, args.window_days, args.max_fetches)
        elif stage == "PROMOTE":
            result = stage_promote(args.apply)
        elif stage == "DATABASE":
            result = stage_database(args.apply)
        elif stage == "RELEASE":
            result = stage_release(args.apply)
        elif stage == "COVERAGE":
            result = stage_coverage(args.apply)
        else:
            result = STAGE_FUNCS[stage]()
        summary["stages"][stage] = result
        if result["status"] in {"COMPLETED"} and args.apply:
            write_checkpoint(stage, result)
        if result["status"] in {"FAILED", "ABORTED"}:
            print(json.dumps(summary, ensure_ascii=False, indent=2))
            return 2
    production_hash_after = sha256(PRODUCTION_ROOT / "database" / "policydb.duckdb")
    summary["production_db_unchanged"] = production_hash_before == production_hash_after
    summary["production_db_sha256"] = production_hash_after
    summary_path = PILOT_ROOT / "evidence" / "pilot_e2e_summary.json"
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
