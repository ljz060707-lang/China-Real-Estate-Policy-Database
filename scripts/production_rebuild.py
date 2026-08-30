"""CRPD Production Root full rebuild via the unified runner.

BUILD -> VALIDATE -> PROMOTE inside E:\\Data Set\\CRPD\\production_rebuild\\<RUN_ID>\\

- Canonical city universe: the 103 cities with registered sources (105-city
  matrix minus 昆山市/慈溪市 which have ZERO registered sources — verified).
- Reuse-first: the production curated evidence layer (records/versions/dedup/
  attachments/crawl_items) is copied into the rebuild root as baseline; the
  unified crawl re-checks with resume=True (re-check evidence preserved,
  versions deduped by content hash); only bounded new network work runs.
- Deterministic extract_actions fills the action layer (no AI dependency).
- Database = single writer (PolicyWriteLock), build_database_atomic.

Usage:
  python scripts/production_rebuild.py                    # dry run
  python scripts/production_rebuild.py --apply            # execute
  python scripts/production_rebuild.py --apply --resume   # resume unfinished
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import shutil
import sys
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import polars as pl

from policydb.coverage_audit import run_coverage_audit
from policydb.crawl.service import CrawlService
from policydb.export.release import create_release
from policydb.ingest.promote_versions import promote_document_versions
from policydb.jobs.manager import PolicyWriteLock
from policydb.jobs.models import CrawlJobRequest
from policydb.settings import Settings
from policydb.source_quality import validate_registry

REPO = Path(__file__).resolve().parents[1]
PRODUCTION_ROOT = Path(r"E:\Data Set\CRPD")
PRODUCTION_DB = PRODUCTION_ROOT / "database" / "policydb.duckdb"
DEFAULT_REBUILD_ROOT = PRODUCTION_ROOT / "production_rebuild" / f"CRPD_REBUILD_{datetime.now(UTC):%Y%m%dT%H%M%SZ}"
REBUILD_ROOT = DEFAULT_REBUILD_ROOT
RELEASE_VERSION = "CRPD_RELEASE_1.0.0"

CANONICAL_CITIES = Path(REPO / "data" / "reference" / "cities_105.csv")
UNREGISTERED_CITIES = {"CITY_320583", "CITY_330282"}  # 昆山市/慈溪市 — zero registered sources

STAGES = [
    "SETUP",
    "SOURCE_GOVERNANCE",
    "CRAWL",
    "CLASSIFY_ACTIONS",
    "COLLECTIONS",
    "PROMOTE",
    "DATABASE",
    "COVERAGE",
    "REVIEW_QUEUE",
    "RELEASE",
    "DIFF",
]

CANONICAL_TERMS = [
    "限购", "限售", "商业住房贷款", "限贷", "公积金", "购房补贴",
    "人才住房", "城市更新", "城中村改造", "老旧小区改造", "危旧房改造",
]


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def checkpoints_dir() -> Path:
    return REBUILD_ROOT / "checkpoints"


def checkpoint_path(stage: str) -> Path:
    return checkpoints_dir() / f"rebuild_{stage}.json"


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
    temp.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    temp.replace(path)


def rebuild_settings() -> Settings:
    return Settings(
        root=REPO,
        data_root_path=REBUILD_ROOT,
        database_path=REBUILD_ROOT / "database" / "policydb.duckdb",
        curated_path=REBUILD_ROOT / "curated",
        raw_path=REBUILD_ROOT / "raw",
        research_path=REBUILD_ROOT / "research",
        outputs_path=REBUILD_ROOT / "outputs",
        logs_path=REBUILD_ROOT / "logs",
        automation_path=REBUILD_ROOT / "automation",
        control_path=REBUILD_ROOT / "control",
        runtime_path=REBUILD_ROOT / "runtime",
        cache_path=REBUILD_ROOT / "cache",
        temp_path=REBUILD_ROOT / "temp",
        test_artifacts_path=REBUILD_ROOT / "test_artifacts",
        dashboard_path=REBUILD_ROOT / "dashboard",
        backups_path=REBUILD_ROOT / "backups",
        dashboard_runtime_path=REBUILD_ROOT / "runtime" / "dashboard",
    )


def assert_isolation(settings: Settings) -> None:
    leaked = {
        name: str(path)
        for name, path in {
            "database": settings.database,
            "curated": settings.curated,
            "outputs": settings.outputs,
            "jobs": settings.jobs,
            "raw": settings.raw,
            "temp": settings.temp,
        }.items()
        if REBUILD_ROOT not in path.parents
    }
    if leaked:
        raise RuntimeError(f"rebuild isolation violated: {leaked}")


def city_universe() -> list[str]:
    """103-city canonical universe: 105 matrix minus the 2 unregistered."""
    with open(CANONICAL_CITIES, encoding="utf-8-sig", newline="") as handle:
        rows = list(csv.DictReader(handle))
    all_ids = {str(row["city_id"]) for row in rows}
    assert len(all_ids) == 105, f"cities_105.csv has {len(all_ids)} rows"
    cities = sorted(all_ids - UNREGISTERED_CITIES)
    assert len(cities) == 103, f"rebuild universe = {len(cities)}"
    return cities


def stage_setup(apply: bool) -> dict:
    if not apply:
        return {"status": "DRY_RUN", "rebuild_root": str(REBUILD_ROOT)}
    REBUILD_ROOT.mkdir(parents=True, exist_ok=True)
    curated = REBUILD_ROOT / "curated"
    curated.mkdir(parents=True, exist_ok=True)
    # Reuse-first: copy the production curated evidence layer (read-only source).
    copied = 0
    for path in sorted((PRODUCTION_ROOT / "curated").glob("*.parquet")):
        shutil.copy2(path, curated / path.name)
        copied += 1
    return {"status": "COMPLETED", "evidence_baseline_copied": copied,
            "rebuild_root": str(REBUILD_ROOT)}


def stage_source_governance(apply: bool) -> dict:
    settings = rebuild_settings()
    report = validate_registry(settings)
    cities = city_universe()
    evidence = REBUILD_ROOT / "evidence" / "rebuild_source_inventory.csv"
    if apply:
        evidence.parent.mkdir(parents=True, exist_ok=True)
        with open(evidence, "w", encoding="utf-8-sig", newline="") as handle:
            writer = csv.writer(handle)
            writer.writerow(["city_id"])
            writer.writerows([[city] for city in cities])
    return {"status": "COMPLETED" if apply else "DRY_RUN", "registry": report,
            "universe_cities": len(cities), "evidence": str(evidence)}


def stage_crawl(apply: bool, window_days: int, max_fetches: int) -> dict:
    settings = rebuild_settings()
    today = date.today()
    request = CrawlJobRequest(
        mode="official_update",
        cities=city_universe(),
        start_date=today - timedelta(days=window_days),
        end_date=today,
        max_fetches=max_fetches,
        max_candidates_total=4000,
        max_candidates_per_source=25,
        max_pages_per_source=10,
        max_attachment_attempts=2,
        run_glm=False,
        run_verification=False,
        enabled_only=True,
        official_first=True,
        resume=True,
    )
    if not apply:
        return {"status": "DRY_RUN", "estimate": CrawlService(settings).estimate(request)}

    progress_log = REBUILD_ROOT / "logs" / "crawl_progress.jsonl"
    progress_log.parent.mkdir(parents=True, exist_ok=True)

    def progress(stage, current, total, message, counters=None) -> None:
        payload = {
            "at": datetime.now(UTC).isoformat(),
            "stage": stage,
            "current": current,
            "total": total,
            "message": message,
            "counters": counters or {},
        }
        with progress_log.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(payload, ensure_ascii=False) + "\n")

    result = CrawlService(settings).execute(request, progress=progress)
    return {"status": "COMPLETED", "run_id": result["run_id"], "metrics": result["metrics"],
            "progress_log": str(progress_log)}


def stage_classify_actions(apply: bool) -> dict:
    """Deterministic action extraction + rule classification over versions."""
    if not apply:
        return {"status": "DRY_RUN"}
    from policydb.intensity.rules import DeterministicPolicyRules, classify_text_completeness
    from policydb.taxonomy_v2 import classify_action

    settings = rebuild_settings()
    rules = DeterministicPolicyRules(REPO / "data" / "reference")
    versions_path = settings.curated / "policy_document_versions.parquet"
    actions: list[dict] = []
    if versions_path.exists():
        frame = pl.read_parquet(versions_path)
        for row in frame.iter_rows(named=True):
            text = str(row.get("extracted_text") or "")
            extracted = rules.extract_actions(
                record_id=str(row.get("record_id") or "R"),
                document_version_id=str(row.get("document_version_id") or None),
                text=text,
                title=str(row.get("title") or None),
                official_status="official",
            )
            completeness = classify_text_completeness(
                text, official_status="official", title=str(row.get("title") or None)
            )
            for action in extracted:
                primary, secondary, mechanism, confidence, method = classify_action(
                    action.instrument, action.clause_text
                )
                actions.append(
                    {
                        "action_id": action.action_id,
                        "record_id": action.record_id,
                        "document_version_id": action.document_version_id,
                        "clause_id": action.clause_id,
                        "clause_text": action.clause_text,
                        "evidence_start": action.evidence_start,
                        "evidence_end": action.evidence_end,
                        "instrument": action.instrument,
                        "direction": action.direction,
                        "action_status": "active" if action.formal_eligible else "provisional",
                        "text_completeness": completeness,
                        "formal_eligible": action.formal_eligible,
                        "evidence_text": action.evidence_text,
                        "primary_category": primary or None,
                        "secondary_category": secondary or None,
                        "instrument_type": mechanism or None,
                        "confidence": confidence,
                        "decision_reason": method,
                        "negation_terms": json.dumps(action.negation_terms, ensure_ascii=False),
                        "mentions": json.dumps(action.mentions, ensure_ascii=False),
                        "extraction_method": "deterministic_rule",
                        "rules_version": rules.version,
                        "created_at": datetime.now(UTC).isoformat(),
                        "updated_at": datetime.now(UTC).isoformat(),
                    }
                )
    if actions:
        pl.DataFrame(actions).write_parquet(settings.curated / "policy_actions.parquet")
    return {"status": "COMPLETED", "actions_extracted": len(actions)}


def stage_collections(apply: bool) -> dict:
    """Deterministic collection layer (existing build_collection_layer machinery).

    Copies staging cells + reference YAMLs into the rebuild root (so the stage
    is rerunnable after repo cleanup) and classifies every record into the
    taxonomy collections — closing the release-validation coverage gap.
    """
    if not apply:
        return {"status": "DRY_RUN"}
    from policydb.transform.collections import build_collection_layer

    staging_source = REPO / "data" / "staging" / "excel"
    staging_target = REBUILD_ROOT / "data" / "staging" / "excel"
    if staging_source.is_dir():
        staging_target.mkdir(parents=True, exist_ok=True)
        for parquet in staging_source.glob("*.parquet"):
            if not (staging_target / parquet.name).exists():
                shutil.copy2(parquet, staging_target / parquet.name)
    reference = REBUILD_ROOT / "data" / "reference"
    reference.mkdir(parents=True, exist_ok=True)
    for name in ("policy_taxonomy_v2.yaml", "cities_105.csv", "source_registry.yaml",
                 "crawl_keywords.yaml", "policy_action_patterns.yaml",
                 "policy_calibration_scales.yaml", "policy_binding_lexicon.yaml"):
        source = REPO / "data" / "reference" / name
        if source.exists() and not (reference / name).exists():
            shutil.copy2(source, reference / name)
    collection_settings = Settings(
        root=REBUILD_ROOT,
        data_root_path=REBUILD_ROOT,
        curated_path=REBUILD_ROOT / "curated",
        outputs_path=REBUILD_ROOT / "outputs",
        logs_path=REBUILD_ROOT / "logs",
    )
    report = build_collection_layer(collection_settings)
    return {"status": "COMPLETED", "collections": report}


def stage_promote(apply: bool) -> dict:
    settings = rebuild_settings()
    checkpoint = load_checkpoint("CRAWL")
    run_id = checkpoint.get("run_id") if checkpoint else None
    if not apply:
        return {"status": "DRY_RUN", "run_id": run_id}
    if not run_id:
        return {"status": "SKIPPED_NO_RUN_ID"}
    with PolicyWriteLock(settings, f"REBUILD_PROMOTE_{run_id}"):
        result = promote_document_versions(settings, run_id=run_id, apply=True)
    return {"status": "COMPLETED", "run_id": run_id, "promote": result}


def stage_database(apply: bool) -> dict:
    if not apply:
        return {"status": "DRY_RUN"}
    from policydb.query.database import build_database_atomic

    settings = rebuild_settings()
    versions_path = settings.curated / "policy_document_versions.parquet"
    terms: list[dict] = []
    if versions_path.exists():
        from policydb.transform.normalization import stable_id

        for row in pl.read_parquet(versions_path).iter_rows(named=True):
            record_id = row.get("record_id")
            if not record_id:
                continue  # unpromoted versions carry no record_id
            text = " ".join(str(row.get(c) or "") for c in ("title", "extracted_text"))
            for term in CANONICAL_TERMS:
                if term in text:
                    start = max(0, text.find(term) - 20)
                    terms.append(
                        {
                            "record_id": record_id,
                            "term_id": stable_id(record_id, term, prefix="TERM"),
                            "taxonomy_name": "topic",
                            "term_name": term,
                            "classification_source": "rule",
                            "confidence": 0.9,
                            "evidence_excerpt": text[start : start + 80].replace("\n", " "),
                            "review_status": "unreviewed",
                        }
                    )
        if terms:
            pl.DataFrame(terms, infer_schema_length=None).unique(
                subset=["record_id", "term_name"], keep="first"
            ).write_parquet(settings.curated / "record_terms.parquet")
    path = build_database_atomic(settings, job_id="CRPD_REBUILD")
    return {"status": "COMPLETED", "database": str(path), "terms_derived": len(terms)}


def stage_coverage(apply: bool) -> dict:
    if not apply:
        return {"status": "DRY_RUN"}
    report = run_coverage_audit(rebuild_settings(), sample_size=10)
    out = REBUILD_ROOT / "evidence" / "rebuild_coverage_audit.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    return {"status": "COMPLETED", "evidence": str(out)}


def stage_review_queue(apply: bool) -> dict:
    if not apply:
        return {"status": "DRY_RUN"}
    settings = rebuild_settings()
    rows: list[dict] = []
    versions_path = settings.curated / "policy_document_versions.parquet"
    if versions_path.exists():
        for row in pl.read_parquet(versions_path).iter_rows(named=True):
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
    out = REBUILD_ROOT / "evidence" / "rebuild_review_queue.csv"
    out.parent.mkdir(parents=True, exist_ok=True)
    with open(out, "w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(
            handle, fieldnames=["document_version_id", "record_id", "title", "parse_status", "http_status", "reason"]
        )
        writer.writeheader()
        writer.writerows(rows)
    return {"status": "COMPLETED", "review_items": len(rows)}


def stage_release(apply: bool) -> dict:
    if not apply:
        return {"status": "DRY_RUN", "version": RELEASE_VERSION}
    release_settings = Settings(
        root=REBUILD_ROOT,
        data_root_path=REBUILD_ROOT,
        database_path=REBUILD_ROOT / "database" / "policydb.duckdb",
        curated_path=REBUILD_ROOT / "curated",
        outputs_path=REBUILD_ROOT / "outputs",
        logs_path=REBUILD_ROOT / "logs",
    )
    reference = REBUILD_ROOT / "data" / "reference"
    reference.mkdir(parents=True, exist_ok=True)
    for name in ("cities_105.csv", "source_registry.yaml", "crawl_keywords.yaml", "policy_action_patterns.yaml"):
        source = REPO / "data" / "reference" / name
        if source.exists():
            shutil.copy2(source, reference / name)
    docs = REBUILD_ROOT / "docs"
    docs.mkdir(parents=True, exist_ok=True)
    for name in ("data_dictionary.md", "methodology.md"):
        source = REPO / "docs" / name
        if source.exists():
            shutil.copy2(source, docs / name)
    # Excel staging cells are release-validation evidence; copy from the repo
    # staging dir (moves to E during final storage cleanup).
    staging_source = REPO / "data" / "staging" / "excel"
    if staging_source.is_dir():
        staging_target = REBUILD_ROOT / "data" / "staging" / "excel"
        staging_target.mkdir(parents=True, exist_ok=True)
        for parquet in staging_source.glob("*.parquet"):
            shutil.copy2(parquet, staging_target / parquet.name)
    path = create_release(RELEASE_VERSION, release_settings)
    return {"status": "COMPLETED", "release_path": str(path)}


def stage_diff() -> dict:
    """Compare rebuild evidence vs production (EXPECTED/IMPROVEMENT/REGRESSION/UNKNOWN)."""
    prod = PRODUCTION_ROOT / "curated"
    rebuild = REBUILD_ROOT / "curated"
    rows: list[dict] = []

    def count(path: Path, table: str) -> int:
        if not path.exists():
            return -1
        try:
            return pl.read_parquet(path).height
        except Exception:  # noqa: BLE001
            return -1

    tables = [
        "records", "policy_document_versions", "dedup_decisions",
        "attachments", "crawl_items", "policy_actions", "record_terms",
    ]
    for table in tables:
        old_n = count(prod / f"{table}.parquet", table)
        new_n = count(rebuild / f"{table}.parquet", table)
        delta = (new_n - old_n) if old_n >= 0 and new_n >= 0 else None
        if table in {"records", "policy_actions"}:
            classification = "EXPECTED" if delta is not None and delta >= 0 else "REGRESSION"
        else:
            classification = "EXPECTED"
        if table == "policy_actions" and old_n == 0:
            classification = "IMPROVEMENT" if new_n and new_n > 0 else "UNKNOWN"
        rows.append(
            {
                "table": table,
                "production_count": old_n,
                "rebuild_count": new_n,
                "delta": delta,
                "classification": classification,
                "note": "",
            }
        )
    # city universe comparison
    rows.append(
        {
            "table": "city_universe",
            "production_count": 103,
            "rebuild_count": len(city_universe()),
            "delta": 0,
            "classification": "EXPECTED",
            "note": "105 matrix minus 昆山市/慈溪市 (zero registered sources)",
        }
    )
    out = REBUILD_ROOT / "evidence" / "CRPD_PRODUCTION_REBUILD_DIFF.csv"
    out.parent.mkdir(parents=True, exist_ok=True)
    with open(out, "w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["table", "production_count", "rebuild_count", "delta", "classification", "note"])
        writer.writeheader()
        writer.writerows(rows)
    unknowns = [r for r in rows if r["classification"] == "UNKNOWN"]
    return {"status": "COMPLETED", "diff_rows": len(rows), "unknowns": len(unknowns),
            "evidence": str(out)}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--root", default=None, help="target an existing rebuild root (resume into it)")
    parser.add_argument("--window-days", type=int, default=180)
    parser.add_argument("--max-fetches", type=int, default=800)
    args = parser.parse_args()
    global REBUILD_ROOT
    if args.root:
        REBUILD_ROOT = Path(args.root).resolve()
        if not REBUILD_ROOT.is_dir():
            print(f"ABORT: --root not a directory: {REBUILD_ROOT}", file=sys.stderr)
            return 3
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except (AttributeError, ValueError):
        pass

    if REBUILD_ROOT == PRODUCTION_ROOT:
        print("ABORT: rebuild root must not equal production root", file=sys.stderr)
        return 3
    if args.apply:
        REBUILD_ROOT.mkdir(parents=True, exist_ok=True)
        (REBUILD_ROOT / "evidence").mkdir(parents=True, exist_ok=True)
    settings = rebuild_settings()
    try:
        assert_isolation(settings)
    except RuntimeError as exc:
        print(f"ABORT: {exc}", file=sys.stderr)
        return 3

    production_hash_before = sha256(PRODUCTION_DB)
    summary: dict = {
        "rebuild_root": str(REBUILD_ROOT),
        "universe_cities": len(city_universe()),
        "apply": args.apply,
        "resume": args.resume,
        "stages": {},
    }
    for stage in STAGES:
        if args.apply and args.resume and load_checkpoint(stage) is not None:
            summary["stages"][stage] = {"status": "SKIPPED_RESUMED"}
            continue
        if stage == "SETUP":
            result = stage_setup(args.apply)
        elif stage == "SOURCE_GOVERNANCE":
            result = stage_source_governance(args.apply)
        elif stage == "CRAWL":
            result = stage_crawl(args.apply, args.window_days, args.max_fetches)
        elif stage == "CLASSIFY_ACTIONS":
            result = stage_classify_actions(args.apply)
        elif stage == "COLLECTIONS":
            result = stage_collections(args.apply)
        elif stage == "PROMOTE":
            result = stage_promote(args.apply)
        elif stage == "DATABASE":
            result = stage_database(args.apply)
        elif stage == "COVERAGE":
            result = stage_coverage(args.apply)
        elif stage == "REVIEW_QUEUE":
            result = stage_review_queue(args.apply)
        elif stage == "RELEASE":
            result = stage_release(args.apply)
        else:
            result = stage_diff()
        summary["stages"][stage] = result
        if args.apply and result["status"] == "COMPLETED":
            write_checkpoint(stage, result)
        if result["status"] in {"FAILED", "ABORTED"}:
            print(json.dumps(summary, ensure_ascii=False, indent=2))
            return 2
    production_hash_after = sha256(PRODUCTION_DB)
    summary["production_db_unchanged"] = production_hash_before == production_hash_after
    summary["production_db_sha256"] = production_hash_after
    summary_path = REBUILD_ROOT / "evidence" / "rebuild_summary.json"
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
