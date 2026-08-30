"""CRPD Forward Watch — bounded recent-window check over the production root.

Runs the unified crawl with a short window (default last 3 days) across the
103-city universe: list pages re-checked via ETag/Last-Modified + resume,
new documents flow through parse -> deterministic extract_actions -> promote.
Updates CRPD_FORWARD_WATCH_STATE.json. Never re-crawls history.
"""
from __future__ import annotations

import argparse
import csv
import json
import sys
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import polars as pl

REPO = Path(r"E:\policy-database")
CURRENT = Path(r"E:\Data Set\CRPD\production\current")
OPS = Path(r"E:\Data Set\CRPD\production\ops")
UNIVERSE_EXCLUDED = {"CITY_320583", "CITY_330282"}


def settings():
    from policydb.settings import Settings

    return Settings(
        root=REPO,
        data_root_path=CURRENT,
        database_path=CURRENT / "database" / "policydb.duckdb",
        curated_path=CURRENT / "curated",
        outputs_path=CURRENT / "outputs",
        logs_path=CURRENT / "logs",
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--days", type=int, default=3)
    parser.add_argument("--max-fetches", type=int, default=80)
    args = parser.parse_args()
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except (AttributeError, ValueError):
        pass

    with open(REPO / "data" / "reference" / "cities_105.csv", encoding="utf-8-sig", newline="") as handle:
        cities = {str(r["city_id"]) for r in csv.DictReader(handle)}
    universe = sorted(cities - UNIVERSE_EXCLUDED)
    assert len(universe) == 103

    from policydb.crawl.service import CrawlService
    from policydb.ingest.promote_versions import promote_document_versions
    from policydb.jobs.manager import PolicyWriteLock
    from policydb.jobs.models import CrawlJobRequest

    started = datetime.now(UTC)
    request = CrawlJobRequest(
        mode="official_update",
        cities=universe,
        start_date=date.today() - timedelta(days=args.days),
        end_date=date.today(),
        max_fetches=args.max_fetches,
        max_candidates_total=800,
        max_candidates_per_source=12,
        max_pages_per_source=4,
        max_attachment_attempts=1,
        run_glm=False,
        run_verification=False,
        enabled_only=True,
        official_first=True,
        resume=True,
    )
    result = CrawlService(settings()).execute(request)
    metrics = result.get("metrics", {})
    run_id = result.get("run_id", "")

    # deterministic actions over versions created by this watch run
    new_actions = 0
    versions_path = CURRENT / "curated" / "policy_document_versions.parquet"
    if versions_path.exists():
        from policydb.intensity.rules import DeterministicPolicyRules

        rules = DeterministicPolicyRules(REPO / "data" / "reference")
        frame = pl.read_parquet(versions_path)
        delta = frame.filter(pl.col("created_at") >= started.isoformat()) if "created_at" in frame.columns else None
        if delta is not None:
            for row in delta.iter_rows(named=True):
                new_actions += len(
                    rules.extract_actions(
                        record_id=str(row.get("record_id") or "R"),
                        text=str(row.get("extracted_text") or ""),
                        title=str(row.get("title") or None),
                        official_status="official",
                    )
                )
    promoted = {}
    if run_id:
        with PolicyWriteLock(settings(), "FORWARD_WATCH"):
            promoted = promote_document_versions(settings(), run_id=run_id, apply=True)

    state = {
        "updated_at": datetime.now(UTC).isoformat(),
        "last_run_started": started.isoformat(),
        "window_days": args.days,
        "universe_cities": len(universe),
        "run_id": run_id,
        "metrics": metrics,
        "new_actions_detected": new_actions,
        "promoted": promoted,
        "state": "ACTIVE",
    }
    OPS.mkdir(parents=True, exist_ok=True)
    (OPS / "CRPD_FORWARD_WATCH_STATE.json").write_text(
        json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(json.dumps(state, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
