"""CRPD EP930 unified-framework adapter (additive, frozen scope untouched).

EP930's frozen scope and artifacts are immutable:
  - scope: 20 cities, 100 QUEUE930_ queue items, 2016-09-25..2016-10-10 window
  - frozen scope hash (user-recorded): a751e9a99d405bc91dc4a5c4a19f900b398400e1e88b43f724e034209807a90d
  - artifacts: E:\\Data Set\\CRPD\\outputs\\special_projects\\2016_930\\

The adapter never writes into the frozen output dir. A re-run uses the frozen
Episode930Pipeline machinery with a NEW stamped output directory.

Seam mapping (how the unified 12 seams map onto EP930's frozen machinery):
  discover_sources   -> episode_930 Episode930Pipeline.scope()/discovery phase
  validate_source    -> deterministic official-domain checks (_official_url, registry)
  plan_crawl         -> 930_TASK_QUEUE (frozen 100 items)
  fetch_document     -> RespectfulFetcher via the frozen pipeline (official recovery)
  extract_document   -> crawl.parser.parse_document (HTML/PDF)
  extract_actions    -> episode_930 clause splitting (_split_clauses, deterministic)
  classify_actions   -> episode_930 ActionClassification + _policy_type/_action_direction
  deduplicate        -> crawl.dedup primitives via the pipeline's DEDUP phase
  evaluate_coverage  -> episode GAP_AUDIT phase + coverage_audit.run_coverage_audit
  recover_gaps       -> episode OFFICIAL_RECOVERY phase (SourceRecoveryEngine pattern)
  promote            -> episode IMPORT phase (episode-scoped curated snapshots)
  release            -> episode FINAL_AUDIT + FINAL_EXPORT artifacts
"""
from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path

from policydb.settings import Settings

FROZEN_SCOPE_HASH = "a751e9a99d405bc91dc4a5c4a19f900b398400e1e88b43f724e034209807a90d"
FROZEN_OUTPUT = Path(r"E:\Data Set\CRPD\outputs\special_projects\2016_930")
FROZEN_CITY_COUNT = 20
FROZEN_QUEUE_ITEM_COUNT = 100
FROZEN_QUEUE_PREFIX = "QUEUE930_"


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _canonical_json(payload: object) -> str:
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str)


def verify_frozen_scope() -> dict:
    """Deterministic verification that the frozen EP930 scope is intact.

    Reports the recorded frozen hash, the adapter's own recomputed fingerprint
    over the scope inputs, and asserts the frozen shape (20 cities / 100 queue
    items / core window). Read-only; never writes into the frozen output dir.
    """
    scope_path = FROZEN_OUTPUT / "930_ANALYSIS_READY_SCOPE.json"
    queue_path = FROZEN_OUTPUT / "930_TASK_QUEUE.parquet"
    checks: dict[str, object] = {"output_dir_exists": FROZEN_OUTPUT.is_dir()}
    errors: list[str] = []

    if not scope_path.exists():
        errors.append("930_ANALYSIS_READY_SCOPE.json missing")
        scope_city_ids: set[str] = set()
    else:
        scope = json.loads(scope_path.read_text(encoding="utf-8"))
        city_ids = scope.get("city_ids") or []
        scope_city_ids = {str(city) for city in city_ids}
        checks["city_count"] = len(city_ids)
        if len(city_ids) != FROZEN_CITY_COUNT:
            errors.append(f"city count {len(city_ids)} != {FROZEN_CITY_COUNT}")
        checks["scope_version"] = scope.get("scope_version")

    if not queue_path.exists():
        errors.append("930_TASK_QUEUE.parquet missing")
    else:
        from datetime import date

        import polars as pl

        frame = pl.read_parquet(queue_path)
        # Frozen slice per the recorded scope rule: predefined SEED_CITIES
        # intersect priority=10 core-window queue.
        frozen_items = frame.filter(
            pl.col("queue_item_id").str.starts_with(FROZEN_QUEUE_PREFIX)
            & (pl.col("priority") == 10)
            & pl.col("city_id").is_in(scope_city_ids)
            & (pl.col("window_start") >= date(2016, 9, 25))
            & (pl.col("window_end") <= date(2016, 10, 10))
        )
        checks["queue_total_rows"] = frame.height
        checks["frozen_queue_items"] = frozen_items.height
        if frozen_items.height != FROZEN_QUEUE_ITEM_COUNT:
            errors.append(
                f"frozen queue items {frozen_items.height} != {FROZEN_QUEUE_ITEM_COUNT}"
            )
        # Deterministic fingerprint over the frozen queue slice (sorted by id).
        frozen_rows = frozen_items.sort("queue_item_id").to_dicts()
        checks["recomputed_queue_fingerprint"] = _sha256_bytes(
            _canonical_json(frozen_rows).encode("utf-8")
        )

    # Fingerprint over the frozen scope directory (00_SCOPE) plus the scope JSON.
    scope_dir = FROZEN_OUTPUT / "00_SCOPE"
    if scope_dir.is_dir():
        entries = sorted(
            path for path in scope_dir.rglob("*") if path.is_file()
        )
        checks["scope_dir_files"] = len(entries)
        manifest = [
            {
                "path": str(path.relative_to(FROZEN_OUTPUT)).replace("\\", "/"),
                "sha256": _sha256_bytes(path.read_bytes()),
            }
            for path in entries
        ]
        manifest.append(
            {
                "path": "930_ANALYSIS_READY_SCOPE.json",
                "sha256": _sha256_bytes(scope_path.read_bytes())
                if scope_path.exists()
                else None,
            }
        )
        checks["recomputed_scope_fingerprint"] = _sha256_bytes(
            _canonical_json(manifest).encode("utf-8")
        )
    else:
        errors.append("00_SCOPE dir missing")

    checks["recorded_frozen_scope_hash"] = FROZEN_SCOPE_HASH
    checks["frozen_scope_intact"] = not errors
    checks["errors"] = errors
    checks["checked_at"] = datetime.now(UTC).isoformat()
    return checks


def rerun_output_dir(settings: Settings | None = None, *, stamp: str | None = None) -> Path:
    """A NEW stamped output dir for any re-run; never the frozen dir."""
    settings = settings or Settings.discover()
    stamp = stamp or datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    return settings.outputs / "special_projects" / f"2016_930_rerun_{stamp}"


def run_episode_rerun(config=None, *, stamp: str | None = None) -> dict:
    """Run the frozen Episode930Pipeline into a NEW stamped output dir.

    Uses the existing frozen machinery unchanged; all writes land under
    rerun_output_dir(). Verification of the frozen scope runs first.
    """
    from policydb.episode_930 import Episode930Pipeline

    settings = Settings.discover()
    verify = verify_frozen_scope()
    if not verify["frozen_scope_intact"]:
        return {"status": "ABORTED", "verify": verify}
    output = rerun_output_dir(settings, stamp=stamp)
    pipeline = Episode930Pipeline(settings, config=config, output=output)
    scope = pipeline.scope()
    return {
        "status": "STARTED",
        "verify": verify,
        "output": str(output),
        "scope": scope,
    }


if __name__ == "__main__":
    import sys

    report = verify_frozen_scope()
    print(json.dumps(report, ensure_ascii=False, indent=2))
    sys.exit(0 if report["frozen_scope_intact"] else 2)
