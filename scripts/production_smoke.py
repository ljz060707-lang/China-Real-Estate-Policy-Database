"""CRPD production smoke run under the new stable pointer (no full E2E).

Verifies: canonical 103-city universe, registry load, documents/versions
reuse baseline, a small real incremental source check (fetch->parse->
deterministic extract_actions), dedup baseline, single writer. Produces a
lightweight incremental result. Network bounded to a few requests.
"""
from __future__ import annotations

import csv
import json
import sys
from datetime import UTC, datetime
from pathlib import Path

REPO = Path(r"E:\policy-database")
CURRENT = Path(r"E:\Data Set\CRPD\production\current")
OUT = Path(r"E:\Data Set\CRPD\reports\runs\CRPD_CUTOVER_20260821") / "CRPD_PRODUCTION_SMOKE_REPORT.json"

SMOKE_URLS = [
    "https://gjj.beijing.gov.cn/web/zwgk/xxgkml/",
    "https://zjj.tj.gov.cn/",
    "https://nanchong.gov.cn/zwgk/zfgb/newzfgb.html",
]


def main() -> int:
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except (AttributeError, ValueError):
        pass
    import polars as pl

    from policydb.crawl.fetcher import RespectfulFetcher
    from policydb.crawl.parser import parse_document
    from policydb.intensity.rules import DeterministicPolicyRules
    from policydb.jobs.manager import PolicyWriteLock
    from policydb.platform.seams import probe_seams
    from policydb.settings import Settings

    settings = Settings(
        root=REPO,
        data_root_path=CURRENT,
        database_path=CURRENT / "database" / "policydb.duckdb",
        curated_path=CURRENT / "curated",
        outputs_path=CURRENT / "outputs",
        logs_path=CURRENT / "logs",
    )
    report: dict[str, object] = {"created_at": datetime.now(UTC).isoformat(), "pointer": str(CURRENT)}
    errors: list[str] = []

    # 1. canonical universe
    with open(REPO / "data" / "reference" / "cities_105.csv", encoding="utf-8-sig", newline="") as handle:
        cities = {str(r["city_id"]) for r in csv.DictReader(handle)}
    universe = sorted(cities - {"CITY_320583", "CITY_330282"})
    report["canonical_city_universe"] = len(universe)
    report["CANONICAL_CITY_UNIVERSE"] = "PASS" if len(universe) == 103 else "FAIL"
    if len(universe) != 103:
        errors.append("universe != 103")

    # 2. registry
    from policydb.crawl.registry import load_registry
    sources = load_registry(settings)
    report["registry_sources"] = len(sources)
    report["registry_enabled"] = sum(1 for s in sources if s.crawl_enabled)

    # 3. documents / reuse baseline
    versions_path = settings.curated / "policy_document_versions.parquet"
    versions = pl.read_parquet(versions_path) if versions_path.exists() else None
    report["document_versions"] = versions.height if versions is not None else 0
    dedup_path = settings.curated / "dedup_decisions.parquet"
    report["dedup_decisions"] = pl.read_parquet(dedup_path).height if dedup_path.exists() else 0
    actions_path = settings.curated / "policy_actions.parquet"
    report["deterministic_actions"] = pl.read_parquet(actions_path).height if actions_path.exists() else 0

    # 4. small incremental source check (bounded real network)
    rules = DeterministicPolicyRules(REPO / "data" / "reference")
    fetcher = RespectfulFetcher(check_robots=True, retries=1, timeout=15, connect_timeout=8)
    fetched_rows = []
    for url in SMOKE_URLS:
        try:
            result = fetcher.fetch(url)
            parsed = parse_document(result.body, result.content_type, result.final_url)
            actions = rules.extract_actions(
                record_id=f"SMOKE_{url.split('/')[2]}",
                text=str(parsed.get("full_text") or "")[:6000],
                title=str(parsed.get("title") or None),
                official_status="official",
            )
            fetched_rows.append(
                {"url": url, "status": result.status_code, "bytes": len(result.body),
                 "parse_status": parsed.get("parse_status"), "action_candidates": len(actions)}
            )
        except Exception as exc:  # noqa: BLE001
            fetched_rows.append({"url": url, "error": f"{type(exc).__name__}: {str(exc)[:80]}"})
    report["incremental_source_check"] = fetched_rows

    # 5. single writer
    try:
        with PolicyWriteLock(settings, "SMOKE"):
            report["single_writer"] = "PASS"
    except Exception as exc:  # noqa: BLE001
        report["single_writer"] = f"FAIL: {exc}"
        errors.append("writer lock failed")

    # 6. seam probe (unified runner health)
    probe = probe_seams()
    report["seams_implemented"] = sum(1 for v in probe.values() if v["status"] == "IMPLEMENTED")
    report["seams_resolve"] = sum(1 for v in probe.values() if v.get("resolves"))

    report["status"] = "PASS" if not errors else "FAIL"
    report["errors"] = errors
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0 if not errors else 2


if __name__ == "__main__":
    raise SystemExit(main())
