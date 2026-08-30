"""CRPD pilot city scoring — read-only evidence from production DB + registry.

Scores cities by cross-stage coverage: crawl items fetched, documents,
attachments, policy actions, dedup evidence, complete coverage windows, and
enabled official sources covering the city. Deterministic; no writes.

Usage:
  python scripts/pilot_city_score.py [--limit 10]
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import duckdb
import polars as pl

DATA_ROOT = Path(r"E:\Data Set\CRPD")
DATABASE = DATA_ROOT / "database" / "policydb.duckdb"
REGISTRY = DATA_ROOT / "curated" / "source_registry.parquet"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--limit", type=int, default=10)
    args = parser.parse_args()

    with duckdb.connect(str(DATABASE), read_only=True) as con:
        item_stats = con.execute(
            """
            WITH city_items AS (
                SELECT item_id, city_id, status FROM crawl_items WHERE city_id IS NOT NULL
            )
            SELECT
                ci.city_id,
                COUNT(DISTINCT ci.item_id)                                        AS items,
                COUNT(DISTINCT CASE WHEN ci.status = 'fetched' THEN ci.item_id END) AS fetched,
                COUNT(DISTINCT fe.item_id)                                        AS fetch_failed,
                COUNT(DISTINCT v.document_version_id)                             AS versions,
                COUNT(DISTINCT a.attachment_id)                                   AS attachments,
                COUNT(DISTINCT pa.action_id)                                      AS actions,
                COUNT(DISTINCT dd.decision_id)                                    AS dedup_decisions
            FROM city_items ci
            LEFT JOIN fetch_errors fe               ON fe.item_id = ci.item_id
            LEFT JOIN policy_document_versions v    ON v.crawl_item_id = ci.item_id
            LEFT JOIN attachments a                 ON a.parent_item_id = ci.item_id
            LEFT JOIN policy_actions pa             ON pa.record_id = v.record_id
            LEFT JOIN dedup_decisions dd            ON dd.crawl_item_id = ci.item_id
            GROUP BY ci.city_id
            """
        ).fetchall()

    registry = pl.read_parquet(REGISTRY) if REGISTRY.exists() else None
    source_counts: dict[str, dict] = {}
    if registry is not None:
        for row in registry.iter_rows(named=True):
            for city in set(row.get("city_ids") or []) | set(row.get("coverage_city_ids") or []):
                entry = source_counts.setdefault(city, {"sources": 0, "enabled": 0})
                entry["sources"] += 1
                if row.get("crawl_enabled"):
                    entry["enabled"] += 1

    rows = []
    for city_id, items, fetched, failed, versions, attachments, actions, dedup in item_stats:
        src = source_counts.get(city_id, {"sources": 0, "enabled": 0})
        score = (
            0.30 * min(1.0, fetched / 200)
            + 0.20 * min(1.0, versions / 40)
            + 0.15 * min(1.0, attachments / 10)
            + 0.15 * min(1.0, actions / 10)
            + 0.10 * min(1.0, src["enabled"] / 3)
            + 0.10 * min(1.0, dedup / 40)
        )
        rows.append(
            {
                "city_id": city_id,
                "score": round(score, 4),
                "items": items,
                "fetched": fetched,
                "fetch_failed": failed or 0,
                "versions": versions or 0,
                "attachments": attachments or 0,
                "actions": actions or 0,
                "dedup_decisions": dedup or 0,
                "registry_sources": src["sources"],
                "enabled_sources": src["enabled"],
            }
        )

    rows.sort(key=lambda r: r["score"], reverse=True)
    print(f"{'city_id':<10}{'score':>7}{'items':>7}{'fetched':>8}{'failed':>7}"
          f"{'ver':>6}{'atts':>6}{'acts':>6}{'dedup':>7}{'reg':>5}{'enab':>5}")
    for row in rows[: args.limit]:
        print(
            f"{row['city_id']:<10}{row['score']:>7.4f}{row['items']:>7}{row['fetched']:>8}"
            f"{row['fetch_failed']:>7}{row['versions']:>6}{row['attachments']:>6}"
            f"{row['actions']:>6}{row['dedup_decisions']:>7}{row['registry_sources']:>5}"
            f"{row['enabled_sources']:>5}"
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
