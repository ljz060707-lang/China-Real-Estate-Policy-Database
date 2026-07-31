from __future__ import annotations

import json
import os
from collections import Counter
from datetime import UTC, datetime, timedelta

import polars as pl

from policydb.settings import Settings
from policydb.source_slots import audit_525


def _parse_time(value: object) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)
    except ValueError:
        return None


def repair_recipe(status: str, *, city: str | None = None, run_id: str | None = None) -> dict:
    city_option = f' --city "{city}"' if city else ""
    recipes = {
        "tun_intercepted": {
            "automatic": False,
            "action": "Add gov.cn to Vortex DIRECT and Fake-IP exclusions, then re-run network audit.",
            "command": "policydb network audit-sources --enabled-only",
        },
        "source_incomplete": {
            "automatic": False,
            "action": "Discover, verify, promote, then strictly enable a reusable official entry.",
            "command": f"policydb sources discover-city{city_option}",
        },
        "partial_network": {
            "automatic": True,
            "action": "Retry only failed leaf shards after the route is healthy.",
            "command": f"policydb crawl exhaustive-resume{city_option} --retry-errors",
        },
        "partial_parser": {
            "automatic": False,
            "action": "Inspect saved HTML evidence and add or repair the source parser adapter.",
            "command": f"policydb progress status{city_option}",
        },
        "partial_temporal": {
            "automatic": False,
            "action": "Inspect unknown or out-of-window dates before retrying the affected leaf shard.",
            "command": f"policydb progress status{city_option}",
        },
        "partial_cap": {
            "automatic": True,
            "action": "Resume generated child shards; split parents are excluded from completion metrics.",
            "command": f"policydb crawl exhaustive-resume{city_option}",
        },
        "ai_pending": {
            "automatic": True,
            "action": "Run AI classification and verification for the recorded crawl run.",
            "command": f"policydb ai classify --run-id {run_id}" if run_id else "policydb ai classify",
        },
        "stalled": {
            "automatic": False,
            "action": "Inspect the last progress event before retrying; never start a second writer.",
            "command": f"policydb progress status{city_option}",
        },
    }
    return {"status": status, **recipes.get(status, recipes["stalled"])}


def supervisor_status(
    settings: Settings | None = None, *, stale_minutes: int = 30
) -> dict:
    settings = settings or Settings.discover()
    now = datetime.now(UTC)
    shards_path = settings.curated / "crawl_shards.parquet"
    shards = pl.read_parquet(shards_path) if shards_path.exists() else pl.DataFrame()
    status_counts = Counter(
        str(value) for value in shards["status"].to_list()
    ) if shards.height else Counter()
    issues: list[dict] = []
    for row in shards.iter_rows(named=True):
        status = str(row.get("status") or "unknown")
        started = _parse_time(row.get("started_at"))
        updated = _parse_time(row.get("updated_at"))
        if status == "pending" and started and (now - (updated or started)) > timedelta(minutes=stale_minutes):
            issues.append(
                {
                    "kind": "stalled",
                    "shard_id": row.get("shard_id"),
                    "city": row.get("city_name"),
                    "run_id": row.get("checkpoint"),
                }
            )
        elif status in {
            "source_incomplete",
            "partial_network",
            "partial_parser",
            "partial_temporal",
            "partial_cap",
            "partial_archive",
            "failed",
        }:
            issues.append(
                {
                    "kind": status,
                    "shard_id": row.get("shard_id"),
                    "city": row.get("city_name"),
                    "run_id": row.get("checkpoint"),
                }
            )
        if int(row.get("ai_pending_count") or 0) > 0:
            issues.append(
                {
                    "kind": "ai_pending",
                    "shard_id": row.get("shard_id"),
                    "city": row.get("city_name"),
                    "run_id": row.get("checkpoint"),
                }
            )
    network_path = settings.outputs / "acceptance" / "network_source_audit.json"
    network = json.loads(network_path.read_text(encoding="utf-8")) if network_path.exists() else {}
    if int(network.get("status_counts", {}).get("tun_intercepted", 0)):
        issues.insert(
            0,
            {
                "kind": "tun_intercepted",
                "source_count": network["status_counts"]["tun_intercepted"],
            },
        )
    source_audit = audit_525(settings)
    unique_recipes: dict[tuple[str, str | None, str | None], dict] = {}
    for issue in issues:
        kind = str(issue["kind"])
        key = (
            kind,
            issue.get("city"),
            issue.get("run_id") if kind == "ai_pending" else None,
        )
        unique_recipes.setdefault(
            key,
            repair_recipe(key[0], city=key[1], run_id=key[2]),
        )
    report = {
        "created_at": now.isoformat(),
        "stale_minutes": stale_minutes,
        "healthy": not issues and source_audit["slots_unresolved"] == 0,
        "shard_status_counts": dict(sorted(status_counts.items())),
        "issue_count": len(issues),
        "issue_counts": dict(sorted(Counter(item["kind"] for item in issues).items())),
        "source_coverage": source_audit,
        "repair_recipes": list(unique_recipes.values()),
    }
    output = settings.outputs / "supervisor" / "latest.json"
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_suffix(".json.tmp")
    temporary.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(temporary, output)
    report["output"] = str(output)
    return report
