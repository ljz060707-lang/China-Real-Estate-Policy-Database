from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from urllib.parse import urlsplit

import polars as pl
import yaml

from policydb.config.providers import SearchProvider, build_search_fallback
from policydb.crawl.models import RegisteredSource
from policydb.crawl.registry import load_registry, save_registry_atomic
from policydb.scope import load_cities_105
from policydb.settings import Settings

REQUIRED_ROLES = (
    "municipal_government",
    "government_gazette",
    "housing_department",
    "provident_fund_center",
    "natural_resources_department",
)
ROLE_TERMS = {
    "municipal_government": "人民政府",
    "government_gazette": "政府公报",
    "housing_department": "住房和城乡建设局",
    "provident_fund_center": "住房公积金管理中心",
    "natural_resources_department": "自然资源和规划局",
    "development_reform_department": "发展和改革委员会",
    "local_financial_regulator": "地方金融管理局",
    "tax_department": "税务局",
    "public_resource_trading_center": "公共资源交易中心",
    "administrative_approval_department": "行政审批局",
    "urban_renewal_or_expropriation_department": "城市更新 征收",
}


def _official_domain(url: str) -> bool:
    host = (urlsplit(url).hostname or "").lower().removeprefix("www.")
    return host == "gov.cn" or host.endswith(".gov.cn")


def _source_id(city_id: str, role: str, domain: str) -> str:
    value = f"{city_id}|{role}|{domain}".encode()
    return "SRC_" + hashlib.sha256(value).hexdigest()[:16].upper()


def load_source_requirements(settings: Settings | None = None) -> dict:
    settings = settings or Settings.discover()
    path = settings.root / "data/reference/city_source_requirements.yaml"
    if not path.exists():
        raise FileNotFoundError(f"City source requirement matrix is missing: {path}")
    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}


def discover_city_sources(
    city: str,
    settings: Settings | None = None,
    *,
    provider: SearchProvider | None = None,
    roles: list[str] | None = None,
    apply: bool = False,
    max_results: int = 5,
) -> dict:
    settings = settings or Settings.discover()
    cities = load_cities_105(settings)
    matched = cities.filter(
        (pl.col("city_name") == city) | (pl.col("city_name_short") == city)
    )
    if matched.height != 1:
        raise ValueError(f"City must resolve to one of the 105-city scope: {city}")
    row = matched.row(0, named=True)
    provider = provider or build_search_fallback(settings)
    selected_roles = roles or list(REQUIRED_ROLES)
    candidates: list[dict] = []
    attempts: list[dict] = []
    for role in selected_roles:
        term = ROLE_TERMS.get(role, role)
        query = f'{row["city_name"]} {term} 官网 政策 通知 site:gov.cn'
        try:
            results = provider.search(query, max_results=max_results)
        except Exception as exc:
            attempts.append({"role": role, "query": query, "status": "failed", "error_type": type(exc).__name__})
            continue
        attempts.append({"role": role, "query": query, "status": "ok", "result_count": len(results)})
        for result in results:
            domain = (urlsplit(result.url).hostname or "").lower().removeprefix("www.")
            official = _official_domain(result.url)
            candidates.append(
                {
                    "city_id": row["city_id"],
                    "city_name": row["city_name"],
                    "province_name": row["province_name"],
                    "source_role": role,
                    "url": result.url,
                    "domain": domain,
                    "title": result.title,
                    "official_domain_verified": official,
                    "candidate_status": "official_candidate" if official else "review_required",
                    "discovery_provider": provider.name,
                    "query": query,
                }
            )
    added = 0
    if apply and candidates:
        existing = load_registry(settings)
        known = {source.source_id for source in existing}
        now = datetime.now(UTC)
        for candidate in candidates:
            if not candidate["official_domain_verified"]:
                continue
            source_id = _source_id(candidate["city_id"], candidate["source_role"], candidate["domain"])
            if source_id in known:
                continue
            existing.append(
                RegisteredSource(
                    source_id=source_id,
                    source_name=candidate["title"] or candidate["domain"],
                    domain=candidate["domain"],
                    homepage_url=candidate["url"],
                    seed_urls=[candidate["url"]],
                    source_type="government",
                    source_role="canonical_candidate",
                    agency_type=candidate["source_role"],
                    official_status="official",
                    scope_type="municipal",
                    city_ids=[candidate["city_id"]],
                    required_level="required" if candidate["source_role"] in REQUIRED_ROLES else "recommended",
                    discovery_method="web_search",
                    discovery_provider=provider.name,
                    official_domain_verified=True,
                    organization_name_standardized=candidate["title"] or None,
                    crawl_enabled=False,
                    health_status="pending_evaluation",
                    created_at=now,
                    updated_at=now,
                )
            )
            known.add(source_id)
            added += 1
        if added:
            save_registry_atomic(existing, settings, action=f"discover_city={row['city_id']};added={added}")
    return {
        "city_id": row["city_id"],
        "city_name": row["city_name"],
        "provider": provider.name,
        "candidate_count": len(candidates),
        "official_candidate_count": sum(item["official_domain_verified"] for item in candidates),
        "added_disabled_sources": added,
        "attempts": attempts,
        "candidates": candidates,
    }


def discover_all_sources(
    settings: Settings | None = None,
    *,
    provider: SearchProvider | None = None,
    apply: bool = False,
    city_limit: int | None = None,
) -> dict:
    settings = settings or Settings.discover()
    cities = load_cities_105(settings)
    if city_limit:
        cities = cities.head(city_limit)
    results = [
        discover_city_sources(
            row["city_name"], settings, provider=provider, apply=apply
        )
        for row in cities.iter_rows(named=True)
    ]
    return {
        "cities": len(results),
        "candidates": sum(item["candidate_count"] for item in results),
        "official_candidates": sum(item["official_candidate_count"] for item in results),
        "added_disabled_sources": sum(item["added_disabled_sources"] for item in results),
        "results": results,
    }


def complete_source_matrix(settings: Settings | None = None) -> dict:
    settings = settings or Settings.discover()
    requirements = load_source_requirements(settings)
    sources = load_registry(settings)
    source_lookup: dict[tuple[str, str], list[RegisteredSource]] = {}
    for source in sources:
        for city_id in source.city_ids:
            source_lookup.setdefault((city_id, source.agency_type), []).append(source)
    rows = []
    for city in requirements.get("cities", []):
        for role in city.get("required_roles", REQUIRED_ROLES):
            matched = source_lookup.get((city["city_id"], role), [])
            rows.append(
                {
                    "city_id": city["city_id"],
                    "city_name": city["city_name"],
                    "province_name": city["province_name"],
                    "source_role": role,
                    "required_level": "required",
                    "registered_source_count": len(matched),
                    "healthy_source_count": sum(
                        (source.source_health_score or 0) >= 90 for source in matched
                    ),
                    "enabled_source_count": sum(source.crawl_enabled for source in matched),
                    "source_ids": ";".join(source.source_id for source in matched),
                    "gap_reason": None if matched else "required_source_not_registered",
                }
            )
    frame = pl.DataFrame(rows, infer_schema_length=None)
    output = settings.outputs / "coverage"
    output.mkdir(parents=True, exist_ok=True)
    frame.write_parquet(output / "city_source_requirement_matrix.parquet", compression="zstd")
    frame.write_csv(output / "city_source_requirement_matrix.csv")
    missing = frame.filter(pl.col("registered_source_count") == 0)
    return {
        "cities": frame["city_id"].n_unique(),
        "required_cells": frame.height,
        "registered_cells": frame.filter(pl.col("registered_source_count") > 0).height,
        "missing_cells": missing.height,
        "cities_with_all_required_roles": frame.group_by("city_id").agg(
            (pl.col("registered_source_count") > 0).all().alias("complete")
        ).filter(pl.col("complete")).height,
        "output": str(output / "city_source_requirement_matrix.parquet"),
    }


def repair_sources(settings: Settings | None = None) -> dict:
    settings = settings or Settings.discover()
    sources = load_registry(settings)
    replacements = {source.replacement_source_id for source in sources if source.replacement_source_id}
    unhealthy = [
        source.source_id
        for source in sources
        if source.crawl_enabled and (source.source_health_score or 0) < 60
    ]
    return {
        "unhealthy_enabled_sources": unhealthy,
        "replacement_links": len(replacements),
        "action": "evaluate candidates before changing enablement",
        "automatic_registry_changes": 0,
    }
