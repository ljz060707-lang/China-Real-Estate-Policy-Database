"""National and provincial source discovery for the existing crawl pipeline.

This module deliberately keeps jurisdictional sources separate from the 105
city source registry semantics.  A national or provincial source is registered
once, carries its explicit scope, and is planned once by ``CrawlPipeline``;
the pipeline never fans it out into one copy per city.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from urllib.parse import urlsplit

import yaml

from policydb.config.providers import SearchProvider, build_search_fallback
from policydb.crawl.checkpoint import append_unique
from policydb.crawl.models import RegisteredSource
from policydb.crawl.registry import (
    load_registry,
    materialize_registry_parquet,
    save_registry_atomic,
)
from policydb.scope import load_cities_105
from policydb.settings import Settings
from policydb.source_discovery import is_reusable_source_entry
from policydb.transform.normalization import stable_id

SUPPORTED_LEVELS = {"national", "provincial"}


def _official_domain(url: str) -> bool:
    host = (urlsplit(str(url)).hostname or "").lower().removeprefix("www.")
    return host == "gov.cn" or host.endswith(".gov.cn")


def _domain(url: str) -> str:
    return (urlsplit(str(url)).hostname or "").lower().removeprefix("www.")


def _url_key(url: str) -> str:
    return str(url).strip().rstrip("/").lower()


def jurisdiction_source_id(
    scope_type: str,
    domain: str,
    source_role: str,
    province_codes: list[str] | tuple[str, ...] = (),
) -> str:
    """Return a stable ID independent of search-result ordering."""

    return stable_id(
        "jurisdiction",
        scope_type,
        domain,
        source_role,
        ",".join(sorted({str(value) for value in province_codes})),
        prefix="JURSRC",
    )


def load_jurisdiction_config(settings: Settings | None = None) -> dict:
    settings = settings or Settings.discover()
    path = settings.root / "config" / "jurisdiction_scope.yaml"
    if not path.exists():
        raise FileNotFoundError(f"Jurisdiction scope config is missing: {path}")
    payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(payload, dict):
        raise ValueError(f"Jurisdiction scope config must be a mapping: {path}")
    return payload


def normalize_levels(levels: list[str] | tuple[str, ...] | None) -> list[str]:
    values = [str(value).strip().lower() for value in (levels or SUPPORTED_LEVELS)]
    normalized = list(dict.fromkeys(value for value in values if value))
    invalid = set(normalized) - SUPPORTED_LEVELS
    if invalid:
        raise ValueError("Unsupported jurisdiction level(s): " + ", ".join(sorted(invalid)))
    return normalized


def _province_rows(settings: Settings) -> list[dict[str, str]]:
    cities = load_cities_105(settings)
    return [
        {"province_code": str(row["province_code"]), "province_name": str(row["province_name"])}
        for row in cities.select("province_code", "province_name")
        .unique(subset=["province_code"], keep="first")
        .sort("province_code")
        .iter_rows(named=True)
    ]


def _candidate_row(
    *,
    scope_type: str,
    url: str,
    source_role: str,
    province_code: str | None,
    province_name: str | None,
    title: str | None,
    query: str | None,
    provider: str,
    method: str,
    candidate_status: str,
    applied_source_id: str | None = None,
) -> dict[str, object]:
    domain = _domain(url)
    codes = [province_code] if province_code else []
    return {
        "candidate_id": stable_id(
            scope_type,
            province_code,
            source_role,
            domain,
            _url_key(url),
            prefix="JURCAND",
        ),
        "scope_type": scope_type,
        "province_code": province_code,
        "province_name": province_name,
        "source_role": source_role,
        "url": url,
        "canonical_url": _url_key(url),
        "domain": domain,
        "title": title,
        "query": query,
        "discovery_provider": provider,
        "discovery_method": method,
        "official_domain_verified": _official_domain(url),
        "stable_entry_candidate": is_reusable_source_entry(url),
        "candidate_status": candidate_status,
        "applied_source_id": applied_source_id,
        "created_at": datetime.now(UTC).isoformat(),
        "province_codes_json": json.dumps(codes, ensure_ascii=False),
    }


def _configured_national_candidates(config: dict, levels: set[str]) -> list[dict[str, object]]:
    if "national" not in levels:
        return []
    rows: list[dict[str, object]] = []
    for spec in config.get("national_sources", []) or []:
        if not isinstance(spec, dict):
            continue
        source_role = str(spec.get("source_role") or "national_policy")
        for url in dict.fromkeys(
            str(value).strip()
            for value in [*(spec.get("list_page_urls") or []), *(spec.get("seed_urls") or [])]
            if str(value).strip()
        ):
            candidate = _candidate_row(
                scope_type="national",
                url=url,
                source_role=source_role,
                province_code=None,
                province_name="全国",
                title=str(spec.get("source_name") or "") or None,
                query=None,
                provider="configured_official_registry",
                method="configured_official_source",
                candidate_status="review_required",
            )
            # The URL is an explicit reviewed list entry in the checked-in
            # config; a legacy ``home.htm`` filename must not be mistaken for
            # a detail page by the generic heuristic.
            if _official_domain(url):
                candidate["candidate_status"] = "eligible_official_entry"
            rows.append(candidate)
    return rows


def _configured_provincial_candidates(
    settings: Settings,
    config: dict,
    levels: set[str],
) -> list[dict[str, object]]:
    if "provincial" not in levels:
        return []
    by_id = {source.source_id: source for source in load_registry(settings)}
    province_names = {row["province_code"]: row["province_name"] for row in _province_rows(settings)}
    rows: list[dict[str, object]] = []
    configured_seeds = config.get("provincial_source_seeds") or []
    if not configured_seeds:
        configured_seeds = [
            {"source_id": source_id}
            for source_id in config.get("provincial_source_ids", []) or []
        ]
    for seed in configured_seeds:
        if not isinstance(seed, dict):
            continue
        source_id = str(seed.get("source_id") or "")
        source = by_id.get(source_id)
        if source is None:
            continue
        codes = [
            str(value)
            for value in (seed.get("province_codes") or source.province_codes)
        ]
        if not codes:
            continue
        for url in dict.fromkeys([*source.list_page_urls, *source.seed_urls]):
            if not url:
                continue
            code = codes[0]
            rows.append(
                _candidate_row(
                    scope_type="provincial",
                    url=str(url),
                    source_role=source.source_role,
                    province_code=code,
                    province_name=province_names.get(code),
                    title=source.source_name,
                    query=None,
                    provider="existing_verified_registry",
                    method="verified_source_scope_clone",
                    candidate_status=(
                        "eligible_official_entry"
                        if source.official_status in {"official", "official_reprint"}
                        and _official_domain(str(url))
                        and is_reusable_source_entry(str(url))
                        else "review_required"
                    ),
                )
            )
    return rows


def _search_provincial_candidates(
    settings: Settings,
    config: dict,
    provider: SearchProvider,
    *,
    max_results: int,
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    rows: list[dict[str, object]] = []
    attempts: list[dict[str, object]] = []
    templates = config.get("provincial_search_queries", []) or []
    for province in _province_rows(settings):
        for spec in templates:
            if not isinstance(spec, dict):
                continue
            role = str(spec.get("source_role") or "provincial_policy")
            template = str(spec.get("query") or "")
            query = template.format(
                province_name=province["province_name"],
                province_code=province["province_code"],
                role=role,
            )
            try:
                results = provider.search(query, max_results=max_results)
                attempts.append(
                    {
                        "province_code": province["province_code"],
                        "source_role": role,
                        "query": query,
                        "provider": provider.name,
                        "status": "ok",
                        "result_count": len(results),
                    }
                )
            except Exception as exc:
                attempts.append(
                    {
                        "province_code": province["province_code"],
                        "source_role": role,
                        "query": query,
                        "provider": provider.name,
                        "status": "failed",
                        "error_type": type(exc).__name__,
                    }
                )
                continue
            for result in results:
                url = str(result.url).strip()
                official = _official_domain(url)
                stable_entry = is_reusable_source_entry(url)
                rows.append(
                    _candidate_row(
                        scope_type="provincial",
                        url=url,
                        source_role=role,
                        province_code=province["province_code"],
                        province_name=province["province_name"],
                        title=result.title,
                        query=query,
                        provider=provider.name,
                        method="search_provider",
                        candidate_status=(
                            "eligible_official_entry"
                            if official and stable_entry
                            else "review_required"
                        ),
                    )
                )
    return rows, attempts


def _registry_identity(source: RegisteredSource) -> tuple[str, str, str, str]:
    return (
        source.scope_type,
        source.domain.lower(),
        source.source_role,
        ",".join(sorted(str(value) for value in source.province_codes)),
    )


def _apply_candidates(
    settings: Settings,
    config: dict,
    candidates: list[dict[str, object]],
) -> tuple[list[RegisteredSource], dict[str, str]]:
    existing = load_registry(settings)
    known_ids = {source.source_id for source in existing}
    known_identity = {_registry_identity(source): source.source_id for source in existing}
    spec_by_role = {
        str(spec.get("source_role") or "national_policy"): spec
        for spec in config.get("national_sources", []) or []
        if isinstance(spec, dict)
    }
    applied: dict[str, str] = {}
    updated = list(existing)
    for candidate in candidates:
        if candidate.get("candidate_status") != "eligible_official_entry":
            continue
        scope_type = str(candidate["scope_type"])
        domain = str(candidate["domain"])
        role = str(candidate["source_role"])
        code = str(candidate.get("province_code") or "")
        codes = [code] if scope_type == "provincial" and code else []
        identity = (scope_type, domain, role, ",".join(sorted(codes)))
        existing_id = known_identity.get(identity)
        if existing_id:
            applied[str(candidate["candidate_id"])] = existing_id
            continue
        source_id = jurisdiction_source_id(scope_type, domain, role, codes)
        if source_id in known_ids:
            applied[str(candidate["candidate_id"])] = source_id
            continue
        source_spec = spec_by_role.get(role, {}) if scope_type == "national" else {}
        url = str(candidate["url"])
        source = RegisteredSource(
            source_id=source_id,
            source_name=str(candidate.get("title") or domain),
            domain=domain,
            source_type="government",
            source_role=role,
            official_status="official",
            seed_urls=[url],
            list_page_urls=[url],
            parser_adapter=str(source_spec.get("parser_adapter") or "generic_government"),
            crawl_enabled=True,
            priority=int(source_spec.get("priority") or 0),
            rate_limit=float(source_spec.get("rate_limit") or 0.5),
            city_ids=[],
            coverage_city_ids=[],
            province_codes=codes,
            scope_type=scope_type,
            agency_type=(
                str(source_spec.get("agency_type") or "other_official")
                if scope_type == "national"
                else "provincial_government"
            ),
            required_level="supplemental",
            discovery_method=str(candidate.get("discovery_method") or "jurisdictional_scope"),
            discovery_provider=str(candidate.get("discovery_provider") or ""),
            official_domain_verified=True,
            health_status="pending_evaluation",
            health_reason="registered by jurisdictional scope bootstrap; crawl validation pending",
            jurisdiction_mapping_id=stable_id(scope_type, domain, role, code, prefix="JURMAP"),
            approval_status="supplemental_scope_bootstrap",
            notes=(
                "Separate national/provincial source; never fanned out as a municipal source."
            ),
        )
        updated.append(source)
        known_ids.add(source_id)
        known_identity[identity] = source_id
        applied[str(candidate["candidate_id"])] = source_id
    if len(updated) != len(existing):
        save_registry_atomic(updated, settings, action="discover_jurisdictional_sources")
        materialize_registry_parquet(updated, settings)
    return updated, applied


def discover_jurisdiction_sources(
    settings: Settings | None = None,
    *,
    levels: list[str] | tuple[str, ...] | None = None,
    provider: SearchProvider | None = None,
    use_search: bool = True,
    apply: bool = False,
    max_results: int = 5,
) -> dict[str, object]:
    """Discover and optionally register national/provincial source entries.

    Search results remain append-only evidence.  Only official ``gov.cn``
    entries that pass the deterministic stable-entry check can be enabled, and
    each applied source is supplemental and scoped explicitly.
    """

    settings = settings or Settings.discover()
    selected_levels = normalize_levels(levels)
    config = load_jurisdiction_config(settings)
    candidates = _configured_national_candidates(config, set(selected_levels))
    candidates.extend(_configured_provincial_candidates(settings, config, set(selected_levels)))
    attempts: list[dict[str, object]] = []
    if use_search and "provincial" in selected_levels:
        provider = provider or build_search_fallback(settings)
        search_rows, attempts = _search_provincial_candidates(
            settings, config, provider, max_results=max(1, int(max_results))
        )
        candidates.extend(search_rows)
    unique: dict[str, dict[str, object]] = {}
    for candidate in candidates:
        unique.setdefault(str(candidate["candidate_id"]), candidate)
    candidates = list(unique.values())
    applied: dict[str, str] = {}
    if apply:
        _, applied = _apply_candidates(settings, config, candidates)
        for candidate in candidates:
            candidate["applied_source_id"] = applied.get(str(candidate["candidate_id"]))
            if candidate["applied_source_id"]:
                candidate["candidate_status"] = "registered_or_existing"
    audit_path = settings.curated / "jurisdiction_source_candidates.parquet"
    if candidates:
        append_unique(audit_path, candidates, "candidate_id")
    summary = {
        "scope_version": str(config.get("scope_version") or "jurisdiction-v1"),
        "levels": selected_levels,
        "provider": getattr(provider, "name", "configured_official_registry"),
        "use_search": use_search,
        "apply": apply,
        "candidate_count": len(candidates),
        "official_entry_candidate_count": sum(
            candidate["candidate_status"] in {"eligible_official_entry", "registered_or_existing"}
            for candidate in candidates
        ),
        "national_candidate_count": sum(candidate["scope_type"] == "national" for candidate in candidates),
        "provincial_candidate_count": sum(candidate["scope_type"] == "provincial" for candidate in candidates),
        "applied_count": len(applied),
        "search_attempt_count": len(attempts),
        "audit_path": str(audit_path),
        "attempts": attempts,
        "created_at": datetime.now(UTC).isoformat(),
    }
    output = settings.outputs / "jurisdictional_source_discovery"
    output.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    (output / f"{stamp}.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    return summary


__all__ = [
    "SUPPORTED_LEVELS",
    "discover_jurisdiction_sources",
    "jurisdiction_source_id",
    "load_jurisdiction_config",
    "normalize_levels",
]
