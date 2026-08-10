"""Run one bounded, city-centric source-completion batch.

This is an orchestration layer only.  Candidate discovery is restricted to
links found on already verified official domains; candidate admission still
uses the existing probe, deterministic verification, promotion, and strict
enablement functions.  The script never writes ``is_verified`` or
``crawl_enabled`` directly.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlsplit

import polars as pl
from bs4 import BeautifulSoup

from policydb.crawl.dedup import canonicalize_url
from policydb.crawl.fetcher import RespectfulFetcher
from policydb.settings import Settings
from policydb.source_discovery import is_reusable_source_entry
from policydb.source_slots import (
    build_requirement_slots,
    enable_source_strict,
    list_candidates,
    probe_candidates,
    promote_candidate,
    upsert_candidates,
)

ROLE_TERMS: dict[str, tuple[str, ...]] = {
    "municipal_government": ("市人民政府", "市政府", "政府信息公开", "政务公开", "政策文件"),
    "government_gazette": ("政府公报", "公报目录", "公报历史", "公报下载"),
    "housing_department": ("住房和城乡建设", "住房城乡建设", "住建", "房屋管理", "住房保障"),
    "provident_fund_center": ("住房公积金", "公积金管理", "公积金中心"),
    "natural_resources_department": ("自然资源", "规划和自然资源", "自然资源和规划", "国土资源"),
}
ENTRY_TERMS = (
    "政府信息公开",
    "政务公开",
    "政策文件",
    "政策法规",
    "通知公告",
    "规范性文件",
    "政府公报",
    "信息公开",
)
PATH_TERMS = (
    "/zwgk",
    "/zfxxgk",
    "/gkml",
    "/zcfg",
    "/zcwj",
    "/tzgg",
    "/gongbao",
    "/xxgk",
    "/info",
    "/column",
    "/channel",
    "/node_",
    "/list",
)
TEMPLATE_PATHS = (
    "/zwgk/",
    "/zfxxgk/",
    "/gkml/",
    "/xxgk/",
    "/zcfg/",
    "/zcwj/",
    "/tzgg/",
    "/gongbao/",
)


def now() -> str:
    return datetime.now(UTC).isoformat()


def sha256_text(value: object) -> str:
    return hashlib.sha256(str(value or "").encode("utf-8")).hexdigest()


def atomic_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + ".tmp")
    temp.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")
    # Windows readers (including status monitors and antivirus scanners) can
    # briefly hold the destination while the writer reaches a checkpoint.
    # Retry the atomic rename without changing the payload or falling back to
    # a non-atomic write.  A persistent failure still aborts the batch so the
    # checkpoint gate remains fail-closed.
    for attempt in range(6):
        try:
            os.replace(temp, path)
            return
        except PermissionError:
            if attempt == 5:
                raise
            time.sleep(0.2 * (2**attempt))


def write_frame(frame: pl.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + ".tmp")
    frame.write_parquet(temp)
    os.replace(temp, path)


def host(value: str | None) -> str:
    return (urlsplit(str(value or "")).hostname or "").lower().removeprefix("www.")


def official(value: str | None) -> bool:
    value_host = host(value)
    return value_host == "gov.cn" or value_host.endswith(".gov.cn")


def clean_text(value: object, limit: int = 1200) -> str:
    return " ".join(str(value or "").split()).strip()[:limit]


def role_matches(role: str, label: str, target: str, page_title: str) -> tuple[int, list[str]]:
    text = " ".join((label, target, page_title)).lower()
    hits = [term for term in ROLE_TERMS.get(role, ()) if term.lower() in text]
    entry_hits = [term for term in ENTRY_TERMS if term.lower() in text]
    path_hit = any(token in urlsplit(target).path.lower() for token in PATH_TERMS)
    score = len(hits) * 40 + len(entry_hits) * 15 + (20 if path_hit else 0)
    return score, hits + entry_hits + (["policy_entry_path"] if path_hit else [])


def parse_links(fetch: dict[str, Any], target_roles: set[str]) -> list[dict[str, Any]]:
    if fetch.get("error") or int(fetch.get("status_code") or 0) != 200:
        return []
    body = fetch.get("body") or b""
    soup = BeautifulSoup(body, "html.parser")
    final_url = str(fetch.get("final_url") or fetch.get("requested_url") or "")
    origin = host(final_url)
    title = clean_text(soup.title.get_text(" ", strip=True) if soup.title else "", 500)
    result: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    direct_target = canonicalize_url(final_url)
    if direct_target and official(direct_target) and is_reusable_source_entry(direct_target):
        for role in sorted(target_roles):
            score, reasons = role_matches(role, title, direct_target, title)
            if score < 35:
                continue
            seen.add((role, direct_target))
            result.append(
                {
                    "source_role": role,
                    "candidate_url": direct_target,
                    "candidate_title": title or "official template entry",
                    "parent_page_title": title,
                    "role_score": score,
                    "role_reasons": reasons,
                    "parent_url": str(fetch.get("requested_url") or final_url),
                    "parent_response_sha256": fetch.get("response_sha256"),
                    "parent_source_id": fetch.get("source_id"),
                    "official_domain": origin,
                }
            )
    for anchor in soup.find_all("a", href=True):
        label = clean_text(anchor.get_text(" ", strip=True), 300)
        target = canonicalize_url(urljoin(final_url, str(anchor.get("href") or "")))
        if not target or not official(target) or host(target) != origin:
            continue
        if target.lower().split("?", 1)[0].endswith((".pdf", ".doc", ".docx", ".xls", ".xlsx")):
            continue
        if not is_reusable_source_entry(target):
            continue
        for role in sorted(target_roles):
            score, reasons = role_matches(role, label, target, title)
            if score < 35:
                continue
            key = (role, target)
            if key in seen:
                continue
            seen.add(key)
            result.append(
                {
                    "source_role": role,
                    "candidate_url": target,
                    "candidate_title": label or title or "official same-domain entry",
                    "parent_page_title": title,
                    "role_score": score,
                    "role_reasons": reasons,
                    "parent_url": final_url,
                    "parent_response_sha256": fetch.get("response_sha256"),
                    "parent_source_id": fetch.get("source_id"),
                    "official_domain": host(target),
                }
            )
    result.sort(key=lambda item: (-int(item["role_score"]), item["source_role"], item["candidate_url"]))
    return result


def parse_stored_links(fetch: dict[str, Any], target_roles: set[str]) -> list[dict[str, Any]]:
    """Map previously captured same-domain links without claiming a new fetch."""
    raw_origin = str(fetch.get("official_domain") or fetch.get("requested_url") or "")
    origin = host(raw_origin if "://" in raw_origin else f"https://{raw_origin}")
    title = clean_text(fetch.get("page_title") or "", 500)
    result: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for link in fetch.get("stored_links") or []:
        target = canonicalize_url(str(link.get("url") or ""))
        label = clean_text(link.get("label"), 300)
        if not target or not official(target) or host(target) != origin:
            continue
        if target.lower().split("?", 1)[0].endswith((".pdf", ".doc", ".docx", ".xls", ".xlsx")):
            continue
        if not is_reusable_source_entry(target):
            continue
        for role in sorted(target_roles):
            score, reasons = role_matches(role, label, target, title)
            parent_role = str(fetch.get("parent_source_role") or "")
            if parent_role == role:
                score = max(score, 35)
                reasons = [*reasons, "stored_parent_role_match"]
            if score < 35:
                continue
            key = (role, target)
            if key in seen:
                continue
            seen.add(key)
            result.append(
                {
                    "source_role": role,
                    "candidate_url": target,
                    "candidate_title": label or title or "stored same-domain entry",
                    "parent_page_title": title,
                    "role_score": score,
                    "role_reasons": reasons,
                    "parent_url": str(fetch.get("requested_url") or ""),
                    "parent_response_sha256": fetch.get("response_sha256"),
                    "parent_source_id": fetch.get("source_id"),
                    "official_domain": origin,
                }
            )
    result.sort(key=lambda item: (-int(item["role_score"]), item["source_role"], item["candidate_url"]))
    return result


def fetch_one(item: dict[str, Any], settings: Settings, run_id: str) -> dict[str, Any]:
    url = str(item["url"])
    try:
        fetcher = RespectfulFetcher(
            timeout=settings.request_timeout,
            connect_timeout=settings.connect_timeout,
            retries=min(settings.max_retries, 2),
            rate_limit=max(settings.default_rate_limit, 0.5),
            check_robots=settings.respect_robots,
        )
        result = fetcher.fetch(url)
        return {
            **item,
            "run_id": run_id,
            "requested_url": url,
            "final_url": result.final_url,
            "status_code": result.status_code,
            "network_route": result.network_route,
            "response_sha256": result.response_sha256,
            "retrieved_at": result.retrieved_at.isoformat(),
            "body": result.body,
            "error": None,
        }
    except Exception as exc:
        return {
            **item,
            "run_id": run_id,
            "requested_url": url,
            "final_url": None,
            "status_code": None,
            "network_route": None,
            "response_sha256": None,
            "retrieved_at": now(),
            "body": b"",
            "error": type(exc).__name__,
            "error_message": clean_text(str(exc), 500),
        }


def select_slots(plan_dir: Path, max_slots: int, strategy: str) -> tuple[list[dict[str, Any]], list[str]]:
    queue = pl.read_parquet(plan_dir / "FAST_TRACK_PRIORITY_QUEUE.parquet")
    ledger_path = plan_dir / "SLOT_ATTEMPT_LEDGER.parquet"
    if ledger_path.exists():
        attempted = (
            pl.read_parquet(ledger_path)
            .filter(
                (pl.col("strategy") == strategy)
                & pl.col("result").is_in(["probed_rejected", "no_retained_candidate"])
            )
            .select(["slot_id", "candidate_set_hash"])
            .unique()
        )
        if attempted.height:
            # A failed slot/candidate-set pair must change strategy or evidence
            # before it can be selected again.  This keeps retries out of the
            # fast-track batch without changing any strict admission gates.
            queue = queue.join(attempted, on=["slot_id", "candidate_set_hash"], how="anti")
    queue = queue.filter(
        pl.col("fast_track_eligible")
        & (pl.col("priority_score") <= 3)
        & (pl.col("verified_domain_count") > 0)
    )
    rows = queue.to_dicts()
    by_city: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        by_city[str(row["city_id"])].append(row)
    cities = sorted(
        by_city,
        key=lambda city_id: (
            -len(by_city[city_id]),
            min(int(row.get("priority_score") or 9) for row in by_city[city_id]),
            -max(int(row.get("verified_domain_count") or 0) for row in by_city[city_id]),
            city_id,
        ),
    )
    selected: list[dict[str, Any]] = []
    selected_cities: list[str] = []
    for city_id in cities:
        if len(selected) >= max_slots:
            break
        selected_cities.append(city_id)
        selected.extend(by_city[city_id][: max_slots - len(selected)])
    return selected, selected_cities


def main(args: argparse.Namespace) -> int:
    settings = Settings.discover()
    plan_dir = Path(args.plan_dir).resolve()
    if not plan_dir.exists():
        raise FileNotFoundError(plan_dir)
    run_id = args.run_id or f"FAST_DOMAIN_{datetime.now(UTC):%Y%m%dT%H%M%SZ}"
    run_dir = plan_dir / "domain_batches" / run_id
    run_dir.mkdir(parents=True, exist_ok=False)
    state_path = run_dir / "current_status.json"
    state: dict[str, Any] = {
        "run_id": run_id,
        "strategy": args.strategy,
        "mode": "FAST_TRACK_TO_400",
        "apply": bool(args.apply),
        "max_slots": args.max_slots,
        "max_proposals_per_slot": args.max_proposals_per_slot,
        "concurrency": args.concurrency,
        "current_step": "planning",
        "current_city": None,
        "selected_slots": [],
        "selected_cities": [],
        "fetches": 0,
        "fetch_successes": 0,
        "proposals": 0,
        "upserted": 0,
        "probes": 0,
        "verified_added": 0,
        "enabled_added": 0,
        "latest_error": None,
        "last_progress_at": now(),
    }
    atomic_json(state_path, state)

    selected_slots, selected_cities = select_slots(plan_dir, args.max_slots, args.strategy)
    state.update(
        {
            "current_step": "domain_fetch",
            "selected_slots": [str(row["slot_id"]) for row in selected_slots],
            "selected_cities": selected_cities,
            "last_progress_at": now(),
        }
    )
    atomic_json(state_path, state)
    selected_slot_by_city: dict[str, list[dict[str, Any]]] = defaultdict(list)
    selected_slot_ids = {str(row["slot_id"]) for row in selected_slots}
    target_roles_by_city: dict[str, set[str]] = defaultdict(set)
    for row in selected_slots:
        selected_slot_by_city[str(row["city_id"])].append(row)
        target_roles_by_city[str(row["city_id"])].add(str(row["source_role"]))

    inventory = pl.read_parquet(plan_dir / "CITY_OFFICIAL_DOMAIN_INVENTORY.parquet")
    fetch_items: list[dict[str, Any]] = []
    for city_id in selected_cities:
        city_rows = inventory.filter(pl.col("city_id") == city_id).to_dicts()
        by_domain: dict[str, list[str]] = defaultdict(list)
        source_by_url: dict[str, str] = {}
        for row in city_rows:
            dom = str(row.get("official_domain") or "")
            if not dom:
                continue
            urls: list[str] = []
            for value in (row.get("entry_url"), row.get("homepage_url")):
                text = str(value or "").strip()
                if text and text not in urls:
                    urls.append(text)
            for value in row.get("list_page_urls") or []:
                text = str(value or "").strip()
                if text and text not in urls:
                    urls.append(text)
            domain_url_limit = 12 if args.strategy in {
                "deep_verified_domain_expansion",
                "deep_verified_domain_template_expansion",
            } else 3
            for url in urls:
                if url not in by_domain[dom] and len(by_domain[dom]) < domain_url_limit:
                    by_domain[dom].append(url)
                    source_by_url[url] = str(row.get("source_id") or "")
        for dom, urls in by_domain.items():
            if args.strategy in {
                "url_template_expansion",
                "deep_verified_domain_template_expansion",
            }:
                template_limit = 24 if args.strategy == "deep_verified_domain_template_expansion" else 12
                for template_path in TEMPLATE_PATHS:
                    template_url = canonicalize_url(f"https://{dom}{template_path}")
                    if template_url and template_url not in urls and len(urls) < template_limit:
                        urls.append(template_url)
            for url in urls:
                fetch_items.append(
                    {
                        "city_id": city_id,
                        "city_name": str(city_rows[0].get("city_name") or "") if city_rows else city_id,
                        "official_domain": dom,
                        "url": url,
                        "source_id": source_by_url.get(url),
                    }
                )

    if args.strategy == "stored_page_evidence_expansion":
        city_domains = {
            city_id: {
                str(row.get("official_domain") or "")
                for row in inventory.filter(pl.col("city_id") == city_id).to_dicts()
                if str(row.get("official_domain") or "")
            }
            for city_id in selected_cities
        }
        stored_items: list[dict[str, Any]] = []
        stored_seen: set[tuple[str, str]] = set()
        for row in list_candidates(settings=settings).to_dicts():
            city_id = str(row.get("city_id") or "")
            candidate_id = str(row.get("candidate_id") or "")
            candidate_url = str(row.get("candidate_url") or "")
            if city_id not in target_roles_by_city or host(candidate_url) not in city_domains.get(city_id, set()):
                continue
            if int(row.get("page_same_domain_link_count") or 0) <= 0:
                continue
            key = (city_id, candidate_id)
            if key in stored_seen:
                continue
            try:
                stored_links = json.loads(str(row.get("page_same_domain_links_json") or "[]"))
            except json.JSONDecodeError:
                stored_links = []
            if not isinstance(stored_links, list) or not stored_links:
                continue
            stored_seen.add(key)
            stored_items.append(
                {
                    "run_id": run_id,
                    "city_id": city_id,
                    "city_name": str(row.get("city_name") or city_id),
                    "official_domain": host(candidate_url),
                    "requested_url": candidate_url,
                    "final_url": candidate_url,
                    "status_code": 200,
                    "network_route": "stored_page_evidence",
                    "response_sha256": row.get("page_response_sha256"),
                    "retrieved_at": str(row.get("last_checked_at") or now()),
                    "error": None,
                    "error_message": None,
                    "source_id": row.get("source_id"),
                    "page_title": row.get("page_title") or row.get("page_heading") or row.get("page_role_evidence"),
                    "parent_source_role": row.get("source_role"),
                    "stored_links": stored_links,
                    "stored_page_evidence": True,
                }
            )
        fetch_items = stored_items

    fetch_results: list[dict[str, Any]] = []
    if args.strategy == "stored_page_evidence_expansion":
        fetch_results = fetch_items
        state["fetches"] = len(fetch_results)
        state["fetch_successes"] = len(fetch_results)
        state["last_progress_at"] = now()
        atomic_json(state_path, state)
    else:
        with ThreadPoolExecutor(max_workers=max(1, min(args.concurrency, 8))) as executor:
            futures = [executor.submit(fetch_one, item, settings, run_id) for item in fetch_items]
            for future in as_completed(futures):
                fetch = future.result()
                fetch_results.append(fetch)
                state["fetches"] = len(fetch_results)
                state["fetch_successes"] = sum(int(int(item.get("status_code") or 0) == 200 and not item.get("error")) for item in fetch_results)
                state["last_progress_at"] = now()
                atomic_json(state_path, state)

    evidence_rows = []
    for fetch in fetch_results:
        evidence_rows.append({key: fetch.get(key) for key in ("run_id", "city_id", "city_name", "official_domain", "requested_url", "final_url", "status_code", "network_route", "response_sha256", "retrieved_at", "error", "error_message", "source_id")})
    write_frame(pl.DataFrame(evidence_rows, infer_schema_length=None), run_dir / "domain_fetch_evidence.parquet")

    existing = list_candidates(settings=settings)
    existing_by_slot_url = {
        (str(row.get("slot_id") or ""), str(row.get("canonical_url") or ""))
        for row in existing.to_dicts()
    }
    proposals_by_slot: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for fetch in fetch_results:
        city_id = str(fetch.get("city_id") or "")
        target_roles = target_roles_by_city.get(city_id, set())
        parser = parse_stored_links if fetch.get("stored_page_evidence") else parse_links
        for link in parser(fetch, target_roles):
            target_role = str(link["source_role"])
            slot_row = next((row for row in selected_slot_by_city[city_id] if str(row["source_role"]) == target_role), None)
            if slot_row is None:
                continue
            slot_id = str(slot_row["slot_id"])
            url = str(link["candidate_url"])
            if (slot_id, url) in existing_by_slot_url:
                continue
            proposals_by_slot[slot_id].append(
                {
                    "city_id": city_id,
                    "source_role": target_role,
                    "candidate_url": url,
                    "candidate_kind": "official_entry_candidate" if target_role == "government_gazette" else "department_entry_candidate",
                    "page_type": "site_or_column_entry",
                    "entry_eligible": True,
                    "site_name": clean_text(link.get("parent_page_title"), 500),
                    "department_name": clean_text(link.get("candidate_title"), 500),
                    "candidate_title": clean_text(link.get("candidate_title"), 500),
                    "candidate_snippet": f"anchor={clean_text(link.get('candidate_title'), 300)}; parent={link.get('parent_url')}; role_reasons={','.join(link.get('role_reasons') or [])}",
                    "discovery_method": args.strategy,
                    "discovery_provider": "verified_domain_navigation",
                    "discovery_evidence_url": link.get("parent_url"),
                    "discovery_evidence_text": f"verified source_id={link.get('parent_source_id')}; city={slot_row.get('city_name')}; domain={link.get('official_domain')}; parent={link.get('parent_url')}; anchor={clean_text(link.get('candidate_title'), 300)}",
                    "official_domain_evidence": f"verified official domain inventory: {link.get('official_domain')}; parent_response_sha256={link.get('parent_response_sha256')}",
                    "city_match_evidence": f"verified source inventory city_id={city_id}; city_name={slot_row.get('city_name')}; parent_source_id={link.get('parent_source_id')}",
                    "role_match_evidence": f"anchor label and path matched {target_role}: {','.join(link.get('role_reasons') or [])}",
                    "official_confidence": 1.0,
                    "city_confidence": 1.0,
                    "role_confidence": 0.8,
                    "overall_confidence": 0.8,
                    "is_official": True,
                    "is_verified": False,
                    "is_enabled": False,
                    "manual_review_status": "pending_probe",
                    "health_status": "pending",
                    "parser_status": "pending",
                    "generation_batch_id": run_id,
                    "prefilter_status": "shortlist",
                    "prefilter_reasons": "verified same-domain official navigation; deterministic probe pending",
                    "prefilter_reason_codes": ["verified_domain_navigation", "role_anchor_match", "reusable_entry_prefilter"],
                    "deterministic_score": int(link["role_score"]),
                    "deterministic_score_reasons": ";".join(link.get("role_reasons") or []),
                    "source_bundle_id": f"CITY_DOMAIN_{city_id}_{link.get('official_domain')}",
                    "homepage_url": link.get("parent_url"),
                    "first_seen_at": now(),
                }
            )

    proposal_rows: list[dict[str, Any]] = []
    for _slot_id, rows in proposals_by_slot.items():
        dedup: dict[str, dict[str, Any]] = {}
        for row in rows:
            dedup.setdefault(str(row["candidate_url"]), row)
        proposal_rows.extend(sorted(dedup.values(), key=lambda row: (-int(row["deterministic_score"]), row["candidate_url"]))[: args.max_proposals_per_slot])
    proposals=pl.DataFrame(proposal_rows, infer_schema_length=None) if proposal_rows else pl.DataFrame({"candidate_id": []})
    write_frame(proposals, run_dir / "candidate_proposals.parquet")
    state.update({"current_step": "candidate_proposals_ready", "proposals": len(proposal_rows), "last_progress_at": now()})
    atomic_json(state_path, state)

    if not args.apply or not proposal_rows:
        state["current_step"] = "dry_run_complete" if not args.apply else "no_new_candidates"
        state["completed_at"] = now()
        atomic_json(state_path, state)
        atomic_json(run_dir / "run_summary.json", state)
        return 0

    for name in ("source_candidates.parquet", "source_requirement_slots.parquet", "source_registry.parquet"):
        source = settings.curated / name
        if source.exists():
            shutil.copy2(source, run_dir / f"before_{name}")
    state["current_step"] = "candidate_upsert"
    atomic_json(state_path, state)
    upsert_result = upsert_candidates(proposal_rows, settings)
    state["upserted"] = int(upsert_result.get("upserted") or 0)
    atomic_json(state_path, state)

    current = list_candidates(settings=settings)
    candidate_ids = [
        str(row["candidate_id"])
        for row in current.to_dicts()
        if str(row.get("generation_batch_id") or "") == run_id and str(row.get("slot_id") or "") in selected_slot_ids
    ]
    probe_rows: list[dict[str, Any]] = []
    for index, candidate_id in enumerate(candidate_ids, start=1):
        state.update({"current_step": "probe_verify_promote_enable", "probes": index - 1, "last_progress_at": now()})
        atomic_json(state_path, state)
        item: dict[str, Any] = {"candidate_id": candidate_id, "status": "started", "started_at": now()}
        try:
            result = probe_candidates(candidate_ids=[candidate_id], rounds=args.rounds, settings=settings)
            item["probe_result"] = result
            current_one = list_candidates(candidate_id=candidate_id, settings=settings)
            if current_one.height == 1 and bool(current_one[0, "is_verified"]):
                item["verified"] = True
                promotion = promote_candidate(candidate_id, settings=settings)
                item["promotion"] = promotion
                source_id = str(promotion.get("source_id") or "")
                if not source_id:
                    raise RuntimeError("promotion returned no source_id")
                enablement = enable_source_strict(source_id, settings=settings)
                item["enablement"] = enablement
                item["enabled"] = True
                state["verified_added"] = int(state.get("verified_added") or 0) + 1
                state["enabled_added"] = int(state.get("enabled_added") or 0) + 1
            else:
                item["verified"] = False
                item["enabled"] = False
            item["status"] = "completed"
        except Exception as exc:
            item["status"] = "failed"
            item["error_type"] = type(exc).__name__
            item["error_message"] = clean_text(str(exc), 1000)
            state["latest_error"] = {"candidate_id": candidate_id, "error_type": type(exc).__name__, "error_message": clean_text(str(exc), 500)}
        item["completed_at"] = now()
        probe_rows.append(item)
        state["probes"] = index
        state["last_progress_at"] = now()
        atomic_json(state_path, state)
        with (run_dir / "probe_results.jsonl").open("a", encoding="utf-8") as stream:
            stream.write(json.dumps(item, ensure_ascii=False, default=str) + "\n")

    audit = build_requirement_slots(settings)
    state.update({"current_step": "audit_525", "audit_after": audit, "completed_at": now(), "latest_error": state.get("latest_error")})
    atomic_json(run_dir / "audit_525_after.json", audit)
    atomic_json(state_path, state)
    atomic_json(run_dir / "run_summary.json", state)
    return 0 if int(audit.get("enabled_unverified_slots") or 0) == 0 else 2


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Bounded verified-domain source completion batch")
    parser.add_argument("--plan-dir", required=True)
    parser.add_argument("--max-slots", type=int, default=50)
    parser.add_argument("--max-proposals-per-slot", type=int, default=3)
    parser.add_argument("--concurrency", type=int, default=6)
    parser.add_argument("--rounds", type=int, default=2)
    parser.add_argument("--run-id")
    parser.add_argument(
        "--strategy",
        choices=(
            "city_official_domain_expansion",
            "url_template_expansion",
            "stored_page_evidence_expansion",
            "deep_verified_domain_expansion",
            "deep_verified_domain_template_expansion",
        ),
        default="city_official_domain_expansion",
    )
    parser.add_argument("--apply", action="store_true")
    parsed = parser.parse_args()
    raise SystemExit(main(parsed))
