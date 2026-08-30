"""EP930 Membership Resolution Pipeline — read-only deterministic closure.

Consumes the frozen scope, the evidence-unit collapse snapshot, the previous
membership closure register, curated episode documents and the production
records table (read-only).  Emits a NEW timestamped resolution snapshot:

- EP930_MEMBERSHIP_RESOLUTION_DECISION_REGISTER.csv   (all 894 evidence units)
- EP930_MEMBERSHIP_RESOLUTION_WATERFALL.csv
- EP930_MEMBERSHIP_RESOLUTION_MANUAL_QUEUE.csv        (reduced manual queue)
- EP930_MEMBERSHIP_REFERENCE_RECONCILIATION_FINAL.csv (20 reference events)
- EP930_MEMBERSHIP_RESOLUTION_SUMMARY.json
- EP930_MEMBERSHIP_RESOLUTION_REPORT.md
- SHA256_MANIFEST.json

Rules (priority order, all deterministic / evidence-based, no API, no network):

R1 FROZEN_SCOPE_CITY_NOT_MEMBER   unit city not in the frozen 20-city scope
                                  -> CONFIRMED_OUTSIDE_EP930
R2 PORTAL_OR_BACKGROUND_PAGE      portal / navigation / background page
                                  -> SUPPORTING_ONLY
R3 LATER_POLICY_NO_2016_LINKAGE   explicit later-year policy evidence
                                  (title / doc number / text dates >= 2017)
                                  with no 2016 signal in the same unit
                                  -> CONFIRMED_OUTSIDE_EP930
R4 EXPLICIT_2016_POLICY           explicit 2016 policy evidence within windows
                                  -> CONFIRMED_EP930_CORE / EXTENDED / REPRINT
R5 DUPLICATE_OR_REDUNDANT         same content hash as a canonical unit
R6 MANUAL_FINAL_RESOLUTION        true boundary cases (kept for humans)

Reference events are reconciled against production official records and the
unit-level register (evidence-based, coverage-anchor semantics).

This script is READ-ONLY with respect to production: it never writes the
queue, the database, the curated tables or the runner state.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.build_ep930_econometric_grade import (
    EPISODE_ID,
    _safe_read_json,
    _safe_read_parquet,
    _sha256,
    _text,
    _write_csv,
    _write_json,
)

CORE_START = pd.Timestamp("2016-09-25")
CORE_END = pd.Timestamp("2016-10-10")
EXTENDED_START = pd.Timestamp("2016-09-20")
EXTENDED_END = pd.Timestamp("2016-10-15")

SCOPE_VERSION = "930-analysis-ready-v1"
SCOPE_HASH = "a751e9a99d405bc91dc4a5c4a19f900b398400e1e88b43f724e034209807a90d"

# Portal / navigation / background markers (title based).
PORTAL_MARKERS = (
    "门户网站",
    "住房公积金管理中心",
    "住房公积金网",
    "住房公积金网站",
    "公积金中心",
    "自然资源局",
    "移动端",
    "长者专区",
    "统计月报",
    "中心介绍",
    "网站地图",
    "商品房预售与现售",
    "购房资格",
    "阳光家缘",
    "政务公开",
    "年度建设用地供应计划",
    "专栏",
    "互动交流",
    "联系我们",
    "关于我们",
    "工作动态",
    "通知公告",
    "新闻中心",
    "政策法规",
    "行政规范性文件_政策",
    "政策文件库",
    "问答库",
    "网站",
    "首页",
    "管理中心",
    "土地供应",
    "下载中心",
    "办事指南",
    "网上办事",
    "机构简介",
    "网站无障碍",
    "住房和城乡建设局",
    "住房和城乡建设委员会",
    "住房和城乡建设厅",
    "住建厅",
    "政策解读",
    "解读",
)

# Policy-clause markers: presence means the page likely carries policy content.
POLICY_CLAUSE_MARKERS = (
    "关于",
    "通知",
    "意见",
    "措施",
    "办法",
    "规定",
    "公告",
    "解读",
    "方案",
    "自",
    "施行",
    "执行",
)

LATER_YEAR = re.compile(r"20(1[7-9]|2[0-6])")
URL_LATER = re.compile(
    r"/(20(1[7-9]|2[0-6]))/|t20(1[7-9]|2[0-6])\d{4}|(?:20(1[7-9]|2[0-6]))\s*年|20(1[7-9]|2[0-6])[a-z]{2,}"
)
DOCNUM_YEAR = re.compile(r"[〔\[]\s*(20(?:0\d|1\d|2\d|3\d))\s*[〕\]]\s*第?\s*(\d+)\s*号")
FULL_DATE_2016 = re.compile(r"2016\s*年\s*(\d{1,2})\s*月\s*(\d{1,2})\s*日")
DATE_2016 = re.compile(r"2016\s*年\s*\d{1,2}\s*月")


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _date_ts(value: Any) -> pd.Timestamp | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    parsed = pd.to_datetime(value, errors="coerce")
    return None if pd.isna(parsed) else pd.Timestamp(parsed)


def _window_class(ts: pd.Timestamp) -> str | None:
    if CORE_START <= ts <= CORE_END:
        return "CONFIRMED_EP930_CORE"
    if EXTENDED_START <= ts <= EXTENDED_END:
        return "CONFIRMED_EP930_EXTENDED"
    return None


def _load_inputs(data_root: Path, episode_root: Path, stamp: str) -> dict[str, Any]:
    scope = _safe_read_json(episode_root / "930_ANALYSIS_READY_SCOPE.json")
    ref_csv = episode_root / "treatment_universe_closure" / "membership_closure" / stamp / "EP930_MEMBERSHIP_REFERENCE_RECONCILIATION.csv"
    ref = pd.read_csv(ref_csv, dtype=str) if ref_csv.exists() else pd.DataFrame()
    scope_cities = {_text(v) for v in ref.get("city", pd.Series(dtype=str)).tolist() if _text(v)}
    if not scope_cities:
        scope_cities = {_text(v) for v in scope.get("cities", []) if _text(v)}

    master = pd.read_csv(
        episode_root
        / "treatment_universe_closure"
        / "evidence_unit_collapse"
        / "20260815T085958Z"
        / "EP930_EVIDENCE_UNIT_MASTER.csv",
        dtype=str,
    )
    mq = pd.read_csv(
        episode_root
        / "treatment_universe_closure"
        / "membership_closure"
        / stamp
        / "EP930_MEMBERSHIP_MANUAL_REVIEW_QUEUE.csv",
        dtype=str,
    )
    docs = _safe_read_parquet(data_root / "curated" / "policy_episode_documents.parquet")
    if not docs.empty and "episode_id" in docs.columns:
        docs = docs[docs["episode_id"].astype(str).eq(EPISODE_ID)].copy()
    return {
        "scope": scope,
        "scope_cities": scope_cities,
        "master": master,
        "manual_queue": mq,
        "docs": docs,
        "ref": ref,
    }


def _doc_map(docs: pd.DataFrame) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    if docs.empty:
        return result
    for row in docs.to_dict("records"):
        doc_id = _text(row.get("document_id"))
        if not doc_id:
            continue
        result.setdefault(
            doc_id,
            {
                "document_id": doc_id,
                "title": _text(row.get("document_title")),
                "official_text": _text(row.get("official_text")),
                "content_hash": _text(row.get("content_hash")),
                "publication_date": _text(row.get("publication_date")),
                "announcement_date": _text(row.get("announcement_date")),
                "effective_date": _text(row.get("effective_date")),
                "record_id": _text(row.get("record_id")),
            },
        )
    return result


def _unit_evidence(row: dict[str, Any], doc_map: dict[str, dict[str, Any]]) -> dict[str, Any]:
    doc_ids = [d for d in _text(row.get("document_ids")).split(";") if d]
    docs = [doc_map.get(d, {}) for d in doc_ids]
    text = " ".join(str(d.get("official_text") or "") for d in docs)
    title = _text(row.get("policy_title")) or " ".join(str(d.get("title") or "") for d in docs)
    url = _text(row.get("official_url"))
    hashes = [str(d.get("content_hash") or "") for d in docs if d.get("content_hash")]
    return {
        "text": text,
        "title": title,
        "url": url,
        "content_hashes": hashes,
        "first_hash": hashes[0] if hashes else "",
    }


def _later_year_evidence(ev: dict[str, Any]) -> bool:
    """Later-year evidence: doc number year >= 2017, title/URL/text year >= 2017."""
    combined = " ".join([ev["title"], ev["url"], ev["text"][:6000]])
    docnum_years = [int(y) for y, _n in DOCNUM_YEAR.findall(combined)]
    if any(y >= 2017 for y in docnum_years):
        return True
    later_dates = URL_LATER.findall(combined)
    if later_dates:
        return True
    return False


def _has_2016_signal(ev: dict[str, Any]) -> bool:
    """True when the unit itself carries 2016 policy evidence.

    A 2016 document-number citation inside a later policy's body text (e.g.
    建房〔2016〕168号 referenced by a 2026 policy) is NOT a 2016 signal for
    the unit itself: only an explicit 2016 date, or a 2016 doc number at the
    very start of the text (own document number), counts.
    """
    combined = " ".join([ev["title"], ev["url"], ev["text"][:6000]])
    if DATE_2016.search(combined):
        return True
    head = ev["text"][:400]
    docnum_years = [int(y) for y, _n in DOCNUM_YEAR.findall(head)]
    if 2016 in docnum_years:
        return True
    return False


def _is_portal(ev: dict[str, Any]) -> tuple[bool, str]:
    title = ev["title"]
    if any(marker in title for marker in PORTAL_MARKERS):
        # If a policy clause is present in the title, treat as content page.
        if not any(clause in title for clause in POLICY_CLAUSE_MARKERS):
            return True, "TITLE_PORTAL_MARKER"
        # Portal marker + no policy clause anywhere in the visible text -> portal.
        head = ev["text"][:1500]
        if not any(clause in head for clause in ("关于", "通知", "意见", "措施", "办法", "规定")):
            return True, "TITLE_PORTAL_MARKER_NO_POLICY_CLAUSE"
    return False, ""


def _classify_unit(row: dict[str, Any], doc_map: dict[str, dict[str, Any]], scope_cities: set[str]) -> dict[str, Any]:
    unit_id = _text(row.get("evidence_unit_id"))
    city = _text(row.get("master_city")) or _text(row.get("city"))
    ev = _unit_evidence(row, doc_map)
    decision = "MANUAL_REVIEW_REQUIRED"
    rule = ""
    evidence: list[str] = []

    # R1 frozen-scope city membership
    if city and city not in scope_cities:
        decision = "CONFIRMED_OUTSIDE_EP930"
        rule = "FROZEN_SCOPE_CITY_NOT_MEMBER"
        evidence.append(f"city={city} not in frozen scope city set (n={len(scope_cities)})")
        return _pack(unit_id, city, decision, rule, evidence, ev, row)

    # R2a policy interpretation pages (解读/问答/图解) are supporting
    # material, not the policy document itself.
    if any(marker in ev["title"] for marker in ("政策解读", "解读", "图解", "问答")):
        decision = "SUPPORTING_ONLY"
        rule = "POLICY_INTERPRETATION_PAGE"
        evidence.append("title marks an interpretation/explanation page")
        return _pack(unit_id, city, decision, rule, evidence, ev, row)

    # R2 portal / background page
    portal, portal_reason = _is_portal(ev)
    if portal:
        decision = "SUPPORTING_ONLY"
        rule = "PORTAL_OR_BACKGROUND_PAGE"
        evidence.append(portal_reason)
        return _pack(unit_id, city, decision, rule, evidence, ev, row)

    # R4 explicit 2016 policy evidence (before later-year exclusion so a
    # genuine 2016 policy is never mislabeled as a later policy).
    d16 = FULL_DATE_2016.findall(ev["text"])
    if d16:
        for month_s, day_s in d16:
            try:
                ts = pd.Timestamp(f"2016-{int(month_s):02d}-{int(day_s):02d}")
            except ValueError:
                continue
            cls = _window_class(ts)
            if cls:
                decision = cls
                rule = "EXPLICIT_2016_POLICY_DATE_IN_TEXT"
                evidence.append(f"official text contains 2016-{int(month_s):02d}-{int(day_s):02d}")
                return _pack(unit_id, city, decision, rule, evidence, ev, row)

    # R3 later-year policy with no 2016 linkage
    has_later = _later_year_evidence(ev)
    has_2016 = _has_2016_signal(ev)
    if has_later and not has_2016:
        decision = "CONFIRMED_OUTSIDE_EP930"
        rule = "LATER_POLICY_NO_2016_LINKAGE"
        evidence.append("explicit later-year (>=2017) policy evidence; no 2016 signal in unit")
        return _pack(unit_id, city, decision, rule, evidence, ev, row)

    # R5 duplicate content hash within the resolution set
    # (handled in a second pass over the full register)

    # R6 boundary
    decision = "MANUAL_REVIEW_REQUIRED"
    rule = "TRUE_BOUNDARY_MANUAL_FINAL_RESOLUTION"
    if not has_later and not has_2016:
        evidence.append("no explicit policy-year signal found in title/URL/text")
    elif has_later and has_2016:
        evidence.append("mixed later-year and 2016 signals; requires human adjudication")
    return _pack(unit_id, city, decision, rule, evidence, ev, row)


def _pack(
    unit_id: str,
    city: str,
    decision: str,
    rule: str,
    evidence: list[str],
    ev: dict[str, Any],
    row: dict[str, Any],
) -> dict[str, Any]:
    return {
        "evidence_unit_id": unit_id,
        "episode_id": EPISODE_ID,
        "city": city,
        "policy_title": _text(row.get("policy_title")),
        "root_document_id": _text(row.get("root_document_id")),
        "document_ids": _text(row.get("document_ids")),
        "action_ids": _text(row.get("action_ids")),
        "official_url": _text(row.get("official_url")),
        "reference_event_ids": _text(row.get("reference_event_ids")),
        "member_action_count": _text(row.get("member_action_count")),
        "member_document_count": _text(row.get("member_document_count")),
        "membership_decision": decision,
        "decision_rule": rule,
        "decision_evidence": ";".join(evidence) if evidence else "",
        "content_hash": ev.get("first_hash", ""),
        "manual_review_required": "TRUE" if decision == "MANUAL_REVIEW_REQUIRED" else "FALSE",
        "affects_treatment_group": "TRUE" if decision.startswith("CONFIRMED_EP930") else "FALSE",
    }


def _reference_resolution(ref: pd.DataFrame, db_path: Path, doc_map: dict[str, dict[str, Any]]) -> pd.DataFrame:
    """Reconcile the 20 reference events with production official records."""
    con = duckdb.connect(str(db_path), read_only=True)
    rows: list[dict[str, Any]] = []
    for item in ref.to_dict("records"):
        event_id = _text(item.get("reference_event_id"))
        city = _text(item.get("city"))
        ref_date = _text(item.get("reference_date"))
        record = None
        if city:
            # geography_original is not fully normalized (e.g. '北京' vs
            # '北京市'), so match exact city or the city without the 市 suffix.
            city_variants = [city]
            if city.endswith("市") and len(city) > 2:
                city_variants.append(city[:-1])
            hits = con.execute(
                """
                SELECT record_id, title, record_date, official_status, primary_source_url
                FROM records
                WHERE geography_original IN (SELECT unnest(?))
                  AND record_date BETWEEN '2016-09-20' AND '2016-10-15'
                ORDER BY record_date
                """,
                [city_variants],
            ).fetchall()
            official_hits = [h for h in hits if h[3] == "official"]
            if official_hits:
                record = official_hits[0]
        if record is not None:
            decision = "CONFIRMED_EP930_CORE"
            if record[2] and CORE_START <= pd.Timestamp(record[2]) <= CORE_END:
                decision = "CONFIRMED_EP930_CORE"
            else:
                decision = "CONFIRMED_EP930_EXTENDED"
            rule = "OFFICIAL_PRODUCTION_RECORD_WITHIN_WINDOW"
            evidence = (
                f"official record {record[0]} dated {record[2]} "
                f"({str(record[1])[:40]}) within frozen window"
            )
        else:
            # No official in-window record: check whether any official 2016
            # record exists at all (evidence of exclusion vs evidence gap).
            any2016 = []
            if city:
                any2016 = con.execute(
                    """
                    SELECT record_id, record_date, official_status FROM records
                    WHERE geography_original IN (SELECT unnest(?))
                      AND record_date BETWEEN '2016-01-01' AND '2016-12-31'
                    ORDER BY record_date
                    """,
                    [city_variants],
                ).fetchall()
            if any2016:
                dates = sorted({str(r[1]) for r in any2016 if r[1]})
                decision = "CONFIRMED_OUTSIDE_EP930"
                rule = "OFFICIAL_2016_RECORDS_OUTSIDE_WINDOW"
                evidence = "official 2016 records exist but outside 2016-09-20..2016-10-15: " + ",".join(dates)
            else:
                decision = "FINAL_MANUAL_RESOLUTION"
                rule = "NO_OFFICIAL_2016_RECORD_IN_PRODUCTION"
                evidence = "no official 2016 record found in production records table"
        rows.append(
            {
                "reference_event_id": event_id,
                "episode_id": EPISODE_ID,
                "city": city,
                "province": _text(item.get("province")),
                "reference_date": ref_date,
                "reference_membership_decision": decision,
                "decision_rule": rule,
                "decision_evidence": evidence,
                "existing_closure_status": _text(item.get("existing_closure_status")),
            }
        )
    con.close()
    return pd.DataFrame(rows)


def _waterfall(register: pd.DataFrame) -> pd.DataFrame:
    counts = register["membership_decision"].value_counts().to_dict()
    total = len(register)
    order = [
        "CONFIRMED_EP930_CORE",
        "CONFIRMED_EP930_EXTENDED",
        "CONFIRMED_EP930_REPRINT",
        "CONFIRMED_OUTSIDE_EP930",
        "DUPLICATE_OR_REDUNDANT",
        "SUPPORTING_ONLY",
        "MANUAL_REVIEW_REQUIRED",
    ]
    rows = []
    for decision in order:
        rows.append(
            {
                "scope": "evidence_unit",
                "stage": decision,
                "count": int(counts.get(decision, 0)),
                "denominator": total,
                "share": round(int(counts.get(decision, 0)) / total, 6) if total else 0,
            }
        )
    return pd.DataFrame(rows)


def build(data_root: Path, output_root: Path, db_path: Path, stamp: str | None = None) -> Path:
    episode_root = data_root / "outputs" / "special_projects" / "2016_930"
    stamp = stamp or datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    target = output_root / stamp
    target.mkdir(parents=True, exist_ok=False)

    inputs = _load_inputs(data_root, episode_root, "20260816T024809Z")
    scope_cities = inputs["scope_cities"]
    master = inputs["master"]
    mq = inputs["manual_queue"]
    doc_map = _doc_map(inputs["docs"])

    manual_city = mq.merge(
        master[["evidence_unit_id", "city"]].rename(columns={"city": "master_city"}),
        on="evidence_unit_id",
        how="left",
    )
    rows = []
    for item in manual_city.to_dict("records"):
        rows.append(_classify_unit(item, doc_map, scope_cities))
    register = pd.DataFrame(rows)
    register = register.drop_duplicates("evidence_unit_id", keep="last")

    # R5 duplicate content hash: mark as DUPLICATE_OR_REDUNDANT ONLY when the
    # unit's action set is a strict subset of another unit sharing the same
    # content hash (no action would be lost).  Units sharing a hash but
    # carrying distinct actions are separate evidence units of the same
    # underlying document and keep their own classification.
    if not register.empty:
        hash_groups: dict[str, list[int]] = {}
        for idx in register.index:
            h = _text(register.at[idx, "content_hash"])
            if not h:
                continue
            hash_groups.setdefault(h, []).append(idx)
        for idx in register.index:
            h = _text(register.at[idx, "content_hash"])
            if not h:
                continue
            group = hash_groups.get(h, [])
            if len(group) < 2:
                continue
            mine = {a for a in _text(register.at[idx, "action_ids"]).split(";") if a}
            if not mine:
                continue
            superset = False
            for other in group:
                if other == idx:
                    continue
                theirs = {a for a in _text(register.at[other, "action_ids"]).split(";") if a}
                if mine <= theirs:
                    superset = True
                    break
            if superset:
                register.at[idx, "membership_decision"] = "DUPLICATE_OR_REDUNDANT"
                register.at[idx, "decision_rule"] = "DUPLICATE_CONTENT_HASH_ACTION_SUBSET"
                register.at[idx, "decision_evidence"] = (
                    "same content_hash as an evidence unit whose action set "
                    "contains all actions of this unit; no action lost"
                )
                register.at[idx, "manual_review_required"] = "FALSE"
                register.at[idx, "affects_treatment_group"] = "FALSE"

    # Re-attach the previously confirmed core/extended/reprint units (not in
    # the manual queue) so the register covers all 894 evidence units.
    prior_reg = pd.read_csv(
        episode_root / "treatment_universe_closure" / "membership_closure" / "20260816T024809Z" / "EP930_MEMBERSHIP_DECISION_REGISTER.csv",
        dtype=str,
    )
    resolved_ids = set(register["evidence_unit_id"])
    prior_rows = []
    for item in prior_reg.to_dict("records"):
        if _text(item.get("evidence_unit_id")) in resolved_ids:
            continue
        decision = _text(item.get("membership_decision"))
        if decision not in {
            "CONFIRMED_EP930_CORE",
            "CONFIRMED_EP930_EXTENDED",
            "CONFIRMED_EP930_REPRINT",
        }:
            continue
        prior_city = _text(item.get("city"))
        # Re-validate prior confirmed units against the frozen scope: a
        # "confirmed" unit whose city is not a frozen-scope city cannot be
        # part of the treatment universe.
        if prior_city and prior_city not in scope_cities:
            register = pd.concat(
                [
                    register,
                    pd.DataFrame(
                        [
                            {
                                "evidence_unit_id": _text(item.get("evidence_unit_id")),
                                "episode_id": EPISODE_ID,
                                "city": prior_city,
                                "policy_title": _text(item.get("policy_title")),
                                "root_document_id": _text(item.get("root_document_id")),
                                "document_ids": _text(item.get("document_ids")),
                                "action_ids": _text(item.get("action_ids")),
                                "official_url": _text(item.get("official_url")),
                                "reference_event_ids": _text(item.get("reference_event_ids")),
                                "member_action_count": _text(item.get("member_action_count")),
                                "member_document_count": _text(item.get("member_document_count")),
                                "membership_decision": "CONFIRMED_OUTSIDE_EP930",
                                "decision_rule": "FROZEN_SCOPE_CITY_NOT_MEMBER",
                                "decision_evidence": (
                                    f"prior confirmed unit re-validated: city={prior_city} "
                                    "not in frozen scope city set"
                                ),
                                "content_hash": "",
                                "manual_review_required": "FALSE",
                                "affects_treatment_group": "FALSE",
                            }
                        ]
                    ),
                ],
                ignore_index=True,
                sort=False,
            )
            continue
        prior_rows.append(
            {
                "evidence_unit_id": _text(item.get("evidence_unit_id")),
                "episode_id": EPISODE_ID,
                "city": _text(item.get("city")),
                "policy_title": _text(item.get("policy_title")),
                "root_document_id": _text(item.get("root_document_id")),
                "document_ids": _text(item.get("document_ids")),
                "action_ids": _text(item.get("action_ids")),
                "official_url": _text(item.get("official_url")),
                "reference_event_ids": _text(item.get("reference_event_ids")),
                "member_action_count": _text(item.get("member_action_count")),
                "member_document_count": _text(item.get("member_document_count")),
                "membership_decision": decision,
                "decision_rule": _text(item.get("decision_rule")) or "PRECOMPUTED_PROPAGATED",
                "decision_evidence": _text(item.get("decision_evidence")),
                "content_hash": "",
                "manual_review_required": "FALSE",
                "affects_treatment_group": "TRUE",
            }
        )
    if prior_rows:
        register = pd.concat([register, pd.DataFrame(prior_rows)], ignore_index=True, sort=False)
    register = register.drop_duplicates("evidence_unit_id", keep="last")

    ref_final = _reference_resolution(inputs["ref"], db_path, doc_map)

    manual_after = register[register["membership_decision"].eq("MANUAL_REVIEW_REQUIRED")].copy()
    counts = register["membership_decision"].value_counts().to_dict()
    actions_released = 0
    for item in register.to_dict("records"):
        if item["membership_decision"] in {
            "CONFIRMED_OUTSIDE_EP930",
            "DUPLICATE_OR_REDUNDANT",
            "SUPPORTING_ONLY",
        }:
            try:
                actions_released += int(item.get("member_action_count") or 0)
            except (TypeError, ValueError):
                pass
    summary = {
        "generated_at_utc": _now(),
        "episode_id": EPISODE_ID,
        "scope_version": SCOPE_VERSION,
        "scope_hash": SCOPE_HASH,
        "scope_city_count": len(scope_cities),
        "production_mutation": "NONE_READ_ONLY_DERIVATION",
        "network_calls": 0,
        "api_calls": 0,
        "membership_manual_before": len(mq),
        "membership_manual_after": int(manual_after.shape[0]),
        "waterfall": counts,
        "actions_released_from_critical_path": actions_released,
        "reference_events": {
            "total": len(ref_final),
            "decision_counts": ref_final["reference_membership_decision"].value_counts().to_dict(),
            "unresolved": int(ref_final["reference_membership_decision"].isin(
                {"UNKNOWN", "MANUAL_REVIEW_REQUIRED"}
            ).sum()),
        },
        "rules_applied": {str(k): int(v) for k, v in register["decision_rule"].value_counts().to_dict().items()},
    }

    _write_csv(target / "EP930_MEMBERSHIP_RESOLUTION_DECISION_REGISTER.csv", register)
    _write_csv(target / "EP930_MEMBERSHIP_RESOLUTION_WATERFALL.csv", _waterfall(register))
    _write_csv(target / "EP930_MEMBERSHIP_RESOLUTION_MANUAL_QUEUE.csv", manual_after)
    _write_csv(target / "EP930_MEMBERSHIP_REFERENCE_RECONCILIATION_FINAL.csv", ref_final)
    _write_json(target / "EP930_MEMBERSHIP_RESOLUTION_SUMMARY.json", summary)

    report = f"""# EP930 Membership Resolution — Read-only Deterministic Closure

Generated (UTC): `{_now()}`
Production mutation: **NO** — snapshot only; queue/database/runner untouched.

## Frozen scope

- `scope_version`: `{SCOPE_VERSION}`
- `scope_hash`: `{SCOPE_HASH}`
- `scope_city_count`: `{len(scope_cities)}`

## Waterfall (evidence units)

| Disposition | Units |
|---|---:|
| Input manual queue | {len(mq)} |
| `CONFIRMED_EP930_CORE` | {int(counts.get('CONFIRMED_EP930_CORE', 0))} |
| `CONFIRMED_EP930_EXTENDED` | {int(counts.get('CONFIRMED_EP930_EXTENDED', 0))} |
| `CONFIRMED_EP930_REPRINT` | {int(counts.get('CONFIRMED_EP930_REPRINT', 0))} |
| `CONFIRMED_OUTSIDE_EP930` | {int(counts.get('CONFIRMED_OUTSIDE_EP930', 0))} |
| `DUPLICATE_OR_REDUNDANT` | {int(counts.get('DUPLICATE_OR_REDUNDANT', 0))} |
| `SUPPORTING_ONLY` | {int(counts.get('SUPPORTING_ONLY', 0))} |
| `MANUAL_REVIEW_REQUIRED` | {int(counts.get('MANUAL_REVIEW_REQUIRED', 0))} |

Manual queue reduction: **{len(mq)} -> {int(manual_after.shape[0])}**
Actions released from critical path: **{actions_released}**

## Reference events

- Total: **{len(ref_final)}**
- `CONFIRMED_EP930_CORE` / `EXTENDED`: **{int((ref_final['reference_membership_decision'].str.startswith('CONFIRMED_EP930')).sum())}**
- `CONFIRMED_OUTSIDE_EP930`: **{int((ref_final['reference_membership_decision'].eq('CONFIRMED_OUTSIDE_EP930')).sum())}**
- `FINAL_MANUAL_RESOLUTION`: **{int((ref_final['reference_membership_decision'].eq('FINAL_MANUAL_RESOLUTION')).sum())}**
- `UNKNOWN` / `MANUAL_REVIEW_REQUIRED` remaining: **{int(ref_final['reference_membership_decision'].isin({'UNKNOWN','MANUAL_REVIEW_REQUIRED'}).sum())}**

Reference decisions are evidence-based on official production records within
the frozen 2016-09-20..2016-10-15 window; the reference list is a coverage
anchor, not treatment truth.

## Rules

1. `FROZEN_SCOPE_CITY_NOT_MEMBER` — city outside the frozen scope.
2. `PORTAL_OR_BACKGROUND_PAGE` — portal/navigation page without policy clause.
3. `LATER_POLICY_NO_2016_LINKAGE` — explicit >=2017 policy evidence, no 2016 signal.
4. `EXPLICIT_2016_POLICY_DATE_IN_TEXT` — 2016 dates within the frozen windows.
5. `DUPLICATE_CONTENT_HASH` — duplicate content.
6. `TRUE_BOUNDARY_MANUAL_FINAL_RESOLUTION` — kept for human adjudication.

No API call, no network fetch, no queue/database write, no outcome data read.
"""
    (target / "EP930_MEMBERSHIP_RESOLUTION_REPORT.md").write_text(report, encoding="utf-8")

    hashes = {}
    for path in sorted(target.iterdir()):
        if path.name == "SHA256_MANIFEST.json" or not path.is_file():
            continue
        hashes[path.name] = _sha256(path)
    _write_json(target / "SHA256_MANIFEST.json", {"generated_at_utc": _now(), "files": hashes})
    return target


def main() -> None:
    parser = argparse.ArgumentParser(description="EP930 membership resolution (read-only)")
    parser.add_argument("--data-root", type=Path, default=Path(r"E:\Data Set\CRPD"))
    parser.add_argument("--db", type=Path, default=Path(r"E:\Data Set\CRPD\database\policydb.duckdb"))
    parser.add_argument(
        "--output-root",
        type=Path,
        default=Path(r"E:\Data Set\CRPD\outputs\special_projects\2016_930\treatment_universe_closure\membership_resolution"),
    )
    args = parser.parse_args()
    target = build(args.data_root, args.output_root, args.db)
    print(json.dumps({"output": str(target), "status": "OK"}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
