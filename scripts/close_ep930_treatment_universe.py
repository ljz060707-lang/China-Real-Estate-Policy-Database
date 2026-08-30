"""Close the deterministic part of the EP930 treatment-universe audit.

This is a read-only, snapshot-producing stage.  It consumes Curated episode
artifacts, does not call the network or AI provider, does not promote actions,
and never mutates the production queue or database.  Deterministic exclusions
are kept separate from evidence-recovery and manual-review states.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from collections import Counter
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.build_ep930_econometric_grade import (
    ALLOWED_POLICY_TYPES,
    EPISODE_ID,
    FORMAL_API_STATUSES,
    OFFICIAL_EVIDENCE_STATUSES,
    _date_value,
    _safe_read_json,
    _safe_read_parquet,
    _sha256,
    _text,
    _truthy,
    _write_csv,
    _write_json,
    _write_xlsx,
)

CORE_START = pd.Timestamp("2016-09-25")
CORE_END = pd.Timestamp("2016-10-10")
EXTENDED_START = pd.Timestamp("2016-09-20")
EXTENDED_END = pd.Timestamp("2016-10-15")

TRIAGE_STATES = (
    "KEEP_FOR_EP930_REVIEW",
    "EXCLUDE_WRONG_YEAR",
    "EXCLUDE_2017_POLICY",
    "EXCLUDE_LATER_POLICY",
    "EXCLUDE_PRE_EPISODE",
    "EXCLUDE_OUTSIDE_EPISODE",
    "EXCLUDE_NOT_TIGHTENING",
    "EXCLUDE_NOT_RELEVANT_HOUSING_POLICY",
    "EXCLUDE_DUPLICATE",
    "EXCLUDE_BACKGROUND_ONLY",
    "NEEDS_EVIDENCE",
    "MANUAL_REVIEW_REQUIRED",
)

DATE_STATES = (
    "EXPLICIT_EFFECTIVE_DATE",
    "PUBLICATION_DATE_EFFECTIVE",
    "ACTION_SPECIFIC_EFFECTIVE_DATE",
    "NO_EXPLICIT_EFFECTIVE_DATE",
    "OFFICIAL_REPRINT_DATE_NOT_POLICY_DATE",
    "DATE_CONFLICT_RESOLVED",
    "MANUAL_REVIEW_REQUIRED",
)

EVIDENCE_TRIAGE_CLASSES = (
    "HIGH_PRIORITY_EVIDENCE_REVIEW",
    "MEMBERSHIP_REVIEW",
    "OFFICIAL_RECOVERY_REQUIRED",
    "OFFICIAL_REPRINT_OF_2016_POLICY",
    "EXCLUDE_LATER_POLICY",
    "EXCLUDE_OUTSIDE_EPISODE",
    "DETERMINISTIC_EXCLUSION",
)

REFERENCE_CLOSURE_STATUSES = (
    "CONFIRMED_INCLUDED",
    "CONFIRMED_EXCLUDED",
    "INSUFFICIENT_EVIDENCE",
    "MANUAL_REVIEW_REQUIRED",
)

HOUSING_POLICY_MARKERS = (
    "住房",
    "房屋",
    "房地产",
    "购房",
    "限购",
    "限售",
    "首付",
    "首付款",
    "贷款",
    "认房认贷",
    "公积金",
    "商品房",
    "住宅",
    "预售",
    "房价",
    "土地供应",
    "住宅用地",
    "价格备案",
)

BACKGROUND_MARKERS = (
    "公共数据开放网",
    "数据目录",
    "个人注册",
    "法人注册",
    "办件进度",
    "政务服务网",
    "网络支持",
    "ICP备",
    "互动交流",
    "统一身份认证",
    "小程序",
    "下载客户端",
    "网站标识码",
)

REPRINT_MARKERS = ("转载", "转发", "原文", "旧文", "2016年")
YEAR_PATTERN = re.compile(r"(?<!\d)(20(?:0\d|1\d|2\d|3\d))(?!\d)")
FULL_DATE_PATTERN = re.compile(
    r"2016(?:[-/.年](\d{1,2})[-/.月](\d{1,2})日?)"
)


def _first_date(row: Mapping[str, Any], names: tuple[str, ...]) -> str | None:
    for name in names:
        value = _date_value(row.get(name))
        if value:
            return value
    return None


def _date_timestamp(value: str | None) -> pd.Timestamp | None:
    if not value:
        return None
    parsed = pd.to_datetime(value, errors="coerce")
    return None if pd.isna(parsed) else pd.Timestamp(parsed)


def _years(text: str) -> list[int]:
    return sorted({int(value) for value in YEAR_PATTERN.findall(text)})


def _text_date(text: str) -> str | None:
    match = FULL_DATE_PATTERN.search(text)
    if not match:
        return None
    month, day = int(match.group(1)), int(match.group(2))
    try:
        return f"2016-{month:02d}-{day:02d}"
    except ValueError:
        return None


def _json_list(value: Any) -> list[str]:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return []
    if isinstance(value, list):
        return [str(item) for item in value]
    try:
        loaded = json.loads(str(value))
        return [str(item) for item in loaded] if isinstance(loaded, list) else []
    except (TypeError, json.JSONDecodeError):
        return []


def _accepted_official(row: Mapping[str, Any]) -> bool:
    status = _text(row.get("doc_official_evidence_status")).upper()
    source = _truthy(row.get("doc_official_source"))
    return status in OFFICIAL_EVIDENCE_STATUSES or source


def _load_joined(data_root: Path, episode_root: Path) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    curated = data_root / "curated"
    actions = _safe_read_parquet(curated / "policy_episode_actions.parquet")
    documents = _safe_read_parquet(curated / "policy_episode_documents.parquet")
    gaps = _safe_read_parquet(curated / "policy_episode_gaps.parquet")
    api = _safe_read_parquet(episode_root / "05_API_CLASSIFICATION" / "2016_930_API_CLASSIFICATION.parquet")
    if "episode_id" in actions.columns:
        actions = actions[actions["episode_id"].astype(str).eq(EPISODE_ID)].copy()
    if "episode_id" in documents.columns:
        documents = documents[documents["episode_id"].astype(str).eq(EPISODE_ID)].copy()
    if "episode_id" in gaps.columns:
        gaps = gaps[gaps["episode_id"].astype(str).eq(EPISODE_ID)].copy()
    if "episode_id" in api.columns:
        api = api[api["episode_id"].astype(str).eq(EPISODE_ID)].copy()

    doc_columns = [
        "document_id",
        "document_title",
        "document_number",
        "issuer",
        "official_url",
        "canonical_url",
        "official_source",
        "official_evidence_status",
        "publication_date",
        "announcement_date",
        "effective_date",
        "date_evidence_text",
        "content_hash",
        "record_id",
        "city",
        "province",
    ]
    available = [column for column in doc_columns if column in documents.columns]
    docs = documents.loc[:, available].copy()
    docs = docs.drop_duplicates("document_id", keep="last") if "document_id" in docs.columns else docs
    docs = docs.rename(
        columns={
            "document_title": "doc_title",
            "document_number": "doc_number",
            "issuer": "doc_issuer",
            "official_url": "doc_official_url",
            "canonical_url": "doc_canonical_url",
            "official_source": "doc_official_source",
            "official_evidence_status": "doc_official_evidence_status",
            "publication_date": "doc_publication_date",
            "announcement_date": "doc_announcement_date",
            "effective_date": "doc_effective_date",
            "date_evidence_text": "doc_date_evidence_text",
            "content_hash": "doc_content_hash",
            "record_id": "doc_record_id",
            "city": "doc_city",
            "province": "doc_province",
        }
    )
    if not actions.empty and "document_id" in actions.columns and "document_id" in docs.columns:
        actions = actions.merge(docs, on="document_id", how="left")
    return actions, documents, gaps, api


def _api_status_maps(api: pd.DataFrame) -> tuple[dict[str, str], dict[str, str]]:
    pass1: dict[str, str] = {}
    pass2: dict[str, str] = {}
    if api.empty or "action_id" not in api.columns:
        return pass1, pass2
    for row in api.to_dict("records"):
        action_id = _text(row.get("action_id"))
        if not action_id:
            continue
        status = _text(row.get("status")).upper() or "NOT_COMPLETED"
        target = pass1 if _text(row.get("pass_name")).lower() in {"first_pass", "pass1"} else pass2
        previous = target.get(action_id)
        if previous not in FORMAL_API_STATUSES or status in FORMAL_API_STATUSES:
            target[action_id] = status
    return pass1, pass2


def _evidence_triage_class(row: Mapping[str, Any]) -> str:
    """Assign a deterministic evidence lane without API or network calls."""

    state = _text(row.get("triage_state"))
    if state in {"EXCLUDE_2017_POLICY", "EXCLUDE_LATER_POLICY"}:
        return "EXCLUDE_LATER_POLICY"
    if state in {
        "EXCLUDE_WRONG_YEAR",
        "EXCLUDE_PRE_EPISODE",
        "EXCLUDE_OUTSIDE_EPISODE",
    }:
        return "EXCLUDE_OUTSIDE_EPISODE"
    if state.startswith("EXCLUDE_"):
        return "DETERMINISTIC_EXCLUSION"
    if _truthy(row.get("official_reprint_of_2016_policy")):
        return "OFFICIAL_REPRINT_OF_2016_POLICY"
    if state == "KEEP_FOR_EP930_REVIEW":
        if _truthy(row.get("official_evidence")) and _text(row.get("underlying_policy_date")):
            return "HIGH_PRIORITY_EVIDENCE_REVIEW"
        return "MEMBERSHIP_REVIEW"
    if state == "MANUAL_REVIEW_REQUIRED":
        return "MEMBERSHIP_REVIEW"
    if state == "NEEDS_EVIDENCE":
        if _truthy(row.get("official_evidence")) and _text(row.get("underlying_policy_date")):
            return "HIGH_PRIORITY_EVIDENCE_REVIEW"
        if _truthy(row.get("official_evidence")):
            return "MEMBERSHIP_REVIEW"
        return "OFFICIAL_RECOVERY_REQUIRED"
    return "MEMBERSHIP_REVIEW"


def classify_action(row: Mapping[str, Any], gap_action_ids: set[str], gap_document_ids: set[str]) -> dict[str, Any]:
    action_id = _text(row.get("action_id"))
    document_id = _text(row.get("document_id"))
    action_text = _text(row.get("action_text"))
    title = _text(row.get("doc_title")) or _text(row.get("document_title"))
    date_evidence = _text(row.get("date_evidence_text")) or _text(row.get("doc_date_evidence_text"))
    combined = " ".join(value for value in (title, action_text, date_evidence) if value)
    action_date = _first_date(row, ("announcement_date", "publication_date"))
    # A reprint page's publication date is page metadata, not the underlying
    # policy date.  Only an action/document announcement date can establish
    # the underlying event without explicit text evidence.
    document_date = _first_date(row, ("doc_announcement_date",))
    underlying_date = action_date or document_date
    webpage_publish_date = _first_date(row, ("doc_publication_date", "publication_date"))
    effective_date = _first_date(row, ("effective_date", "doc_effective_date"))
    implementation_date = _first_date(row, ("implementation_date",))
    text_date = _text_date(combined)
    years = _years(combined)
    direction = _text(row.get("action_direction")).upper()
    policy_type = _text(row.get("policy_type")).upper()
    if policy_type == "PF_OTHER":
        policy_type = "PF_OTHER_CONDITIONS"
    official = _accepted_official(row)
    housing_signal = any(marker in combined for marker in HOUSING_POLICY_MARKERS)
    background_signal = any(marker in combined for marker in BACKGROUND_MARKERS)
    duplicate = _text(row.get("dedup_status")).lower() in {"duplicate_action", "duplicate", "noncanonical"}
    reasons: list[str] = []
    state: str | None = None
    membership_class = "UNKNOWN"
    reprint = False
    underlying_policy_date = underlying_date

    # Hard deterministic exclusions are intentionally ordered before evidence gaps.
    if duplicate:
        state = "EXCLUDE_DUPLICATE"
        reasons.append("dedup_status_marks_duplicate")
    elif not housing_signal and background_signal:
        state = "EXCLUDE_BACKGROUND_ONLY"
        reasons.append("portal_or_background_page_without_policy_clause")
    elif not housing_signal:
        state = "EXCLUDE_NOT_RELEVANT_HOUSING_POLICY"
        reasons.append("no_deterministic_housing_policy_signal")
    elif direction == "SUPPORTIVE":
        state = "EXCLUDE_NOT_TIGHTENING"
        reasons.append("direction_is_supportive")

    underlying_ts = _date_timestamp(underlying_policy_date)
    if state is None and underlying_ts is not None:
        if underlying_ts.year == 2017:
            state = "EXCLUDE_2017_POLICY"
            reasons.append("underlying_policy_year_2017")
        elif underlying_ts.year > 2017:
            state = "EXCLUDE_LATER_POLICY"
            reasons.append("underlying_policy_year_after_2017")
        elif underlying_ts.year < 2016:
            state = "EXCLUDE_WRONG_YEAR"
            reasons.append("underlying_policy_year_before_2016")
        elif underlying_ts < EXTENDED_START:
            state = "EXCLUDE_PRE_EPISODE"
            reasons.append("date_before_extended_episode_window")
        elif underlying_ts > EXTENDED_END:
            state = "EXCLUDE_OUTSIDE_EPISODE"
            reasons.append("date_after_extended_episode_window")
        else:
            membership_class = "CORE_WINDOW" if CORE_START <= underlying_ts <= CORE_END else "EXTENDED_WINDOW"

    if state is None and underlying_ts is None:
        later_years = [year for year in years if year > 2016]
        earlier_years = [year for year in years if year < 2016]
        webpage_ts = _date_timestamp(webpage_publish_date)
        if (
            2016 in years
            and official
            and webpage_ts is not None
            and webpage_ts.year > 2016
            and any(marker in combined for marker in REPRINT_MARKERS)
            and text_date
        ):
            reprint = True
            underlying_policy_date = text_date
            underlying_ts = _date_timestamp(text_date)
            membership_class = "OFFICIAL_REPRINT_OF_2016_POLICY"
        elif 2016 in years and later_years:
            if official and any(marker in combined for marker in REPRINT_MARKERS) and text_date:
                reprint = True
                underlying_policy_date = text_date
                underlying_ts = _date_timestamp(text_date)
                membership_class = "OFFICIAL_REPRINT_OF_2016_POLICY"
            else:
                state = "MANUAL_REVIEW_REQUIRED"
                reasons.append("mixed_2016_and_later_year_signals")
        elif len(later_years) == 1:
            year = later_years[0]
            state = "EXCLUDE_2017_POLICY" if year == 2017 else "EXCLUDE_LATER_POLICY"
            reasons.append(f"text_year_signal_{year}")
        elif earlier_years and 2016 not in years:
            state = "EXCLUDE_PRE_EPISODE"
            reasons.append("text_year_signal_before_2016")
        elif 2016 in years:
            state = "MANUAL_REVIEW_REQUIRED"
            reasons.append("2016_signal_without_exact_underlying_date")
        else:
            reasons.append("missing_underlying_policy_date")

    if state is None and underlying_ts is not None:
        membership_class = "CORE_WINDOW" if CORE_START <= underlying_ts <= CORE_END else membership_class
        if direction == "TIGHTENING" and action_text and policy_type in ALLOWED_POLICY_TYPES:
            state = "KEEP_FOR_EP930_REVIEW"
        else:
            if direction != "TIGHTENING":
                reasons.append("direction_unknown_requires_evidence")
            if not action_text:
                reasons.append("empty_action_text")
            if policy_type not in ALLOWED_POLICY_TYPES:
                reasons.append("policy_type_not_in_episode_taxonomy")
            state = "NEEDS_EVIDENCE"

    if state is None:
        if direction != "TIGHTENING":
            reasons.append("direction_unknown_requires_evidence")
        if not action_text:
            reasons.append("empty_action_text")
        if not official:
            reasons.append("official_evidence_not_yet_closed")
        if not _text(row.get("geographic_scope")):
            reasons.append("geographic_scope_not_yet_closed")
        state = "NEEDS_EVIDENCE"

    if state in {"KEEP_FOR_EP930_REVIEW", "MANUAL_REVIEW_REQUIRED", "NEEDS_EVIDENCE"}:
        if not official:
            reasons.append("official_evidence_not_yet_closed")
        if not _text(row.get("geographic_scope")):
            reasons.append("geographic_scope_not_yet_closed")
    reasons = list(dict.fromkeys(reasons))
    result_row = {
        "triage_state": state,
        "official_reprint_of_2016_policy": reprint,
        "official_evidence": official,
        "underlying_policy_date": underlying_policy_date,
    }
    evidence_triage = _evidence_triage_class(result_row)
    if reprint:
        date_state = "OFFICIAL_REPRINT_DATE_NOT_POLICY_DATE"
    elif effective_date:
        date_state = "EXPLICIT_EFFECTIVE_DATE"
    elif implementation_date:
        date_state = "ACTION_SPECIFIC_EFFECTIVE_DATE"
    elif underlying_policy_date:
        date_state = "NO_EXPLICIT_EFFECTIVE_DATE"
    else:
        date_state = "MANUAL_REVIEW_REQUIRED"
    return {
        "episode_id": EPISODE_ID,
        "document_id": document_id,
        "action_id": action_id,
        "record_id": _text(row.get("record_id")) or _text(row.get("doc_record_id")),
        "city_id": _text(row.get("city_id")) or _text(row.get("doc_city_id")),
        "city": _text(row.get("city")) or _text(row.get("doc_city")),
        "province": _text(row.get("province")) or _text(row.get("doc_province")),
        "title": title,
        "action_text": action_text,
        "policy_type": policy_type,
        "direction": direction,
        "geographic_scope": _text(row.get("geographic_scope")),
        "webpage_publish_date": webpage_publish_date,
        "underlying_policy_date": underlying_policy_date,
        "announcement_date": action_date,
        "publication_date": _first_date(row, ("publication_date", "doc_publication_date")),
        "effective_date": effective_date,
        "implementation_date": implementation_date,
        "date_state": date_state,
        "date_evidence": date_evidence,
        "date_confidence": _text(row.get("date_confidence")),
        "date_basis": "OFFICIAL_REPRINT_OF_2016_POLICY" if reprint else "CURATED_ACTION_OR_DOCUMENT_DATE" if underlying_policy_date else "MISSING_DATE_EVIDENCE",
        "official_reprint_of_2016_policy": reprint,
        "official_evidence_status": _text(row.get("doc_official_evidence_status")) or "UNRESOLVED",
        "official_evidence": official,
        "official_url": _text(row.get("doc_official_url")) or _text(row.get("official_url")),
        "content_hash": _text(row.get("doc_content_hash")) or _text(row.get("content_hash")),
        "dedup_status": _text(row.get("dedup_status")),
        "gap_linked": action_id in gap_action_ids or document_id in gap_document_ids,
        "membership_class": membership_class,
        "triage_state": state,
        "triage_reason": ";".join(reasons),
        "manual_review_required": state == "MANUAL_REVIEW_REQUIRED",
        "evidence_recovery_required": state == "NEEDS_EVIDENCE",
        "api_eligible_after_triage": state == "KEEP_FOR_EP930_REVIEW",
        "evidence_triage_class": evidence_triage,
    }


def build_triage(actions: pd.DataFrame, gaps: pd.DataFrame) -> pd.DataFrame:
    gap_actions = set(gaps.get("action_id", pd.Series(dtype=str)).dropna().astype(str))
    gap_documents = set(gaps.get("document_id", pd.Series(dtype=str)).dropna().astype(str))
    if actions.empty:
        return pd.DataFrame()
    rows = [classify_action(row, gap_actions, gap_documents) for row in actions.to_dict("records")]
    result = pd.DataFrame(rows)
    return result.sort_values(["triage_state", "city", "document_id", "action_id"], na_position="last").reset_index(drop=True)


def _reference_closure_status(resolution: str, membership: str) -> str:
    """Normalize evidence resolution into the four auditable closure states."""

    if resolution in REFERENCE_CLOSURE_STATUSES:
        return resolution
    if resolution == "MATCHED_WITH_DATE_RECONCILIATION":
        return "CONFIRMED_INCLUDED"
    if membership in REFERENCE_CLOSURE_STATUSES:
        return membership
    return "MANUAL_REVIEW_REQUIRED"


def build_reference_reconciliation(discovery: pd.DataFrame, triage: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    if discovery.empty:
        return pd.DataFrame()
    for reference in discovery.to_dict("records"):
        city = _text(reference.get("city"))
        city_rows = triage[triage["city"].eq(city)] if not triage.empty else pd.DataFrame()
        matched = city_rows[city_rows["triage_state"].eq("KEEP_FOR_EP930_REVIEW")] if not city_rows.empty else pd.DataFrame()
        matched_actions = matched["action_id"].dropna().astype(str).tolist() if not matched.empty else []
        matched_documents = matched["document_id"].dropna().astype(str).unique().tolist() if not matched.empty else []
        official = bool(not matched.empty and matched["official_evidence"].any())
        reference_date = _date_value(reference.get("earliest_known_policy_date"))
        if matched_actions:
            resolution = "MATCHED_WITH_DATE_RECONCILIATION"
            membership = "CONFIRMED_INCLUDED"
            confidence = "MEDIUM"
            manual = False
        elif _truthy(reference.get("official_policy_found")):
            resolution = "INSUFFICIENT_EVIDENCE"
            membership = "MANUAL_REVIEW_REQUIRED"
            confidence = "LOW"
            manual = True
        else:
            resolution = "MANUAL_REVIEW_REQUIRED"
            membership = "MANUAL_REVIEW_REQUIRED"
            confidence = "LOW"
            manual = True
        closure_status = _reference_closure_status(resolution, membership)
        rows.append(
            {
                "reference_event_id": "REF930_" + hashlib.sha256(city.encode()).hexdigest()[:16].upper(),
                "episode_id": EPISODE_ID,
                "city": city,
                "province": _text(reference.get("province")),
                "reference_date": reference_date,
                "reference_policy_type": ";".join(_json_list(reference.get("expected_policy_types"))),
                "reference_source": "01_DISCOVERY/2016_930_CITY_DISCOVERY.parquet",
                "matched_document_id": ";".join(matched_documents),
                "matched_action_ids": ";".join(matched_actions),
                "official_evidence": official,
                "announcement_date": reference_date,
                "effective_date": "",
                "episode_membership": membership,
                "resolution_status": resolution,
                "closure_status": closure_status,
                "resolution_reason": ";".join(
                    [
                        "reference_city_has_relevant_triage_match" if matched_actions else "no_relevant_action_match",
                        "official_evidence_observed" if official else "official_evidence_not_closed",
                    ]
                ),
                "confidence": confidence,
                "manual_review_required": manual,
                "reference_official_policy_found": _truthy(reference.get("official_policy_found")),
                "reference_policy_count": reference.get("official_policy_count"),
            }
        )
    return pd.DataFrame(rows)


def _split_action_ids(value: object) -> list[str]:
    return [item for item in _text(value).split(";") if item]


def build_reference_recovery_queue(
    reference: pd.DataFrame,
    triage: pd.DataFrame,
) -> pd.DataFrame:
    """Create a bounded, auditable queue for only the unresolved anchors."""

    columns = [
        "reference_event_id",
        "episode_id",
        "city",
        "province",
        "reference_date",
        "reference_policy_type",
        "status",
        "priority",
        "recovery_class",
        "candidate_action_ids",
        "matched_action_ids",
        "resolution_status",
        "closure_status",
        "resolution_reason",
        "official_evidence",
        "manual_review_required",
    ]
    if reference.empty:
        return pd.DataFrame(columns=columns)
    rows: list[dict[str, Any]] = []
    for item in reference.to_dict("records"):
        if not _truthy(item.get("manual_review_required")):
            continue
        city = _text(item.get("city"))
        candidates = triage[
            triage["city"].eq(city)
            & triage["triage_state"].isin(
                ["KEEP_FOR_EP930_REVIEW", "NEEDS_EVIDENCE", "MANUAL_REVIEW_REQUIRED"]
            )
        ] if not triage.empty else pd.DataFrame()
        candidate_ids = candidates["action_id"].dropna().astype(str).tolist() if not candidates.empty else []
        official = _truthy(item.get("official_evidence"))
        recovery_class = "MEMBERSHIP_REVIEW" if official else "OFFICIAL_RECOVERY_REQUIRED"
        rows.append(
            {
                "reference_event_id": _text(item.get("reference_event_id")),
                "episode_id": EPISODE_ID,
                "city": city,
                "province": _text(item.get("province")),
                "reference_date": _text(item.get("reference_date")),
                "reference_policy_type": _text(item.get("reference_policy_type")),
                "status": "RECOVERY_REQUIRED",
                "priority": 0,
                "recovery_class": recovery_class,
                "candidate_action_ids": ";".join(candidate_ids),
                "matched_action_ids": _text(item.get("matched_action_ids")),
                "resolution_status": _text(item.get("resolution_status")) or "MANUAL_REVIEW_REQUIRED",
                "closure_status": _text(item.get("closure_status")) or "MANUAL_REVIEW_REQUIRED",
                "resolution_reason": _text(item.get("resolution_reason")),
                "official_evidence": official,
                "manual_review_required": True,
            }
        )
    return pd.DataFrame(rows, columns=columns)


def build_api_fast_lane_plan(
    triage: pd.DataFrame,
    reference: pd.DataFrame,
) -> pd.DataFrame:
    """Rank only reference/membership-relevant actions for later API work.

    This is a scheduling overlay.  It never changes treatment membership,
    promotes an action, or treats an AI result as verified evidence.
    """

    columns = [
        "episode_id",
        "action_id",
        "document_id",
        "record_id",
        "city",
        "province",
        "official_url",
        "content_hash",
        "triage_state",
        "evidence_triage_class",
        "priority",
        "priority_reason",
        "reference_event_ids",
    ]
    if triage.empty:
        return pd.DataFrame(columns=columns)
    matched_by_action: dict[str, set[str]] = {}
    unresolved_cities: set[str] = set()
    if not reference.empty:
        for item in reference.to_dict("records"):
            event_id = _text(item.get("reference_event_id"))
            action_ids = _split_action_ids(item.get("matched_action_ids"))
            if _truthy(item.get("manual_review_required")):
                unresolved_cities.add(_text(item.get("city")))
            for action_id in action_ids:
                matched_by_action.setdefault(action_id, set()).add(event_id)

    selected: dict[str, dict[str, Any]] = {}

    def add(item: Mapping[str, Any], priority: int, reason: str, events: set[str]) -> None:
        action_id = _text(item.get("action_id"))
        if not action_id:
            return
        current = selected.get(action_id)
        if current is not None and int(current["priority"]) < priority:
            current["reference_event_ids"] = ";".join(
                sorted(set(_split_action_ids(current.get("reference_event_ids"))) | events)
            )
            return
        selected[action_id] = {
            "episode_id": EPISODE_ID,
            "action_id": action_id,
            "document_id": _text(item.get("document_id")),
            "record_id": _text(item.get("record_id")),
            "city": _text(item.get("city")),
            "province": _text(item.get("province")),
            "official_url": _text(item.get("official_url")),
            "content_hash": _text(item.get("content_hash")),
            "triage_state": _text(item.get("triage_state")),
            "evidence_triage_class": _text(item.get("evidence_triage_class")),
            "priority": priority,
            "priority_reason": reason,
            "reference_event_ids": ";".join(sorted(events)),
        }

    for item in triage.to_dict("records"):
        action_id = _text(item.get("action_id"))
        city = _text(item.get("city"))
        state = _text(item.get("triage_state"))
        if action_id in matched_by_action:
            add(item, 0, "REFERENCE_MATCHED_ACTION", matched_by_action[action_id])
        elif city in unresolved_cities and state in {
            "KEEP_FOR_EP930_REVIEW",
            "NEEDS_EVIDENCE",
            "MANUAL_REVIEW_REQUIRED",
        }:
            add(item, 1, "UNRESOLVED_REFERENCE_CITY_CANDIDATE", set())
        elif state == "KEEP_FOR_EP930_REVIEW":
            add(item, 2, "KEEP_FOR_EP930_REVIEW", set())

    if not selected:
        return pd.DataFrame(columns=columns)
    return pd.DataFrame(list(selected.values()), columns=columns).sort_values(
        ["priority", "city", "document_id", "action_id"],
        na_position="last",
    ).reset_index(drop=True)


def build_scope_reconciliation(
    episode_root: Path,
    discovery: pd.DataFrame,
    documents: pd.DataFrame,
    actions: pd.DataFrame,
) -> pd.DataFrame:
    scope = _safe_read_json(episode_root / "930_ANALYSIS_READY_SCOPE.json")
    current_seed = {_text(value) for value in scope.get("cities", []) if _text(value)}
    current_reference = {_text(value) for value in discovery.get("city", pd.Series(dtype=str)).tolist() if _text(value)}
    evidence_cities = set()
    for frame in (documents, actions):
        if "city" in frame.columns:
            evidence_cities.update(_text(value) for value in frame["city"].dropna().tolist() if _text(value))
    event_counts = discovery.get("city", pd.Series(dtype=str)).value_counts().to_dict()
    rows: list[dict[str, Any]] = []
    for city in sorted(current_seed | current_reference | evidence_cities):
        scopes = []
        if city in current_seed:
            scopes.append("CURRENT_FROZEN_SCOPE")
        if city in current_reference:
            scopes.append("CURRENT_REFERENCE_EVENT")
        if city in evidence_cities:
            scopes.append("EVIDENCE_IDENTIFIED_CANDIDATE_CITY")
        rows.append(
            {
                "scope_name": "CITY_LEVEL_RECONCILIATION",
                "city": city,
                "scope_memberships": ";".join(scopes),
                "current_reference_event_count": int(event_counts.get(city, 0)),
                "evidence_identified": city in evidence_cities,
                "current_frozen_scope_member": city in current_seed,
                "discrepancy": ";".join(
                    [
                        "REFERENCE_WITHOUT_FROZEN_SCOPE" if city in current_reference and city not in current_seed else "",
                        "FROZEN_SCOPE_WITHOUT_REFERENCE_EVENT" if city in current_seed and city not in current_reference else "",
                        "EVIDENCE_CITY_NOT_REFERENCE_EVENT" if city in evidence_cities and city not in current_reference else "",
                    ]
                ).strip(";"),
                "resolution": "REQUIRES_REFERENCE_RECONCILIATION" if city not in current_reference else "OPEN_RECONCILIATION",
                "source": "930_ANALYSIS_READY_SCOPE.json;01_DISCOVERY/2016_930_CITY_DISCOVERY.parquet;Curated episode snapshots",
            }
        )
    rows.extend(
        [
            {
                "scope_name": "PROMPT_BASELINE_CORE_930",
                "city": "",
                "scope_memberships": "",
                "current_reference_event_count": len(current_reference),
                "evidence_identified": False,
                "current_frozen_scope_member": False,
                "discrepancy": "PROMPT_BASELINE_21_NOT_PRESENT_AS_SEPARATE_ARTIFACT",
                "resolution": "NOT_VERIFIED_FROM_CURRENT_ARTIFACTS",
                "source": "user-provided baseline only; no separate 21-city file found",
            },
            {
                "scope_name": "PROMPT_BASELINE_EXPANSION_930",
                "city": "",
                "scope_memberships": "",
                "current_reference_event_count": len(current_reference),
                "evidence_identified": False,
                "current_frozen_scope_member": False,
                "discrepancy": "PROMPT_BASELINE_22_NOT_PRESENT_AS_SEPARATE_ARTIFACT",
                "resolution": "NOT_VERIFIED_FROM_CURRENT_ARTIFACTS",
                "source": "user-provided baseline only; no separate 22-city file found",
            },
        ]
    )
    return pd.DataFrame(rows)


def _date_state_for_relevant(row: Mapping[str, Any]) -> str:
    state = _text(row.get("date_state"))
    return state if state in DATE_STATES else "MANUAL_REVIEW_REQUIRED"


def build_date_audit(triage: pd.DataFrame, reference: pd.DataFrame) -> pd.DataFrame:
    if triage.empty:
        return pd.DataFrame()
    referenced = set(reference.get("matched_action_ids", pd.Series(dtype=str)).astype(str).str.split(";").explode())
    relevant = triage[
        triage["triage_state"].isin({"KEEP_FOR_EP930_REVIEW", "MANUAL_REVIEW_REQUIRED"})
        | triage["action_id"].isin(referenced)
    ].copy()
    return relevant.assign(
        date_state=relevant.apply(_date_state_for_relevant, axis=1),
        manual_verified=False,
        conflict_flag=False,
        date_reconciliation=relevant["date_state"].map(
            lambda value: "NO_EXPLICIT_EFFECTIVE_DATE" if value == "NO_EXPLICIT_EFFECTIVE_DATE" else value
        ),
    )[
        [
            "episode_id",
            "city",
            "document_id",
            "action_id",
            "announcement_date",
            "publication_date",
            "effective_date",
            "implementation_date",
            "webpage_publish_date",
            "underlying_policy_date",
            "date_state",
            "date_evidence",
            "date_confidence",
            "date_basis",
            "manual_verified",
            "conflict_flag",
            "date_reconciliation",
            "triage_state",
        ]
    ].drop_duplicates("action_id")


def build_promotion_blockers(triage: pd.DataFrame, api: pd.DataFrame) -> pd.DataFrame:
    pass1, pass2 = _api_status_maps(api)
    rows: list[dict[str, Any]] = []
    for row in triage.to_dict("records"):
        state = _text(row.get("triage_state"))
        if state != "KEEP_FOR_EP930_REVIEW":
            root = "episode_membership"
        elif not bool(row.get("official_evidence")):
            root = "official_evidence"
        elif not _text(row.get("action_text")):
            root = "action_extraction"
        elif _text(row.get("date_state")) == "MANUAL_REVIEW_REQUIRED":
            root = "date_state"
        elif pass1.get(_text(row.get("action_id")), "NOT_COMPLETED") not in FORMAL_API_STATUSES:
            root = "api_pass1"
        elif pass2.get(_text(row.get("action_id")), "NOT_COMPLETED") not in FORMAL_API_STATUSES:
            root = "api_pass2"
        elif _text(row.get("dedup_status")).lower() in {"duplicate_action", "duplicate", "noncanonical"}:
            root = "dedup"
        elif bool(row.get("gap_linked")):
            root = "critical_gap"
        elif bool(row.get("manual_review_required")):
            root = "manual_review"
        else:
            root = "none"
        rows.append({"action_id": _text(row.get("action_id")), "root_blocker": root})
    details = pd.DataFrame(rows)
    counts = details["root_blocker"].value_counts().to_dict() if not details.empty else {}
    raw_counts = Counter()
    for row in triage.to_dict("records"):
        if _text(row.get("triage_state")) != "KEEP_FOR_EP930_REVIEW":
            raw_counts["episode_membership"] += 1
        if not bool(row.get("official_evidence")):
            raw_counts["official_evidence"] += 1
        if not _text(row.get("action_text")):
            raw_counts["action_extraction"] += 1
        if _text(row.get("date_state")) == "MANUAL_REVIEW_REQUIRED":
            raw_counts["date_state"] += 1
        if pass1.get(_text(row.get("action_id")), "NOT_COMPLETED") not in FORMAL_API_STATUSES:
            raw_counts["api_pass1"] += 1
        if pass2.get(_text(row.get("action_id")), "NOT_COMPLETED") not in FORMAL_API_STATUSES:
            raw_counts["api_pass2"] += 1
        if bool(row.get("gap_linked")):
            raw_counts["critical_gap"] += 1
        if bool(row.get("manual_review_required")):
            raw_counts["manual_review"] += 1
    out = []
    for blocker in sorted(set(raw_counts) | set(counts)):
        raw = int(raw_counts.get(blocker, 0))
        root = int(counts.get(blocker, 0))
        out.append(
            {
                "blocker": blocker,
                "raw_gap_count": raw,
                "root_blocker_count": root,
                "derived_gap_count": max(raw - root, 0),
                "definition": "first unmet gate in deterministic promotion order",
            }
        )
    return pd.DataFrame(out)


def _gap_category(row: Mapping[str, Any]) -> str:
    text = " ".join(_text(row.get(key)).lower() for key in ("gap_type", "reason", "evidence", "policy_tool"))
    if any(token in text for token in ("api", "classif", "model")):
        return "API_DEPENDENT"
    if any(token in text for token in ("membership", "episode", "city", "policy_type")):
        return "MEMBERSHIP_DEPENDENT"
    if any(token in text for token in ("reference", "discovery", "source_gap")):
        return "REFERENCE_DEPENDENT"
    if any(token in text for token in ("date", "effective", "announcement")):
        return "DATE_DEPENDENT"
    if any(token in text for token in ("official", "evidence", "url")):
        return "OFFICIAL_EVIDENCE_DEPENDENT"
    if any(token in text for token in ("action", "extract", "clause")):
        return "ACTION_DEPENDENT"
    if any(token in text for token in ("dedup", "duplicate")):
        return "DEDUP_DEPENDENT"
    if any(token in text for token in ("manual", "ambiguous", "review")):
        return "MANUAL_REVIEW"
    return "TRUE_INDEPENDENT_GAP"


def build_gap_root_causes(gaps: pd.DataFrame, scope_cities: set[str]) -> pd.DataFrame:
    if gaps.empty:
        return pd.DataFrame(columns=["scope", "root_cause", "raw_gap_count", "root_blocker_count", "derived_gap_count"])
    frames = [("GLOBAL", gaps), ("CORE_FROZEN_CITIES", gaps[gaps.get("city", pd.Series(dtype=str)).astype(str).isin(scope_cities)])]
    rows: list[dict[str, Any]] = []
    for scope, frame in frames:
        if frame.empty:
            continue
        work = frame.copy()
        work["root_cause"] = work.apply(_gap_category, axis=1)
        work["root_key"] = work.apply(
            lambda row: "|".join(
                [
                    _text(row.get("root_cause")),
                    _text(row.get("city")),
                    _text(row.get("policy_tool")),
                    _text(row.get("action_id")) or _text(row.get("document_id")) or _text(row.get("gap_key")),
                ]
            ),
            axis=1,
        )
        for cause, group in work.groupby("root_cause", dropna=False):
            raw = len(group)
            root = int(group["root_key"].nunique())
            rows.append(
                {
                    "scope": scope,
                    "root_cause": str(cause),
                    "raw_gap_count": raw,
                    "root_blocker_count": root,
                    "derived_gap_count": max(raw - root, 0),
                    "example_reason": _text(group.iloc[0].get("reason")),
                }
            )
    return pd.DataFrame(rows).sort_values(["scope", "raw_gap_count"], ascending=[True, False])


def _manual_queue(triage: pd.DataFrame, reference: pd.DataFrame) -> pd.DataFrame:
    candidate = triage[triage["triage_state"].eq("MANUAL_REVIEW_REQUIRED")].copy()
    if not candidate.empty:
        candidate = candidate.assign(
            review_type="CANDIDATE_MEMBERSHIP",
            ambiguity=candidate["triage_reason"],
            suggested_resolution="Confirm underlying 2016 policy date and episode membership from official evidence.",
            impact_if_included="May enter the formal 930 candidate set after all downstream gates.",
            impact_if_excluded="Remains outside the formal 930 treatment universe with provenance retained.",
        )
    ref = reference[reference["manual_review_required"].astype(bool)].copy() if not reference.empty else pd.DataFrame()
    if not ref.empty:
        ref = ref.assign(
            review_type="REFERENCE_EVENT",
            action_id=ref["matched_action_ids"],
            document_id=ref["matched_document_id"],
            ambiguity=ref["resolution_reason"],
            suggested_resolution="Resolve reference event against official document and date evidence.",
            impact_if_included="Closes a reference coverage discrepancy.",
            impact_if_excluded="Reference event remains explicitly excluded or insufficiently evidenced.",
        )
    frames = [frame for frame in (candidate, ref) if not frame.empty]
    if not frames:
        return pd.DataFrame(columns=["review_type", "city", "document_id", "action_id", "ambiguity", "suggested_resolution", "impact_if_included", "impact_if_excluded"])
    columns = ["review_type", "city", "province", "document_id", "action_id", "official_url", "ambiguity", "suggested_resolution", "impact_if_included", "impact_if_excluded"]
    result = pd.concat(frames, ignore_index=True, sort=False)
    for column in columns:
        if column not in result.columns:
            result[column] = ""
    return result[columns].drop_duplicates()


def _report(manifest: Mapping[str, Any]) -> str:
    counts = manifest["counts"]
    return f"""# EP930 Treatment Universe Closure

## Status

`{manifest['econometric_grade_status']}` — this is a deterministic triage and
reconciliation snapshot, not a promotion operation.

## Treatment-universe counts

- Original Curated candidates: **{counts['original_candidates']}**
- Deterministically excluded: **{counts['deterministically_excluded']}**
- Remaining membership candidates: **{counts['membership_candidates_remaining']}**
- Keep-for-review candidates: **{counts['keep_for_review']}**
- Evidence recovery candidates: **{counts['needs_evidence']}**
- Manual adjudication candidates: **{counts['manual_review_required']}**
- Formal actions currently promoted: **{counts['formal_actions']}**
- Unknown triage states: **{counts['unknown_triage_states']}**

The deterministic exclusion set is not sent to API processing.  Missing date,
direction, geography or official evidence remains explicitly unresolved and is
not coded as untreated or as a formal exclusion.

## Reference closure

- Current reference events: **{counts['reference_events']}**
- Reference rows closed without UNKNOWN: **{counts['reference_rows_without_unknown']}**
- Reference rows still requiring review: **{counts['reference_manual_review']}**
- Reference recovery queue rows: **{counts['reference_recovery_required']}**
- Current frozen scope cities in the artifact: **{counts['frozen_scope_cities']}**
- Evidence-identified candidate cities: **{counts['evidence_cities']}**

The requested 21-city core and 22-city expansion baselines were not present as
separate artifacts; the scope reconciliation records this discrepancy rather
than manufacturing missing cities.

## Formal blockers

The first unmet promotion gate is recorded in
`EP930_PROMOTION_BLOCKER_DECOMPOSITION.csv`.  API Pass1/Pass2 are not bypassed,
and the current formal master remains unchanged.

The bounded API fast-lane overlay is written to
`EP930_API_FAST_LANE_PLAN.csv`.  It ranks reference-matched actions first,
then candidates in unresolved reference cities, then the deterministic KEEP
set.  It is a scheduling aid only; it cannot verify, promote, or enable an
action.

## Safety

- No network or AI call was made.
- No queue, database, Curated Parquet, or production state was written.
- No outcome data was read.
- No commit or push was performed.
"""


def build(data_root: Path, output_root: Path, timestamp: str | None = None) -> Path:
    episode_root = data_root / "outputs" / "special_projects" / "2016_930"
    stamp = timestamp or datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    target = output_root / stamp
    target.mkdir(parents=True, exist_ok=False)
    actions, documents, gaps, api = _load_joined(data_root, episode_root)
    discovery = _safe_read_parquet(episode_root / "01_DISCOVERY" / "2016_930_CITY_DISCOVERY.parquet")
    triage = build_triage(actions, gaps)
    reference = build_reference_reconciliation(discovery, triage)
    reference_recovery = build_reference_recovery_queue(reference, triage)
    api_fast_lane = build_api_fast_lane_plan(triage, reference)
    scope = build_scope_reconciliation(episode_root, discovery, documents, actions)
    dates = build_date_audit(triage, reference)
    blockers = build_promotion_blockers(triage, api)
    scope_json = _safe_read_json(episode_root / "930_ANALYSIS_READY_SCOPE.json")
    scope_cities = {_text(value) for value in scope_json.get("cities", []) if _text(value)}
    gap_roots = build_gap_root_causes(gaps, scope_cities)
    manual = _manual_queue(triage, reference)
    evidence_queue = triage[triage["triage_state"].eq("NEEDS_EVIDENCE")].copy()

    _write_csv(target / "EP930_MEMBERSHIP_CANDIDATE_ACTIONS.csv", triage)
    _write_csv(target / "EP930_REFERENCE_EVENT_RECONCILIATION.csv", reference)
    _write_csv(target / "EP930_REFERENCE_SCOPE_RECONCILIATION.csv", scope)
    _write_csv(target / "EP930_DATE_AUDIT_RELEVANT.csv", dates)
    _write_csv(target / "EP930_PROMOTION_BLOCKER_DECOMPOSITION.csv", blockers)
    _write_csv(target / "EP930_TREATMENT_GAP_ROOT_CAUSE.csv", gap_roots)
    _write_csv(target / "EP930_EVIDENCE_RECOVERY_QUEUE.csv", evidence_queue)
    _write_csv(target / "EP930_REFERENCE_RECOVERY_QUEUE.csv", reference_recovery)
    _write_csv(target / "EP930_API_FAST_LANE_PLAN.csv", api_fast_lane)
    _write_xlsx(
        target / "EP930_MANUAL_ADJUDICATION_QUEUE.xlsx",
        {"Candidate review": manual, "Reference review": reference[reference["manual_review_required"].astype(bool)] if not reference.empty else pd.DataFrame()},
    )

    state_counts = triage["triage_state"].value_counts().to_dict() if not triage.empty else {}
    excluded = sum(int(state_counts.get(state, 0)) for state in TRIAGE_STATES if state.startswith("EXCLUDE_"))
    manual_count = int(state_counts.get("MANUAL_REVIEW_REQUIRED", 0))
    reference_manual = int(reference["manual_review_required"].astype(bool).sum()) if not reference.empty else 0
    formal_actions = 0
    counts = {
        "original_candidates": len(actions),
        "deterministically_excluded": excluded,
        "membership_candidates_remaining": len(triage) - excluded,
        "keep_for_review": int(state_counts.get("KEEP_FOR_EP930_REVIEW", 0)),
        "needs_evidence": int(state_counts.get("NEEDS_EVIDENCE", 0)),
        "manual_review_required": manual_count,
        "formal_actions": formal_actions,
        "unknown_triage_states": int((~triage["triage_state"].isin(TRIAGE_STATES)).sum()) if not triage.empty else 0,
        "reference_events": len(reference),
        "reference_rows_without_unknown": int((~reference["closure_status"].eq("UNKNOWN")).sum()) if not reference.empty else 0,
        "reference_closure_status_counts": {
            str(key): int(value)
            for key, value in reference["closure_status"].value_counts().to_dict().items()
        } if not reference.empty else {},
        "reference_manual_review": reference_manual,
        "reference_recovery_required": len(reference_recovery),
        "api_fast_lane_actions": len(api_fast_lane),
        "api_fast_lane_documents": int(api_fast_lane["document_id"].nunique()) if not api_fast_lane.empty else 0,
        "frozen_scope_cities": len(scope_cities),
        "evidence_cities": len({value for value in documents.get("city", pd.Series(dtype=str)).dropna().astype(str) if value}),
    }
    manifest = {
        "episode_id": EPISODE_ID,
        "stage": "EP930_TREATMENT_UNIVERSE_CLOSURE",
        "econometric_grade_status": "BLOCKED" if formal_actions == 0 or counts["unknown_triage_states"] else "BLOCKED",
        "generated_at": datetime.now(UTC).isoformat(),
        "read_only": True,
        "network_calls": 0,
        "api_calls": 0,
        "counts": counts,
        "triage_state_counts": {str(key): int(value) for key, value in state_counts.items()},
        "evidence_triage_class_counts": {
            str(key): int(value)
            for key, value in triage["evidence_triage_class"].value_counts().to_dict().items()
        } if not triage.empty else {},
        "api_fast_lane": {
            "actions": len(api_fast_lane),
            "documents": int(api_fast_lane["document_id"].nunique()) if not api_fast_lane.empty else 0,
            "priority_counts": {
                str(key): int(value)
                for key, value in api_fast_lane["priority"].value_counts().sort_index().to_dict().items()
            } if not api_fast_lane.empty else {},
            "source": "EP930_API_FAST_LANE_PLAN.csv",
        },
        "api_status": {
            "provider": _text(api.iloc[0].get("provider")) if not api.empty and "provider" in api.columns else "",
            "model": _text(api.iloc[0].get("model")) if not api.empty and "model" in api.columns else "",
            "pass1_rows": int((api.get("pass_name", pd.Series(dtype=str)).astype(str).str.lower().isin({"first_pass", "pass1"})).sum()) if not api.empty else 0,
            "pass2_rows": int((api.get("pass_name", pd.Series(dtype=str)).astype(str).str.lower().isin({"second_pass", "pass2"})).sum()) if not api.empty else 0,
        },
        "scope": {
            "scope_version": scope_json.get("scope_version"),
            "scope_hash": scope_json.get("scope_hash"),
            "frozen": scope_json.get("frozen"),
            "scope_file_sha256": _sha256(episode_root / "930_ANALYSIS_READY_SCOPE.json") if (episode_root / "930_ANALYSIS_READY_SCOPE.json").exists() else None,
        },
        "source_hashes": {
            "actions": _sha256(data_root / "curated" / "policy_episode_actions.parquet"),
            "documents": _sha256(data_root / "curated" / "policy_episode_documents.parquet"),
            "gaps": _sha256(data_root / "curated" / "policy_episode_gaps.parquet"),
            "discovery": _sha256(episode_root / "01_DISCOVERY" / "2016_930_CITY_DISCOVERY.parquet"),
        },
        "candidate_master_is_not_treatment": True,
        "outcome_data_read": False,
        "outcome_driven_selection": False,
    }
    _write_json(target / "EP930_TREATMENT_UNIVERSE_CLOSURE_MANIFEST.json", manifest)
    _write_json(target / "EP930_TREATMENT_UNIVERSE_CLOSURE_SUMMARY.json", {"counts": counts, "triage_state_counts": manifest["triage_state_counts"]})
    (target / "EP930_TREATMENT_UNIVERSE_CLOSURE_REPORT.md").write_text(_report(manifest), encoding="utf-8")

    hashes = {}
    for path in sorted(target.iterdir()):
        if path.name == "EP930_SHA256_MANIFEST.json" or not path.is_file():
            continue
        hashes[path.name] = _sha256(path)
    _write_json(target / "EP930_SHA256_MANIFEST.json", {"generated_at": datetime.now(UTC).isoformat(), "files": hashes})
    return target


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-root", type=Path, default=Path(r"E:\Data Set\CRPD"))
    parser.add_argument(
        "--output-root",
        type=Path,
        default=Path(r"E:\Data Set\CRPD\outputs\special_projects\2016_930\treatment_universe_closure"),
    )
    parser.add_argument("--timestamp", default=None)
    args = parser.parse_args()
    target = build(args.data_root, args.output_root, args.timestamp)
    print(json.dumps({"output": str(target), "status": "COMPLETED_READ_ONLY"}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
