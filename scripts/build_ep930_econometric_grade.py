"""Build an auditable, read-only EP930 econometric treatment freeze.

The builder consumes immutable Curated/episode artifacts and never calls the
network, the AI provider, or the production writer.  Candidate actions are
kept separate from the formal econometric treatment master.  A non-empty
formal master is emitted only when every action-level gate is satisfied.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from collections.abc import Iterable, Mapping, Sequence
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pandas as pd

EPISODE_ID = "EP_2016_930_TIGHTENING"
EPISODE_NAME = "2016年930楼市调控潮"
EPISODE_DIRECTION = "TIGHTENING"
ALLOWED_POLICY_TYPES = (
    "LIMIT_PURCHASE",
    "LIMIT_RESALE",
    "COMMERCIAL_DOWNPAYMENT",
    "PF_DOWNPAYMENT",
    "PF_LOAN_CEILING",
    "PF_OTHER_CONDITIONS",
    "LAND_SUPPLY",
    "PRICE_REGULATION",
    "MARKET_SUPERVISION",
    "OTHER_HOUSING_POLICY",
)
FORMAL_API_STATUSES = {"SUCCESS", "COMPLETED", "VALID", "APPROVED"}
OFFICIAL_EVIDENCE_STATUSES = {
    "CURATED_OFFICIAL",
    "LIVE_HTTP_200",
    "OFFICIAL_POLICY",
    "OFFICIAL_REPRINT",
}

ACTION_MASTER_COLUMNS = [
    "episode_id",
    "episode_name",
    "city",
    "province",
    "document_id",
    "action_id",
    "title",
    "document_number",
    "issuer",
    "policy_type",
    "policy_subtype",
    "mechanism_labels",
    "direction",
    "announcement_date",
    "publication_date",
    "effective_date",
    "implementation_date",
    "recommended_treatment_date",
    "date_type",
    "date_evidence",
    "date_confidence",
    "parameter_name",
    "old_value",
    "new_value",
    "unit",
    "target_population",
    "geographic_scope",
    "bundle_id",
    "bundle_size",
    "co_treatment_types",
    "official_url",
    "canonical_url",
    "source_confidence",
    "classification_confidence",
    "episode_confidence",
    "api_pass1_status",
    "api_pass2_status",
    "manual_review_required",
    "treatment_status",
]


def _json_default(value: Any) -> Any:
    if isinstance(value, (datetime, date, pd.Timestamp)):
        return value.isoformat()
    if isinstance(value, (pd.Int64Dtype, pd.Float64Dtype)):
        return str(value)
    if hasattr(value, "item"):
        return value.item()
    return str(value)


def _is_missing(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return not value.strip() or value.strip().lower() in {"nan", "none", "null"}
    try:
        result = pd.isna(value)
        return bool(result) if not hasattr(result, "__len__") else False
    except (TypeError, ValueError):
        return False


def _text(value: Any) -> str:
    return "" if _is_missing(value) else str(value).strip()


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return _text(value).lower() in {"1", "true", "yes", "y", "pass", "valid"}


def _json_text(value: Any) -> str:
    if _is_missing(value):
        return ""
    if isinstance(value, str):
        stripped = value.strip()
        if stripped.startswith(("[", "{")):
            try:
                return json.dumps(json.loads(stripped), ensure_ascii=False, sort_keys=True)
            except json.JSONDecodeError:
                return stripped
        return stripped
    if isinstance(value, (list, tuple, set, dict)):
        return json.dumps(value, ensure_ascii=False, sort_keys=True, default=_json_default)
    return _text(value)


def _date_value(value: Any) -> str | None:
    if _is_missing(value):
        return None
    try:
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            unit = "ms" if abs(float(value)) > 10_000_000_000 else "s"
            parsed = pd.to_datetime(value, unit=unit, errors="coerce", utc=True)
        else:
            parsed = pd.to_datetime(value, errors="coerce", utc=True)
        if pd.isna(parsed):
            return None
        return parsed.date().isoformat()
    except (TypeError, ValueError, OverflowError):
        return None


def _date_series(frame: pd.DataFrame, column: str) -> pd.Series:
    if column not in frame.columns:
        return pd.Series(pd.NaT, index=frame.index, dtype="datetime64[ns]")
    values = frame[column]
    if pd.api.types.is_numeric_dtype(values):
        return pd.to_datetime(values, unit="ms", errors="coerce", utc=True).dt.tz_localize(None)
    return pd.to_datetime(values, errors="coerce", utc=True).dt.tz_localize(None)


def _safe_read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
        return value if isinstance(value, dict) else {}
    except (OSError, json.JSONDecodeError):
        return {}


def _safe_read_parquet(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_parquet(path)
    except (OSError, ValueError, RuntimeError):
        return pd.DataFrame()


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _atomic_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(content, encoding="utf-8")
    temporary.replace(path)


def _write_json(path: Path, value: Mapping[str, Any]) -> None:
    _atomic_text(path, json.dumps(value, ensure_ascii=False, indent=2, default=_json_default) + "\n")


def _write_csv(path: Path, frame: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    frame.to_csv(temporary, index=False, encoding="utf-8-sig")
    temporary.replace(path)


def _write_xlsx(path: Path, sheets: Mapping[str, pd.DataFrame]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp.xlsx")
    with pd.ExcelWriter(temporary, engine="openpyxl") as writer:
        for name, frame in sheets.items():
            frame.to_excel(writer, sheet_name=name[:31], index=False)
    temporary.replace(path)


def _ensure_columns(frame: pd.DataFrame, columns: Sequence[str]) -> pd.DataFrame:
    result = frame.copy()
    for column in columns:
        if column not in result.columns:
            result[column] = pd.NA
    return result.loc[:, list(columns)]


def _scope_definition(output: Path) -> dict[str, Any]:
    scope = _safe_read_json(output / "00_SCOPE" / "2016_930_SCOPE.json")
    core = scope.get("core_window") or {"start": "2016-09-25", "end": "2016-10-10"}
    extended = scope.get("extended_window") or {"start": "2016-09-20", "end": "2016-10-15"}
    provenance = scope.get("provenance_window") or {"start": "2016-09-01", "end": "2016-10-31"}
    return {
        "episode_id": EPISODE_ID,
        "episode_name": EPISODE_NAME,
        "episode_direction": EPISODE_DIRECTION,
        "core_window": {"start": str(core.get("start")), "end": str(core.get("end"))},
        "extended_window": {"start": str(extended.get("start")), "end": str(extended.get("end"))},
        "provenance_window": {"start": str(provenance.get("start")), "end": str(provenance.get("end"))},
        "seed_cities": list(scope.get("seed_cities") or []),
        "policy_tools": list(scope.get("policy_tools") or ALLOWED_POLICY_TYPES),
    }


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
        previous = target.get(action_id, "NOT_COMPLETED")
        if previous not in FORMAL_API_STATUSES and status in FORMAL_API_STATUSES:
            target[action_id] = status
        else:
            target.setdefault(action_id, status)
    return pass1, pass2


def _parameter_map(parameters: pd.DataFrame) -> dict[str, list[dict[str, Any]]]:
    result: dict[str, list[dict[str, Any]]] = {}
    if parameters.empty or "action_id" not in parameters.columns:
        return result
    for row in parameters.to_dict("records"):
        action_id = _text(row.get("action_id"))
        if not action_id:
            continue
        result.setdefault(action_id, []).append(
            {
                "parameter_name": _text(row.get("parameter_name")),
                "old_value": _text(row.get("old_value")),
                "new_value": _text(row.get("new_value")),
                "unit": _text(row.get("unit")),
                "evidence_text": _text(row.get("evidence_text")),
                "confidence": row.get("parameter_confidence"),
            }
        )
    return result


def _date_map(date_audit: pd.DataFrame) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    if date_audit.empty:
        return result
    key = "action_id" if "action_id" in date_audit.columns else "document_id"
    for row in date_audit.to_dict("records"):
        value = _text(row.get(key))
        if value and value not in result:
            result[value] = row
    return result


def _official_doc_status(row: Mapping[str, Any]) -> tuple[bool, str]:
    status = _text(row.get("official_evidence_status")).upper()
    official_source = _truthy(row.get("official_source"))
    url = _text(row.get("official_url")) or _text(row.get("canonical_url"))
    if status in OFFICIAL_EVIDENCE_STATUSES or (official_source and url):
        return True, status or "OFFICIAL_SOURCE"
    if url:
        return False, status or "UNVERIFIED_URL"
    return False, "MISSING_OFFICIAL_EVIDENCE"


def _normalize_candidates(
    actions: pd.DataFrame,
    documents: pd.DataFrame,
    parameters: pd.DataFrame,
    date_audit: pd.DataFrame,
    api: pd.DataFrame,
    gaps: pd.DataFrame,
    scope: Mapping[str, Any],
) -> pd.DataFrame:
    actions = actions.copy()
    if actions.empty:
        return pd.DataFrame()
    if "episode_id" in actions.columns:
        actions = actions[actions["episode_id"].astype(str).eq(EPISODE_ID)].copy()
    actions = actions.drop_duplicates(subset=[c for c in ["action_id"] if c in actions.columns], keep="last")
    if documents.empty:
        documents = pd.DataFrame(columns=["document_id"])
    if "episode_id" in documents.columns:
        documents = documents[documents["episode_id"].astype(str).eq(EPISODE_ID)]
    if "document_id" in documents.columns:
        documents = documents.drop_duplicates("document_id", keep="last")
    doc_columns = [
        "document_id",
        "document_title",
        "document_number",
        "issuer",
        "official_url",
        "canonical_url",
        "official_source",
        "official_evidence_status",
        "content_hash",
        "is_formal_eligible",
        "city_id",
        "city",
        "province",
    ]
    available_doc_columns = [c for c in doc_columns if c in documents.columns]
    doc_frame = documents.loc[:, available_doc_columns].rename(
        columns={"official_url": "document_official_url"}
    )
    if "document_id" in actions.columns and "document_id" in doc_frame.columns:
        actions = actions.merge(doc_frame, on="document_id", how="left", suffixes=("", "_doc"))
    params = _parameter_map(parameters)
    dates = _date_map(date_audit)
    pass1, pass2 = _api_status_maps(api)
    gap_doc_ids = set(gaps.get("document_id", pd.Series(dtype=str)).dropna().astype(str)) if not gaps.empty else set()
    gap_action_ids = set(gaps.get("action_id", pd.Series(dtype=str)).dropna().astype(str)) if not gaps.empty else set()
    core_start = pd.Timestamp(scope["core_window"]["start"])
    core_end = pd.Timestamp(scope["core_window"]["end"])
    extended_start = pd.Timestamp(scope["extended_window"]["start"])
    extended_end = pd.Timestamp(scope["extended_window"]["end"])
    provenance_start = pd.Timestamp(scope["provenance_window"]["start"])
    provenance_end = pd.Timestamp(scope["provenance_window"]["end"])
    rows: list[dict[str, Any]] = []
    for row in actions.to_dict("records"):
        action_id = _text(row.get("action_id"))
        document_id = _text(row.get("document_id"))
        doc_official, evidence_status = _official_doc_status(row)
        official_url = _text(row.get("official_url")) or _text(row.get("document_official_url"))
        event_date = _date_value(row.get("announcement_date")) or _date_value(row.get("publication_date"))
        event_ts = pd.Timestamp(event_date) if event_date else pd.NaT
        effective_date = _date_value(row.get("effective_date"))
        implementation_date = _date_value(row.get("implementation_date"))
        date_row = dates.get(action_id) or dates.get(document_id) or {}
        date_evidence = _text(row.get("date_evidence_text")) or _text(date_row.get("notes"))
        date_type = "UNKNOWN_DATE_STATE"
        if effective_date:
            date_type = "EXPLICIT_EFFECTIVE_DATE"
        elif implementation_date:
            date_type = "ACTION_SPECIFIC_EFFECTIVE_DATE"
        elif event_date:
            date_type = "NO_EXPLICIT_EFFECTIVE_DATE"
        policy_type = _text(row.get("policy_type")).upper()
        if policy_type == "PF_OTHER":
            policy_type = "PF_OTHER_CONDITIONS"
        direction = _text(row.get("action_direction")) or _text(row.get("episode_direction"))
        geography = _text(row.get("geographic_scope"))
        reasons: list[str] = []
        if _text(row.get("episode_id")) != EPISODE_ID:
            reasons.append("WRONG_EPISODE")
        if not event_date:
            reasons.append("UNKNOWN_DATE_STATE")
        elif event_ts < core_start or event_ts > core_end:
            year_hint = event_ts.year
            reasons.append("2017_POLICY" if year_hint == 2017 else "LATER_POLICY" if year_hint > 2017 else "OUTSIDE_EPISODE_WINDOW")
        if not doc_official or not official_url:
            reasons.append("MISSING_OFFICIAL_EVIDENCE")
        if not _text(row.get("action_text")):
            reasons.append("MISSING_ACTION_EXTRACTION")
        if direction.upper() != "TIGHTENING":
            reasons.append("NOT_TIGHTENING")
        if policy_type not in ALLOWED_POLICY_TYPES:
            reasons.append("TYPE_UNRESOLVED")
        if not geography:
            reasons.append("GEOGRAPHY_UNRESOLVED")
        if pass1.get(action_id, "NOT_COMPLETED") not in FORMAL_API_STATUSES:
            reasons.append("API_PASS1_INCOMPLETE")
        if pass2.get(action_id, "NOT_COMPLETED") not in FORMAL_API_STATUSES:
            reasons.append("API_PASS2_INCOMPLETE")
        if action_id in gap_action_ids or document_id in gap_doc_ids:
            reasons.append("CRITICAL_TREATMENT_GAP")
        if _text(row.get("dedup_status")).lower() not in {"", "canonical", "unique"}:
            reasons.append("DUPLICATE")
        reasons = list(dict.fromkeys(reasons))
        params_for_action = params.get(action_id, [])
        first_param = params_for_action[0] if params_for_action else {}
        rows.append(
            {
                "episode_id": EPISODE_ID,
                "episode_name": EPISODE_NAME,
                "city_id": _text(row.get("city_id")) or _text(row.get("city_id_doc")),
                "city": _text(row.get("city")) or _text(row.get("city_doc")),
                "province": _text(row.get("province")) or _text(row.get("province_doc")),
                "document_id": document_id,
                "action_id": action_id,
                "title": _text(row.get("document_title")),
                "document_number": _text(row.get("document_number")),
                "issuer": _text(row.get("issuer")),
                "policy_type": policy_type,
                "policy_subtype": _text(row.get("policy_subtype")),
                "mechanism_labels": _json_text(row.get("mechanism_labels")),
                "direction": direction.upper(),
                "announcement_date": event_date,
                "publication_date": _date_value(row.get("publication_date")),
                "effective_date": effective_date,
                "implementation_date": implementation_date,
                "expiry_date": _date_value(row.get("expiry_date")),
                "date_type": date_type,
                "date_evidence": date_evidence,
                "date_confidence": _text(row.get("date_confidence")) or _text(date_row.get("date_confidence")),
                "parameter_name": _text(first_param.get("parameter_name")),
                "old_value": _text(first_param.get("old_value")) or _text(row.get("old_value")),
                "new_value": _text(first_param.get("new_value")) or _text(row.get("new_value")),
                "unit": _text(first_param.get("unit")) or _text(row.get("unit")),
                "target_population": _text(row.get("target_population")),
                "geographic_scope": geography,
                "official_url": official_url,
                "canonical_url": _text(row.get("canonical_url")) or official_url,
                "official_evidence_status": evidence_status,
                "official_evidence": doc_official,
                "source_confidence": row.get("source_confidence"),
                "classification_confidence": row.get("classification_confidence"),
                "episode_confidence": row.get("episode_confidence"),
                "api_pass1_status": pass1.get(action_id, "NOT_COMPLETED"),
                "api_pass2_status": pass2.get(action_id, "NOT_COMPLETED"),
                "api_provider": _text(api.iloc[0].get("provider")) if not api.empty and "provider" in api.columns else "",
                "api_model": _text(api.iloc[0].get("model")) if not api.empty and "model" in api.columns else "",
                "date_state": date_type,
                "in_core_window": bool(event_date and core_start <= event_ts <= core_end),
                "in_extended_window": bool(event_date and extended_start <= event_ts <= extended_end),
                "in_provenance_window": bool(event_date and provenance_start <= event_ts <= provenance_end),
                "gap_linked": bool(action_id in gap_action_ids or document_id in gap_doc_ids),
                "exclusion_reason": ";".join(reasons),
                "treatment_status": "CONFIRMED_INCLUDED" if not reasons else "MANUAL_REVIEW_REQUIRED",
                "candidate_source": "CURATED_POLICY_EPISODE_ACTIONS",
            }
        )
    return pd.DataFrame(rows)


def _bundle_type(policy_types: Iterable[str]) -> str:
    types = {str(value) for value in policy_types if value}
    admin = bool(types & {"LIMIT_PURCHASE", "LIMIT_RESALE", "PRICE_REGULATION", "MARKET_SUPERVISION"})
    commercial = "COMMERCIAL_DOWNPAYMENT" in types
    pf = bool(types & {"PF_DOWNPAYMENT", "PF_LOAN_CEILING", "PF_OTHER_CONDITIONS"})
    if admin and commercial and pf:
        return "MULTI_FAMILY_COMPREHENSIVE"
    if admin and commercial:
        return "ADMIN_CREDIT_BUNDLE"
    if admin and pf:
        return "ADMIN_PF_BUNDLE"
    if commercial and pf:
        return "CREDIT_PF_BUNDLE"
    if commercial:
        return "COMMERCIAL_CREDIT_ONLY"
    if pf:
        return "PF_ONLY"
    if admin:
        return "ADMIN_ONLY"
    return "OTHER"


def _build_reference_tables(discovery: pd.DataFrame, candidates: pd.DataFrame, documents: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    if discovery.empty:
        discovery = pd.DataFrame(columns=["city_id", "city", "province", "reference_count", "expected_policy_types"])
    reference_rows: list[dict[str, Any]] = []
    for row in discovery.to_dict("records"):
        city = _text(row.get("city"))
        if not city:
            continue
        reference_rows.append(
            {
                "reference_event_id": "REF930_" + hashlib.sha256(city.encode("utf-8")).hexdigest()[:16].upper(),
                "episode_id": EPISODE_ID,
                "city_id": _text(row.get("city_id")),
                "city": city,
                "province": _text(row.get("province")),
                "reference_source": "01_DISCOVERY/2016_930_CITY_DISCOVERY.parquet",
                "reference_count": row.get("reference_count"),
                "expected_policy_types": _json_text(row.get("expected_policy_types")),
                "reference_status": "MANUAL_REVIEW_REQUIRED",
                "status_reason": "INTERNAL_DISCOVERY_SEED_IS_NOT_AN_EXTERNAL_REFERENCE_MASTER",
            }
        )
    reference = pd.DataFrame(reference_rows)
    evidence_docs = documents.copy()
    if not evidence_docs.empty and "episode_id" in evidence_docs:
        evidence_docs = evidence_docs[evidence_docs["episode_id"].astype(str).eq(EPISODE_ID)]
    evidence_city = {}
    if not evidence_docs.empty:
        for row in evidence_docs.to_dict("records"):
            city = _text(row.get("city"))
            if city:
                evidence_city.setdefault(city, {"documents": 0, "official_documents": 0})
                evidence_city[city]["documents"] += 1
                if _truthy(row.get("official_source")) or _text(row.get("official_evidence_status")).upper() in OFFICIAL_EVIDENCE_STATUSES:
                    evidence_city[city]["official_documents"] += 1
    candidate_city = candidates.groupby("city").size().to_dict() if not candidates.empty and "city" in candidates else {}
    cities = set(reference.get("city", pd.Series(dtype=str))) | set(evidence_city) | set(candidate_city)
    reconciliation_rows = []
    for city in sorted(value for value in cities if value):
        ref = reference[reference["city"].eq(city)] if not reference.empty else pd.DataFrame()
        info = evidence_city.get(city, {"documents": 0, "official_documents": 0})
        candidate_count = int(candidate_city.get(city, 0))
        in_reference = not ref.empty
        has_evidence = info["official_documents"] > 0
        discrepancy = "ALIGNED" if in_reference and has_evidence else "REFERENCE_ONLY" if in_reference else "EVIDENCE_ONLY"
        reconciliation_rows.append(
            {
                "episode_id": EPISODE_ID,
                "city": city,
                "province": _text(ref.iloc[0].get("province")) if not ref.empty else "",
                "reference_identified": in_reference,
                "evidence_identified": has_evidence,
                "official_document_count": info["official_documents"],
                "candidate_action_count": candidate_count,
                "inclusion_status": "MANUAL_REVIEW_REQUIRED",
                "discrepancy": discrepancy,
                "resolution": "EXTERNAL_REFERENCE_AND_ACTION_LEVEL_MEMBERSHIP_REVIEW_REQUIRED",
            }
        )
    return reference, pd.DataFrame(reconciliation_rows)


def _build_city_policy_matrix(reference: pd.DataFrame, candidates: pd.DataFrame, documents: pd.DataFrame) -> pd.DataFrame:
    cities = reference[["city_id", "city", "province"]].drop_duplicates() if not reference.empty else pd.DataFrame(columns=["city_id", "city", "province"])
    candidate_counts = (
        candidates.groupby(["city", "policy_type"], dropna=False).size().to_dict()
        if not candidates.empty
        else {}
    )
    official_counts = {}
    if not documents.empty and "city" in documents.columns:
        docs = documents.copy()
        for row in docs.to_dict("records"):
            city = _text(row.get("city"))
            if city:
                official_counts[city] = official_counts.get(city, 0) + int(_truthy(row.get("official_source")))
    rows = []
    for city_row in cities.to_dict("records"):
        for policy_type in ALLOWED_POLICY_TYPES:
            candidate_count = int(candidate_counts.get((_text(city_row.get("city")), policy_type), 0))
            official_count = int(official_counts.get(_text(city_row.get("city")), 0))
            if candidate_count:
                state = "MANUAL_REVIEW_REQUIRED"
            elif official_count:
                state = "MISSING_ACTION_EXTRACTION"
            else:
                state = "MISSING_OFFICIAL_DOCUMENT"
            rows.append(
                {
                    "episode_id": EPISODE_ID,
                    "city_id": _text(city_row.get("city_id")),
                    "city": _text(city_row.get("city")),
                    "province": _text(city_row.get("province")),
                    "policy_type": policy_type,
                    "state": state,
                    "candidate_action_count": candidate_count,
                    "official_document_count": official_count,
                    "evidence": "",
                    "resolution": "NO_SILENT_ZERO;REQUIRES_ACTION_AND_EPISODE_MEMBERSHIP_REVIEW",
                }
            )
    return pd.DataFrame(rows)


def _build_date_audit(candidates: pd.DataFrame) -> pd.DataFrame:
    if candidates.empty:
        return pd.DataFrame()
    rows = []
    for row in candidates.to_dict("records"):
        announcement = _text(row.get("announcement_date"))
        publication = _text(row.get("publication_date"))
        effective = _text(row.get("effective_date"))
        implementation = _text(row.get("implementation_date"))
        if effective:
            date_type = "EXPLICIT_EFFECTIVE_DATE"
        elif implementation:
            date_type = "ACTION_SPECIFIC_EFFECTIVE_DATE"
        elif announcement or publication:
            date_type = "NO_EXPLICIT_EFFECTIVE_DATE"
        else:
            date_type = "UNKNOWN_DATE_STATE"
        conflict = bool(announcement and effective and announcement != effective)
        rows.append(
            {
                "episode_id": EPISODE_ID,
                "city": _text(row.get("city")),
                "document_id": _text(row.get("document_id")),
                "action_id": _text(row.get("action_id")),
                "announcement_date": announcement,
                "publication_date": publication,
                "effective_date": effective,
                "implementation_date": implementation,
                "expiry_date": _text(row.get("expiry_date")),
                "date_type": date_type,
                "date_source_text": _text(row.get("date_evidence")),
                "date_confidence": _text(row.get("date_confidence")),
                "manual_verified": False,
                "conflict_flag": conflict,
                "date_reconciliation": "ANNOUNCEMENT_EFFECTIVE_CLOCK_DIFFERENCE" if conflict else "NO_EXPLICIT_EFFECTIVE_DATE" if date_type == "NO_EXPLICIT_EFFECTIVE_DATE" else date_type,
                "status": "CLOSED" if date_type != "UNKNOWN_DATE_STATE" and not conflict else "MANUAL_REVIEW_REQUIRED",
            }
        )
    return pd.DataFrame(rows)


def _build_bundles(candidates: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "episode_id",
        "bundle_id",
        "city",
        "province",
        "document_id",
        "bundle_size",
        "policy_types",
        "bundle_type",
        "bundle_status",
        "treatment_status",
    ]
    if candidates.empty:
        return pd.DataFrame(columns=columns)
    rows = []
    for (city, document_id), group in candidates.groupby(["city", "document_id"], dropna=False):
        city_text = _text(city)
        doc_text = _text(document_id)
        types = sorted(set(group["policy_type"].dropna().astype(str)))
        bundle_id = "BUNDLE930_" + hashlib.sha256(f"{city_text}|{doc_text}".encode()).hexdigest()[:16].upper()
        formal = bool((group["treatment_status"] == "CONFIRMED_INCLUDED").all())
        rows.append(
            {
                "episode_id": EPISODE_ID,
                "bundle_id": bundle_id,
                "city": city_text,
                "province": _text(group["province"].iloc[0]),
                "document_id": doc_text,
                "bundle_size": int(len(group)),
                "policy_types": json.dumps(types, ensure_ascii=False),
                "bundle_type": _bundle_type(types),
                "bundle_status": "BUNDLE_CONFIRMED" if formal else "MANUAL_REVIEW_REQUIRED",
                "treatment_status": "CONFIRMED_INCLUDED" if formal else "MANUAL_REVIEW_REQUIRED",
            }
        )
    return pd.DataFrame(rows, columns=columns)


def _build_city_master(reference: pd.DataFrame, candidates: pd.DataFrame, bundles: pd.DataFrame) -> pd.DataFrame:
    cities = reference[["city_id", "city", "province"]].drop_duplicates() if not reference.empty else pd.DataFrame(columns=["city_id", "city", "province"])
    rows = []
    for row in cities.to_dict("records"):
        city = _text(row.get("city"))
        group = candidates[candidates["city"].eq(city)] if not candidates.empty else pd.DataFrame()
        formal = group[group["treatment_status"].eq("CONFIRMED_INCLUDED")] if not group.empty else group
        dates = pd.to_datetime(formal["announcement_date"], errors="coerce") if not formal.empty else pd.Series(dtype="datetime64[ns]")
        effective = pd.to_datetime(formal["effective_date"], errors="coerce") if not formal.empty else pd.Series(dtype="datetime64[ns]")
        types = sorted(set(formal["policy_type"].dropna().astype(str))) if not formal.empty else []
        rows.append(
            {
                "episode_id": EPISODE_ID,
                "city_id": _text(row.get("city_id")),
                "city": city,
                "province": _text(row.get("province")),
                "treated": 1 if len(formal) else pd.NA,
                "treatment_status": "CONFIRMED_TREATED" if len(formal) else "MANUAL_REVIEW_REQUIRED",
                "first_announcement_date": dates.min().date().isoformat() if not dates.empty and dates.notna().any() else pd.NA,
                "first_effective_date": effective.min().date().isoformat() if not effective.empty and effective.notna().any() else pd.NA,
                "treatment_date_recommended": dates.min().date().isoformat() if not dates.empty and dates.notna().any() else pd.NA,
                "number_of_actions": int(len(formal)) if len(formal) else pd.NA,
                "candidate_action_count": int(len(group)),
                "policy_families": json.dumps(types, ensure_ascii=False),
                "bundle_type": _text(bundles[bundles["city"].eq(city)]["bundle_type"].iloc[0]) if not bundles.empty and not bundles[bundles["city"].eq(city)].empty else pd.NA,
                "treatment_confidence": "HIGH" if len(formal) else "UNRESOLVED",
            }
        )
    return pd.DataFrame(rows)


def _build_panel(city_master: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for row in city_master.to_dict("records"):
        treated = row.get("treated")
        resolved = row.get("treatment_status") == "CONFIRMED_TREATED"
        indicator = {policy: (0 if resolved else pd.NA) for policy in ALLOWED_POLICY_TYPES}
        for policy in json.loads(_text(row.get("policy_families")) or "[]") if resolved else []:
            indicator[policy] = 1
        rows.append(
            {
                "city_code": _text(row.get("city_id")),
                "city": _text(row.get("city")),
                "province": _text(row.get("province")),
                "episode_id": EPISODE_ID,
                "treated": treated if resolved else pd.NA,
                "announcement_treatment_date": row.get("first_announcement_date"),
                "effective_treatment_date": row.get("first_effective_date"),
                "recommended_treatment_date": row.get("treatment_date_recommended"),
                "admin_tightening": indicator.get("LIMIT_PURCHASE"),
                "commercial_credit_tightening": indicator.get("COMMERCIAL_DOWNPAYMENT"),
                "pf_tightening": indicator.get("PF_DOWNPAYMENT"),
                "limit_purchase": indicator.get("LIMIT_PURCHASE"),
                "limit_resale": indicator.get("LIMIT_RESALE"),
                "downpayment_tightening": indicator.get("COMMERCIAL_DOWNPAYMENT"),
                "bundle_size": pd.NA,
                "bundle_type": row.get("bundle_type"),
                "evidence_confidence": row.get("treatment_confidence"),
                "date_confidence": "UNRESOLVED" if not resolved else "RECORDED",
                "episode_confidence": "UNRESOLVED" if not resolved else "RECORDED",
            }
        )
    return pd.DataFrame(rows)


def _build_identification_audit(candidates: pd.DataFrame, city_master: pd.DataFrame, scope: Mapping[str, Any]) -> str:
    event_dates = pd.to_datetime(candidates["announcement_date"], errors="coerce") if not candidates.empty else pd.Series(dtype="datetime64[ns]")
    same_day = int(event_dates.dropna().value_counts().gt(1).sum()) if not event_dates.empty else 0
    later = int((event_dates.dropna().dt.year >= 2017).sum()) if not event_dates.empty else 0
    unknown = int((candidates.get("date_type", pd.Series(dtype=str)).astype(str) == "UNKNOWN_DATE_STATE").sum()) if not candidates.empty else 0
    unresolved = int((city_master.get("treatment_status", pd.Series(dtype=str)).astype(str) == "MANUAL_REVIEW_REQUIRED").sum()) if not city_master.empty else 0
    return f"""# EP930 Identification Audit

Status: **BLOCKED / DESIGN AUDIT ONLY**

This artifact is independent of outcomes. No outcome data, regression result,
significance result, or treatment-effect estimate was read.

## Frozen design

- Episode: `{EPISODE_ID}`
- Core treatment window: `{scope['core_window']['start']}` to `{scope['core_window']['end']}`
- Extended identification window: `{scope['extended_window']['start']}` to `{scope['extended_window']['end']}`
- Provenance window: `{scope['provenance_window']['start']}` to `{scope['provenance_window']['end']}`

## Audited risks

| Item | Count | Interpretation |
|---|---:|---|
| Candidate actions | {len(candidates)} | Candidate universe only |
| Formal treatment actions | {int((candidates.get('treatment_status', pd.Series(dtype=str)) == 'CONFIRMED_INCLUDED').sum()) if not candidates.empty else 0} | Must not be inferred from candidate rows |
| Candidate events dated 2017+ | {later} | Must remain outside EP930 |
| Same-day candidate event clusters | {same_day} | Bundle/timing review required |
| Unknown date states | {unknown} | Cannot be used as a closed treatment clock |
| Cities needing treatment adjudication | {unresolved} | No silent zero coding |

## Required before DID/event-study use

1. Close reference-vs-evidence city reconciliation.
2. Resolve action-level episode membership and geography.
3. Complete independent Pass1/Pass2 classification for included actions.
4. Preserve announcement and effective clocks separately.
5. Resolve 2017/recurrent treatment contamination and pre-930 tightening.
6. Close critical treatment gaps and manual adjudication.

No causal analysis is authorized by this audit.
"""


def _build_freeze_report(manifest: Mapping[str, Any]) -> str:
    counts = manifest["counts"]
    gates = manifest["gates"]
    return f"""# EP930 Econometric Freeze Report

## Status

`{manifest['econometric_grade_status']}` — `EP930_ECONOMETRIC_GRADE_V1` is **not frozen**.

The output is an auditable diagnostic snapshot. The formal treatment master is
empty unless every action-level gate passes. Candidate actions and provisional
records are not treatment coding.

## Current answers

- Reference events: {counts['reference_events']}; unresolved reference statuses: {counts['reference_unresolved']}.
- Evidence cities: {counts['evidence_cities']}; city discrepancies: {counts['city_discrepancies']}.
- Candidate actions: {counts['candidate_actions']}.
- Formal treatment actions: {counts['formal_actions']}.
- Bundles: {counts['bundles']}.
- Candidate actions with explicit effective date: {counts['explicit_effective_dates']}.
- Candidate actions with no explicit effective date: {counts['no_explicit_effective_dates']}.
- Unknown date states: {counts['unknown_date_states']}.
- Exclusion rows: {counts['exclusion_rows']}.
- Manual review rows: {counts['manual_review_rows']}.

## Gate result

{json.dumps(gates, ensure_ascii=False, indent=2)}

## Interpretation

The current source artifacts do not support a closed econometric treatment
universe. In particular, official evidence, geography, API Pass2, date state,
reference reconciliation, and critical treatment gaps remain unresolved. The
city panel seed uses explicit missing values for unresolved treatment rather
than coding untreated cities as zero.

## Design risks to disclose in a paper

- Search/provenance window is not the treatment window.
- Announcement and effective dates are separate clocks.
- Same-day actions may be bundles, not independent treatments.
- 2017 and later policies must not contaminate the 2016 episode.
- Earlier tightening may make 930 an additional, not first, treatment.
- Treatment selection is independent of outcomes.
"""


def build(data_root: Path, output_root: Path, timestamp: str | None = None) -> Path:
    data_root = data_root.resolve()
    episode_output = data_root / "outputs" / "special_projects" / "2016_930"
    timestamp = timestamp or datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    target = output_root / timestamp
    target.mkdir(parents=True, exist_ok=False)

    scope = _scope_definition(episode_output)
    curated = data_root / "curated"
    source_paths = {
        "scope": episode_output / "00_SCOPE" / "2016_930_SCOPE.json",
        "analysis_ready_scope": episode_output / "930_ANALYSIS_READY_SCOPE.json",
        "city_discovery": episode_output / "01_DISCOVERY" / "2016_930_CITY_DISCOVERY.parquet",
        "actions": curated / "policy_episode_actions.parquet",
        "documents": curated / "policy_episode_documents.parquet",
        "parameters": curated / "policy_episode_parameters.parquet",
        "date_audit": episode_output / "06_DATE_VERIFICATION" / "2016_930_DATE_AUDIT.parquet",
        "api_classification": episode_output / "05_API_CLASSIFICATION" / "2016_930_API_CLASSIFICATION.parquet",
        "gaps": curated / "policy_episode_gaps.parquet",
        "analysis_ready_gate": episode_output / "930_ANALYSIS_READY_GATE.json",
        "rolling_metrics": episode_output / "930_ANALYSIS_READY_ROLLING_METRICS.json",
        "provider_status": episode_output / "930_API_PROVIDER_STATUS.json",
    }
    discovery = _safe_read_parquet(source_paths["city_discovery"])
    actions = _safe_read_parquet(source_paths["actions"])
    documents = _safe_read_parquet(source_paths["documents"])
    parameters = _safe_read_parquet(source_paths["parameters"])
    date_audit = _safe_read_parquet(source_paths["date_audit"])
    api = _safe_read_parquet(source_paths["api_classification"])
    gaps = _safe_read_parquet(source_paths["gaps"])
    if not gaps.empty and "episode_id" in gaps.columns:
        gaps = gaps[gaps["episode_id"].astype(str).eq(EPISODE_ID)]

    candidates = _normalize_candidates(actions, documents, parameters, date_audit, api, gaps, scope)
    reference, reconciliation = _build_reference_tables(discovery, candidates, documents)
    matrix = _build_city_policy_matrix(reference, candidates, documents)
    dates = _build_date_audit(candidates)
    bundles = _build_bundles(candidates)
    city_master = _build_city_master(reference, candidates, bundles)
    panel = _build_panel(city_master)
    exclusions = candidates[candidates["exclusion_reason"].astype(str).ne("")].copy() if not candidates.empty else pd.DataFrame()
    formal = candidates[candidates["exclusion_reason"].astype(str).eq("")].copy() if not candidates.empty else pd.DataFrame()

    if not formal.empty:
        formal["bundle_id"] = formal.apply(
            lambda row: "BUNDLE930_" + hashlib.sha256(f"{row['city']}|{row['document_id']}".encode()).hexdigest()[:16].upper(), axis=1
        )
        formal["bundle_size"] = formal.groupby("bundle_id")["action_id"].transform("size")
        formal["co_treatment_types"] = formal.groupby("bundle_id")["policy_type"].transform(lambda s: json.dumps(sorted(set(s)), ensure_ascii=False))
    formal_master = _ensure_columns(formal, ACTION_MASTER_COLUMNS)
    formal_master["mechanism_labels"] = formal_master["mechanism_labels"].map(_json_text)
    for column in ["manual_review_required"]:
        if column in formal_master:
            formal_master[column] = False

    manual_action = candidates[candidates["exclusion_reason"].astype(str).ne("")].copy() if not candidates.empty else pd.DataFrame()
    manual_city = city_master[city_master["treatment_status"].astype(str).eq("MANUAL_REVIEW_REQUIRED")].copy() if not city_master.empty else pd.DataFrame()
    manual_matrix = matrix[matrix["state"].astype(str).eq("MANUAL_REVIEW_REQUIRED")].copy() if not matrix.empty else pd.DataFrame()
    manual_review = _ensure_columns(
        pd.concat(
            [
                manual_action.assign(review_type="ACTION", precise_question="Resolve action-level episode membership, official evidence, geography, dates, API status, and treatment eligibility."),
                manual_city.assign(review_type="CITY", precise_question="Resolve reference-vs-evidence city membership and treated/not-treated status."),
                manual_matrix.assign(review_type="CITY_POLICY_TYPE", precise_question="Resolve whether the city-policy-type cell is action, no action, not applicable, or unresolved."),
            ],
            ignore_index=True,
            sort=False,
        ),
        ["review_type", "city", "document_id", "action_id", "policy_type", "exclusion_reason", "precise_question", "official_url", "date_type", "treatment_status"],
    )

    reference_unresolved = int((reference.get("reference_status", pd.Series(dtype=str)).astype(str) == "MANUAL_REVIEW_REQUIRED").sum()) if not reference.empty else 0
    city_discrepancies = int((reconciliation.get("discrepancy", pd.Series(dtype=str)).astype(str) != "ALIGNED").sum()) if not reconciliation.empty else 0
    explicit_effective = int((dates.get("date_type", pd.Series(dtype=str)).astype(str).isin({"EXPLICIT_EFFECTIVE_DATE", "ACTION_SPECIFIC_EFFECTIVE_DATE"})).sum()) if not dates.empty else 0
    no_explicit = int((dates.get("date_type", pd.Series(dtype=str)).astype(str) == "NO_EXPLICIT_EFFECTIVE_DATE").sum()) if not dates.empty else 0
    unknown_dates = int((dates.get("date_type", pd.Series(dtype=str)).astype(str) == "UNKNOWN_DATE_STATE").sum()) if not dates.empty else 0
    critical_gap_count = int(candidates["gap_linked"].sum()) if not candidates.empty and "gap_linked" in candidates else 0
    gates = {
        "reference_coverage_closed": reference_unresolved == 0,
        "evidence_coverage_closed": city_discrepancies == 0,
        "episode_membership_closed": bool(len(formal) == len(candidates) and len(candidates) > 0),
        "city_policy_coverage_closed": bool(not matrix.empty and not matrix["state"].isin(["MANUAL_REVIEW_REQUIRED", "MISSING_OFFICIAL_DOCUMENT", "MISSING_ACTION_EXTRACTION", "DATE_UNRESOLVED", "TYPE_UNRESOLVED"]).any()),
        "official_evidence_pass": bool(not formal.empty and formal.get("official_evidence", pd.Series(dtype=bool)).all()),
        "action_extraction_pass": bool(len(formal) > 0),
        "date_state_pass": unknown_dates == 0 and not dates.get("conflict_flag", pd.Series(dtype=bool)).fillna(False).any(),
        "api_classification_pass": bool(len(formal) > 0 and (formal["api_pass1_status"].isin(FORMAL_API_STATUSES) & formal["api_pass2_status"].isin(FORMAL_API_STATUSES)).all()),
        "dedup_pass": bool(not candidates.empty and not candidates["exclusion_reason"].astype(str).str.contains("DUPLICATE", regex=False).any()),
        "critical_treatment_gaps_zero": critical_gap_count == 0,
        "unknown_treatment_states_zero": unknown_dates == 0,
    }
    gates["econometric_grade_pass"] = all(gates.values())

    source_manifest = {}
    for name, path in source_paths.items():
        item: dict[str, Any] = {"path": str(path), "exists": path.exists()}
        if path.exists():
            item["sha256"] = _sha256(path)
            if path.suffix == ".parquet":
                frame = _safe_read_parquet(path)
                item["rows"] = int(len(frame))
                item["columns"] = list(frame.columns)
        source_manifest[name] = item
    counts = {
        "reference_events": int(len(reference)),
        "reference_unresolved": reference_unresolved,
        "evidence_cities": int(reconciliation["evidence_identified"].sum()) if not reconciliation.empty else 0,
        "city_discrepancies": city_discrepancies,
        "candidate_actions": int(len(candidates)),
        "formal_actions": int(len(formal_master)),
        "bundles": int(len(bundles)),
        "explicit_effective_dates": explicit_effective,
        "no_explicit_effective_dates": no_explicit,
        "unknown_date_states": unknown_dates,
        "exclusion_rows": int(len(exclusions)),
        "manual_review_rows": int(len(manual_review)),
        "critical_treatment_gaps": critical_gap_count,
    }
    analysis_gate = _safe_read_json(source_paths["analysis_ready_gate"])
    rolling = _safe_read_json(source_paths["rolling_metrics"])
    consistency = {
        "analysis_ready_gate_core_gaps": (analysis_gate.get("analysis_ready_core_blocking_gaps") or {}).get("blocking_gap_count"),
        "rolling_metrics_core_gaps": (rolling.get("analysis_ready_core_blocking_gaps") or {}).get("blocking_gap_count"),
        "consistent": (analysis_gate.get("analysis_ready_core_blocking_gaps") or {}).get("blocking_gap_count") == (rolling.get("analysis_ready_core_blocking_gaps") or {}).get("blocking_gap_count"),
        "status": "PASS" if (analysis_gate.get("analysis_ready_core_blocking_gaps") or {}).get("blocking_gap_count") == (rolling.get("analysis_ready_core_blocking_gaps") or {}).get("blocking_gap_count") else "BLOCKED_SCOPE_METRIC_CONFLICT",
    }
    manifest: dict[str, Any] = {
        "episode_id": EPISODE_ID,
        "episode_name": EPISODE_NAME,
        "econometric_grade_status": "PASS" if gates["econometric_grade_pass"] else "BLOCKED",
        "freeze_status": "FROZEN" if gates["econometric_grade_pass"] else "NOT_FROZEN",
        "generated_at": datetime.now(UTC).isoformat(),
        "definition": scope,
        "source_hierarchy": ["OFFICIAL_POLICY", "OFFICIAL_REPRINT", "OFFICIAL_INTERPRETATION", "DISCOVERY_ONLY"],
        "policy_taxonomy": list(ALLOWED_POLICY_TYPES),
        "inclusion_rules": ["official evidence", "core treatment window", "TIGHTENING direction", "action-level extraction", "explicit geography", "API Pass1 and Pass2", "dedup", "critical gaps zero"],
        "exclusion_rules": ["outside episode window", "wrong year", "not tightening", "missing official evidence", "unknown date state", "unresolved geography", "incomplete API review", "critical gap", "duplicate"],
        "date_rules": ["announcement/publication/effective/implementation remain separate", "NO_EXPLICIT_EFFECTIVE_DATE is distinct from UNKNOWN_DATE_STATE", "no guessed dates"],
        "bundle_rules": ["same document and city may form a candidate bundle", "time proximity alone never confirms a bundle"],
        "api_provider": _safe_read_json(source_paths["provider_status"]),
        "scope_consistency": consistency,
        "gates": gates,
        "counts": counts,
        "source_inputs": source_manifest,
        "candidate_master_is_not_treatment": True,
        "outcome_data_read": False,
        "outcome_driven_selection": False,
    }
    _write_csv(target / "EP930_REFERENCE_EVENT_MASTER.csv", reference)
    _write_csv(target / "EP930_CITY_RECONCILIATION.csv", reconciliation)
    _write_csv(target / "EP930_CITY_POLICY_COVERAGE.csv", matrix)
    _write_csv(target / "EP930_ECONOMETRIC_CANDIDATE_ACTIONS.csv", candidates)
    _write_csv(target / "EP930_EXCLUSION_REGISTER.csv", exclusions)
    _write_csv(target / "EP930_ECONOMETRIC_ACTION_MASTER.csv", formal_master)
    _write_csv(target / "EP930_DATE_AUDIT.csv", dates)
    _write_csv(target / "EP930_BUNDLE_MASTER.csv", bundles)
    _write_csv(target / "EP930_EPISODE_CITY_MASTER.csv", city_master)
    _write_csv(target / "EP930_CITY_TREATMENT_PANEL_SEED.csv", panel)
    _write_xlsx(
        target / "EP930_COVERAGE_AUDIT.xlsx",
        {
            "Reference Events": reference,
            "City Coverage": reconciliation,
            "City x Policy Type": matrix,
            "Missing-Resolved": exclusions,
            "Date Reconciliation": dates,
            "Official Evidence": candidates[[c for c in ["city", "document_id", "action_id", "official_url", "official_evidence_status", "official_evidence", "treatment_status"] if c in candidates.columns]],
            "Manual Review": manual_review,
        },
    )
    _write_xlsx(
        target / "EP930_MANUAL_ADJUDICATION_QUEUE.xlsx",
        {"Action Review": manual_action, "City Review": manual_city, "Policy Type Review": manual_matrix},
    )
    _atomic_text(target / "EP930_IDENTIFICATION_AUDIT.md", _build_identification_audit(candidates, city_master, scope))
    _atomic_text(target / "EP930_ECONOMETRIC_FREEZE_REPORT.md", _build_freeze_report(manifest))
    _write_json(target / "EP930_ECONOMETRIC_MANIFEST.json", manifest)

    output_hashes = {}
    for path in sorted(target.iterdir()):
        if path.is_file() and path.name not in {"EP930_SHA256_MANIFEST.json"}:
            output_hashes[path.name] = _sha256(path)
    _write_json(
        target / "EP930_SHA256_MANIFEST.json",
        {"generated_at": datetime.now(UTC).isoformat(), "files": output_hashes},
    )
    return target


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-root", type=Path, default=Path(r"E:\Data Set\CRPD"))
    parser.add_argument(
        "--output-root",
        type=Path,
        default=Path(r"E:\Data Set\CRPD\outputs\special_projects\2016_930\econometric_grade"),
    )
    parser.add_argument("--timestamp", default=None)
    args = parser.parse_args()
    target = build(args.data_root, args.output_root, args.timestamp)
    print(json.dumps({"output": str(target), "status": "COMPLETED_READ_ONLY"}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
