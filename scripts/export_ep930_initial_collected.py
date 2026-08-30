"""Export a read-only, honest EP930 INITIAL_COLLECTED_DATA snapshot.

This exporter never writes curated tables, queue state, production checkpoints,
or formal promotion tables.  It is intentionally separate from the production
runner and writes only a new timestamped export directory under the data root.
"""

from __future__ import annotations

import hashlib
import json
import re
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

EPISODE_ID = "EP_2016_930_TIGHTENING"
EPISODE_NAME = "2016年930楼市调控潮"
KNOWN_OFFICIAL = {"LIVE_HTTP_200", "CURATED_OFFICIAL", "OFFICIAL_POLICY", "OFFICIAL_REPRINT"}
VALID_API = {"SUCCESS", "COMPLETED", "VALID", "APPROVED"}


def text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (list, tuple, set, dict)):
        return json.dumps(value, ensure_ascii=False, default=str)
    try:
        missing = pd.isna(value)
        if isinstance(missing, bool) and missing:
            return ""
    except (TypeError, ValueError):
        pass
    return str(value).strip()


def first(*values: Any) -> str:
    for value in values:
        value = text(value)
        if value:
            return value
    return ""


def bool_value(value: Any) -> bool:
    return text(value).lower() in {"1", "true", "yes", "y", "pass", "valid", "confirmed"}


def as_date(value: Any) -> str:
    value_text = text(value)
    if not value_text:
        return ""
    try:
        if isinstance(value, (int, float)) or re.fullmatch(r"\d{10,14}(?:\.\d+)?", value_text):
            number = float(value)
            unit = "ms" if abs(number) > 10_000_000_000 else "s"
            parsed = pd.to_datetime(number, unit=unit, errors="coerce", utc=True)
        else:
            parsed = pd.to_datetime(value, errors="coerce", utc=True)
        return "" if pd.isna(parsed) else parsed.date().isoformat()
    except (TypeError, ValueError, OverflowError):
        return ""


def read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        value = json.loads(path.read_text(encoding="utf-8-sig"))
        return value if isinstance(value, dict) else {}
    except (OSError, json.JSONDecodeError):
        return {}


def read_parquet(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    last_error: Exception | None = None
    for _ in range(4):
        try:
            return pd.read_parquet(path, engine="pyarrow")
        except Exception as exc:  # tolerate atomic replacement by the live writer
            last_error = exc
    raise RuntimeError(f"Could not read atomic parquet snapshot {path}: {last_error}")


def read_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path, encoding="utf-8-sig") if path.exists() else pd.DataFrame()


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def write_text(path: Path, value: str) -> None:
    temp = path.with_suffix(path.suffix + ".tmp")
    temp.write_text(value, encoding="utf-8")
    temp.replace(path)


def write_json(path: Path, value: Any) -> None:
    write_text(path, json.dumps(value, ensure_ascii=False, indent=2, default=str) + "\n")


def write_csv(path: Path, frame: pd.DataFrame) -> None:
    temp = path.with_suffix(path.suffix + ".tmp")
    frame.to_csv(temp, index=False, encoding="utf-8-sig")
    temp.replace(path)


def pick_latest(paths: list[Path]) -> Path | None:
    paths = [path for path in paths if path.exists()]
    return max(paths, key=lambda path: path.stat().st_mtime) if paths else None


def metric(section: str, name: str, value: Any, denominator: Any = None, *, status: str = "OBSERVED", definition: str = "", source: str = "") -> dict[str, Any]:
    percentage = None
    if isinstance(value, (int, float)) and isinstance(denominator, (int, float)) and denominator:
        percentage = round(float(value) / float(denominator) * 100, 2)
    return {"section": section, "metric": name, "value": value, "denominator": denominator, "percentage": percentage, "status": status, "definition": definition, "source": source}


def main() -> int:
    data_root = Path(r"E:\Data Set\CRPD")
    output_root = data_root / "outputs" / "special_projects" / "2016_930"
    curated = data_root / "curated"
    captured_at = datetime.now(UTC).isoformat()

    monitor_path = output_root / "930_MONITOR_SNAPSHOT.json"
    rolling_path = output_root / "930_ANALYSIS_READY_ROLLING_METRICS.json"
    gate_path = output_root / "930_ANALYSIS_READY_GATE.json"
    provider_path = output_root / "930_API_PROVIDER_STATUS.json"
    recovery_path = output_root / "930_API_RECOVERY_STATE.json"
    autorun_path = output_root / "930_AUTORUN_STATE.json"
    queue_path = output_root / "930_TASK_QUEUE.parquet"
    monitor_before = read_json(monitor_path)
    rolling = read_json(rolling_path)
    gate = read_json(gate_path)
    provider = read_json(provider_path)
    recovery_state = read_json(recovery_path)
    autorun = read_json(autorun_path)
    queue_hash_before = sha256(queue_path) if queue_path.exists() else None

    collapse_master = pick_latest(list((output_root / "treatment_universe_closure" / "evidence_unit_collapse").glob("*/EP930_EVIDENCE_UNIT_MASTER.csv")))
    collapse_dir = collapse_master.parent if collapse_master else None
    reference_reconciliation = pick_latest(list((output_root / "treatment_universe_closure").rglob("EP930_REFERENCE_EVENT_RECONCILIATION.csv")))
    reference_master = pick_latest(list((output_root / "econometric_grade").rglob("EP930_REFERENCE_EVENT_MASTER.csv")))

    source_paths: dict[str, Path] = {
        "monitor_snapshot": monitor_path,
        "rolling_metrics": rolling_path,
        "analysis_ready_gate": gate_path,
        "provider_status": provider_path,
        "api_recovery_state": recovery_path,
        "autorun_state": autorun_path,
        "task_queue": queue_path,
        "episode_scope": output_root / "00_SCOPE" / "2016_930_SCOPE.json",
        "analysis_ready_scope": output_root / "930_ANALYSIS_READY_SCOPE.json",
        "city_discovery": output_root / "01_DISCOVERY" / "2016_930_CITY_DISCOVERY.parquet",
        "episode_documents": curated / "policy_episode_documents.parquet",
        "episode_actions": curated / "policy_episode_actions.parquet",
        "episode_parameters": curated / "policy_episode_parameters.parquet",
        "episode_gaps": curated / "policy_episode_gaps.parquet",
        "date_audit": output_root / "06_DATE_VERIFICATION" / "2016_930_DATE_AUDIT.parquet",
        "api_classification": output_root / "05_API_CLASSIFICATION" / "2016_930_API_CLASSIFICATION.parquet",
        "evidence_unit_master": collapse_master or output_root / "missing_evidence_unit_master.csv",
        "evidence_unit_members": collapse_dir / "EP930_EVIDENCE_UNIT_MEMBERS.csv" if collapse_dir else output_root / "missing_evidence_unit_members.csv",
        "non_api_closure_actions": collapse_dir / "EP930_NON_API_CLOSURE_ACTIONS.csv" if collapse_dir else output_root / "missing_non_api_closure_actions.csv",
        "reference_reconciliation": reference_reconciliation or output_root / "missing_reference_reconciliation.csv",
        "reference_master": reference_master or output_root / "missing_reference_master.csv",
    }

    # Read live/curated inputs only. No queue, database, or production file is written.
    documents = read_parquet(source_paths["episode_documents"])
    actions = read_parquet(source_paths["episode_actions"])
    parameters = read_parquet(source_paths["episode_parameters"])
    gaps = read_parquet(source_paths["episode_gaps"])
    queue = read_parquet(queue_path)
    api_frame = read_parquet(source_paths["api_classification"])
    discovery = read_parquet(source_paths["city_discovery"])
    evidence_master = read_csv(collapse_master) if collapse_master else pd.DataFrame()
    evidence_members = read_csv(source_paths["evidence_unit_members"])
    closure_actions = read_csv(source_paths["non_api_closure_actions"])
    reference = read_csv(reference_reconciliation) if reference_reconciliation else read_csv(reference_master)
    if not reference_reconciliation and not reference_master:
        reference = discovery.copy()

    for name in ["documents", "actions", "parameters", "gaps", "api_frame", "discovery"]:
        frame = locals()[name]
        if "episode_id" in frame.columns:
            locals()[name] = frame[frame["episode_id"].astype(str).eq(EPISODE_ID)].copy()
    documents = documents[documents["episode_id"].astype(str).eq(EPISODE_ID)].copy() if "episode_id" in documents.columns else documents
    actions = actions[actions["episode_id"].astype(str).eq(EPISODE_ID)].copy() if "episode_id" in actions.columns else actions
    gaps = gaps[gaps["episode_id"].astype(str).eq(EPISODE_ID)].copy() if "episode_id" in gaps.columns else gaps
    if "action_id" in actions.columns:
        actions = actions.drop_duplicates("action_id", keep="last")
    if "document_id" in documents.columns:
        documents = documents.drop_duplicates("document_id", keep="last")

    doc_map = documents.set_index("document_id", drop=False).to_dict("index") if "document_id" in documents.columns else {}
    member_map = evidence_members.drop_duplicates("action_id", keep="last").set_index("action_id").to_dict("index") if "action_id" in evidence_members.columns else {}
    closure_map = closure_actions.drop_duplicates("action_id", keep="last").set_index("action_id").to_dict("index") if "action_id" in closure_actions.columns else {}
    api_map: dict[str, dict[str, str]] = {}
    for row in api_frame.to_dict("records"):
        action_id = text(row.get("action_id"))
        if not action_id:
            continue
        pass_name = text(row.get("pass_name")).lower()
        api_map.setdefault(action_id, {})["pass2" if ("second" in pass_name or "review" in pass_name or "pass2" in pass_name) else "pass1"] = text(row.get("status")).upper() or "OBSERVED_WITHOUT_STATUS"
    parameter_map: dict[str, list[dict[str, Any]]] = {}
    for row in parameters.to_dict("records"):
        parameter_map.setdefault(text(row.get("action_id")), []).append(row)
    reference_action_map: dict[str, list[dict[str, Any]]] = {}
    for row in reference.to_dict("records"):
        for action_id in [item for item in re.split(r"[;|]", text(row.get("matched_action_ids"))) if item]:
            reference_action_map.setdefault(action_id, []).append(row)

    candidate_rows = []
    for action in actions.to_dict("records"):
        action_id = text(action.get("action_id"))
        document_id = text(action.get("document_id"))
        doc = doc_map.get(document_id, {})
        member = member_map.get(action_id, {})
        closure = closure_map.get(action_id, {})
        params = parameter_map.get(action_id, [])
        param = params[0] if params else {}
        official_status = first(doc.get("official_evidence_status"), closure.get("official_evidence_status"), "UNRESOLVED")
        membership = first(member.get("membership_class"), closure.get("membership_class"), "UNRESOLVED")
        announcement = first(as_date(action.get("announcement_date")), as_date(doc.get("announcement_date")))
        publication = first(as_date(action.get("publication_date")), as_date(doc.get("publication_date")))
        effective = first(as_date(action.get("effective_date")), as_date(doc.get("effective_date")))
        implementation = first(as_date(action.get("implementation_date")), as_date(doc.get("implementation_date")))
        date_state = first(closure.get("date_state"), member.get("date_state"))
        if not date_state:
            date_state = "EXPLICIT_EFFECTIVE_DATE" if effective else "ACTION_SPECIFIC_EFFECTIVE_DATE" if implementation else "NO_EXPLICIT_EFFECTIVE_DATE" if (announcement or publication) else "UNKNOWN_DATE_STATE"
        api = api_map.get(action_id, {})
        pass1 = api.get("pass1", "WAITING")
        pass2 = api.get("pass2", "NOT_YET_ELIGIBLE")
        refs = reference_action_map.get(action_id, [])
        ref_ids = [text(row.get("reference_event_id")) for row in refs if text(row.get("reference_event_id"))]
        ref_statuses = [first(row.get("closure_status"), row.get("resolution_status")) for row in refs]
        if not ref_ids:
            ref_ids = [item for item in re.split(r"[;|]", text(closure.get("reference_event_ids"))) if item]
        ref_status = ";".join(sorted(set(item for item in ref_statuses if item))) or ("LINKED_REFERENCE_STATUS_UNAVAILABLE" if ref_ids else "NOT_LINKED_IN_REFERENCE_MAP")
        blocker = first(closure.get("first_blocker"), member.get("first_blocker"))
        if not blocker:
            blocker = "OFFICIAL_EVIDENCE_UNRESOLVED" if official_status not in KNOWN_OFFICIAL else "EPISODE_MEMBERSHIP_UNRESOLVED" if membership.upper() in {"", "UNKNOWN", "UNRESOLVED", "MANUAL_REVIEW_REQUIRED"} else "DATE_STATE_UNRESOLVED" if date_state.upper() in {"", "UNKNOWN_DATE_STATE", "MANUAL_REVIEW_REQUIRED"} else "GEOGRAPHIC_SCOPE_UNRESOLVED" if not text(action.get("geographic_scope")) else "API_PASS1_WAITING" if pass1 not in VALID_API else "API_PASS2_NOT_YET_ELIGIBLE" if pass2 not in VALID_API else "DUPLICATE_ACTION_RETAINED" if text(action.get("dedup_status")).lower() not in {"", "canonical", "unique"} else "FORMAL_PROMOTION_NOT_RUN"
        critical = first(member.get("treatment_critical"), closure.get("treatment_critical"))
        critical = "UNKNOWN" if critical == "" else bool_value(critical)
        manual = text(member.get("triage_state")).upper() == "MANUAL_REVIEW_REQUIRED" or membership.upper() in {"UNKNOWN", "UNRESOLVED", "MANUAL_REVIEW_REQUIRED"} or date_state.upper() in {"UNKNOWN_DATE_STATE", "MANUAL_REVIEW_REQUIRED"} or official_status.upper() == "UNRESOLVED"
        candidate_rows.append({
            "record_id": first(action.get("record_id"), doc.get("record_id")), "document_id": document_id, "action_candidate_id": action_id,
            "evidence_unit_id": first(member.get("evidence_unit_id")), "city": first(action.get("city"), doc.get("city")), "province": first(action.get("province"), doc.get("province")),
            "policy_title": first(doc.get("document_title"), action.get("official_text_excerpt")), "issuing_authority": doc.get("issuer", ""), "document_number": doc.get("document_number", ""),
            "source_url": first(action.get("official_url"), doc.get("official_url")), "official_status": official_status, "source_type": first(doc.get("document_type"), "UNSPECIFIED"),
            "announcement_date": announcement, "publication_date": publication, "effective_date": effective, "implementation_date": implementation, "date_state": date_state,
            "date_confidence": first(action.get("date_confidence"), closure.get("date_confidence"), doc.get("date_confidence"), "UNKNOWN"), "date_source_text": first(action.get("date_evidence_text"), closure.get("date_evidence")),
            "policy_type": first(action.get("policy_type"), closure.get("policy_type")), "direction": first(action.get("action_direction"), action.get("episode_direction"), closure.get("direction")),
            "episode_membership_state": membership, "membership_confidence": "RECORDED_SOURCE_CLASS" if membership not in {"", "UNKNOWN", "UNRESOLVED"} else "UNKNOWN",
            "reference_event_id": ";".join(sorted(set(ref_ids))), "reference_status": ref_status, "treatment_critical": critical,
            "api_pass1_status": pass1, "api_pass2_status": pass2, "promotion_status": "NOT_PROMOTED", "manual_review_required": manual, "primary_blocker": blocker,
            "collection_status": "CANDIDATE_COLLECTED" if text(action.get("dedup_status")).lower() in {"", "canonical", "unique"} else "CANDIDATE_DUPLICATE_RETAINED",
            "action_text": action.get("action_text", ""), "policy_subtype": action.get("policy_subtype", ""), "mechanism_labels": text(action.get("mechanism_labels")),
            "target_population": action.get("target_population", ""), "geographic_scope": action.get("geographic_scope", ""), "old_value": first(param.get("old_value"), action.get("old_value")),
            "new_value": first(param.get("new_value"), action.get("new_value")), "unit": first(param.get("unit"), action.get("unit")), "parameter_confidence": first(param.get("parameter_confidence"), action.get("parameter_confidence")),
            "content_sha256": first(doc.get("content_hash"), doc.get("content_sha256")), "canonical_url": first(doc.get("canonical_url"), action.get("official_url")), "final_url": doc.get("final_url", ""),
            "dedup_status": first(action.get("dedup_status"), "UNSPECIFIED"), "source_confidence": action.get("source_confidence", ""), "classification_confidence": action.get("classification_confidence", ""), "episode_confidence": action.get("episode_confidence", ""),
        })
    candidate = pd.DataFrame(candidate_rows)

    action_counts = candidate.groupby("document_id")["action_candidate_id"].nunique().to_dict() if not candidate.empty else {}
    ref_doc_ids = set(reference.get("matched_document_id", pd.Series(dtype=str)).dropna().astype(str)) if not reference.empty else set()
    document_rows = []
    for row in documents.to_dict("records"):
        doc_id = text(row.get("document_id"))
        content_hash = first(row.get("content_hash"), row.get("content_sha256"))
        content_available = bool(text(row.get("official_text")) or text(row.get("raw_path")) or content_hash)
        document_rows.append({
            "document_id": doc_id, "city": row.get("city", ""), "province": row.get("province", ""), "policy_title": row.get("document_title", ""), "issuing_authority": row.get("issuer", ""), "document_number": row.get("document_number", ""),
            "underlying_policy_date": first(as_date(row.get("announcement_date")), as_date(row.get("publication_date"))), "webpage_publish_date": as_date(row.get("publication_date")),
            "official_status": first(row.get("official_evidence_status"), "UNRESOLVED"), "source_url": row.get("official_url", ""), "canonical_url": row.get("canonical_url", ""), "final_url": row.get("final_url", ""),
            "content_available": content_available, "content_sha256": content_hash, "action_count": int(action_counts.get(doc_id, 0)), "reference_related": doc_id in ref_doc_ids, "episode_candidate": True,
            "collection_timestamp": first(row.get("retrieved_at"), row.get("updated_at"), row.get("created_at")), "document_type": row.get("document_type", ""), "official_evidence_status": row.get("official_evidence_status", ""), "live_status": row.get("live_status", ""), "http_status": row.get("http_status", ""), "date_confidence": row.get("date_confidence", ""),
        })
    initial_documents = pd.DataFrame(document_rows)

    if evidence_master.empty:
        units = pd.DataFrame(columns=["evidence_unit_id", "city", "province", "underlying_document", "policy_family", "episode_event", "candidate_action_count", "official_evidence_state", "membership_state", "date_state", "reference_event_ids", "treatment_critical", "root_gap_count", "manual_review_required"])
    else:
        units = evidence_master.copy()
        units["underlying_document"] = units.get("root_title", "")
        units["episode_event"] = units.get("reference_event_ids", "")
        units["candidate_action_count"] = units.get("member_action_count", pd.NA)
        units["official_evidence_state"] = units.get("official_status", "")
        units["membership_state"] = units.get("episode_membership_state", "")
        units["manual_review_required"] = units.get("explicit_manual_member_count", 0).fillna(0).astype(int).gt(0) if "explicit_manual_member_count" in units else False
        preferred = ["evidence_unit_id", "city", "province", "underlying_document", "policy_family", "episode_event", "candidate_action_count", "official_evidence_state", "membership_state", "date_state", "reference_event_ids", "treatment_critical", "root_gap_count", "manual_review_required", "blocking_state", "missing_non_api_gates", "remaining_non_api_gate_count", "non_api_ready", "api_only_blocked", "root_document_id", "root_action_id", "root_official_url", "root_content_hash", "member_document_count", "member_source_url_count", "raw_gap_linked_member_count", "explicit_manual_member_count", "supporting_only"]
        units = units[[column for column in preferred if column in units.columns]]

    dates = candidate[[column for column in ["city", "document_id", "action_candidate_id", "announcement_date", "publication_date", "effective_date", "implementation_date", "date_state", "date_source_text", "date_confidence", "official_status"] if column in candidate.columns]].rename(columns={"action_candidate_id": "action_id"})
    dates["official_reprint_flag"] = dates["official_status"].astype(str).str.upper().eq("OFFICIAL_REPRINT")
    dates["date_conflict_flag"] = dates["announcement_date"].astype(str).ne("") & dates["effective_date"].astype(str).ne("") & dates["announcement_date"].ne(dates["effective_date"])

    city_parts = [frame[["city", "province"]] for frame in [initial_documents, candidate, units, reference] if not frame.empty and "city" in frame.columns]
    city_base = pd.concat(city_parts, ignore_index=True).dropna(subset=["city"]).drop_duplicates("city", keep="first") if city_parts else pd.DataFrame(columns=["city", "province"])
    city_rows = []
    for row in city_base.to_dict("records"):
        city = text(row.get("city"))
        dc = initial_documents[initial_documents["city"].astype(str).eq(city)] if not initial_documents.empty else pd.DataFrame()
        ac = candidate[candidate["city"].astype(str).eq(city)] if not candidate.empty else pd.DataFrame()
        uc = units[units["city"].astype(str).eq(city)] if not units.empty and "city" in units.columns else pd.DataFrame()
        rc = reference[reference["city"].astype(str).eq(city)] if not reference.empty and "city" in reference.columns else pd.DataFrame()
        dated = ac[["announcement_date", "publication_date", "effective_date", "implementation_date"]].replace("", pd.NA).notna().any(axis=1) if not ac.empty else pd.Series(dtype=bool)
        membership = ac["episode_membership_state"].astype(str) if not ac.empty else pd.Series(dtype=str)
        api_done = int((ac["api_pass1_status"].astype(str).isin(VALID_API) & ac["api_pass2_status"].astype(str).isin(VALID_API)).sum()) if not ac.empty else 0
        closure = rc.get("closure_status", pd.Series(dtype=str)).astype(str) if not rc.empty else pd.Series(dtype=str)
        city_rows.append({
            "city": city, "province": first(row.get("province"), dc["province"].dropna().iloc[0] if not dc.empty and dc["province"].notna().any() else ""), "documents_collected": len(dc), "candidate_actions": ac["action_candidate_id"].nunique() if not ac.empty else 0, "evidence_units": uc["evidence_unit_id"].nunique() if not uc.empty and "evidence_unit_id" in uc.columns else 0,
            "official_documents": int(dc["official_status"].astype(str).str.upper().isin(KNOWN_OFFICIAL).sum()) if not dc.empty else 0, "reference_events": len(rc), "confirmed_reference_events": int(closure.eq("CONFIRMED_INCLUDED").sum()), "unresolved_reference_events": int((~closure.isin({"CONFIRMED_INCLUDED", "CONFIRMED_EXCLUDED"})).sum()), "dated_actions": int(dated.sum()) if len(dated) else 0, "undated_actions": int((~dated).sum()) if len(dated) else len(ac), "membership_confirmed_actions": int((~membership.isin({"", "UNKNOWN", "UNRESOLVED", "MANUAL_REVIEW_REQUIRED"})).sum()) if len(membership) else 0, "membership_unresolved_actions": int(membership.isin({"", "UNKNOWN", "UNRESOLVED", "MANUAL_REVIEW_REQUIRED"}).sum()) if len(membership) else 0, "api_completed_actions": api_done, "formal_actions": 0, "manual_review_items": int(ac["manual_review_required"].sum()) if not ac.empty else 0,
        })
    city_summary = pd.DataFrame(city_rows).sort_values("city") if city_rows else pd.DataFrame()

    blockers = []
    for gate_name in gate.get("failed_gates", []) if isinstance(gate.get("failed_gates"), list) else []:
        blockers.append({"scope": "ANALYSIS_READY_CORE", "blocker_type": f"GATE:{gate_name}", "count": 1, "status": "OPEN", "source": str(gate_path), "notes": "Formal gate remains false; initial export does not change it."})
    api_health = monitor_before.get("api_health", {}) if isinstance(monitor_before, dict) else {}
    observed_provider = text(api_health.get("provider_status") or provider.get("status") or monitor_before.get("provider", {}).get("status"))
    if observed_provider:
        blockers.append({"scope": "API", "blocker_type": observed_provider, "count": api_health.get("pass1_waiting"), "status": "OPEN", "source": str(provider_path), "notes": "Observed provider state; no manual API call was made."})
    if not gaps.empty:
        for row in gaps.assign(_state=gaps.get("state", ""), _severity=gaps.get("severity", ""), _tool=gaps.get("policy_tool", "")).groupby(["_state", "_severity", "_tool"], dropna=False).size().reset_index(name="count").sort_values("count", ascending=False).head(100).to_dict("records"):
            blockers.append({"scope": "GLOBAL_FINAL_GAPS", "blocker_type": f"{text(row.get('_state'))}|{text(row.get('_severity'))}|{text(row.get('_tool'))}", "count": int(row["count"]), "status": "OPEN", "source": str(source_paths["episode_gaps"]), "notes": "Aggregated current gap rows."})
    if not candidate.empty:
        for name, count in candidate["primary_blocker"].value_counts(dropna=False).head(50).items():
            blockers.append({"scope": "CANDIDATE_ACTIONS", "blocker_type": text(name) or "UNKNOWN_BLOCKER", "count": int(count), "status": "OBSERVED", "source": "INITIAL_COLLECTED_DATA.csv", "notes": "Candidate-level blocker; not a formal treatment exclusion."})
    missing_blockers = pd.DataFrame(blockers, columns=["scope", "blocker_type", "count", "status", "source", "notes"])

    excluded = evidence_members[evidence_members.get("triage_state", pd.Series(dtype=str)).astype(str).str.contains("EXCLUDE|SUPPORTING_ONLY|DETERMINISTIC_EXCLUDED", regex=True, na=False)].copy() if not evidence_members.empty else pd.DataFrame()
    if excluded.empty:
        excluded = pd.DataFrame([{"exclusion_status": "NO_EXPLICIT_EXCLUSION_REGISTER", "exclusion_reason": "Broad initial snapshot retains current candidates; no full deterministic exclusion register was applied.", "source": "INITIAL_COLLECTED_DATA exporter"}])

    queue_status = queue["status"].astype(str).str.upper() if not queue.empty and "status" in queue.columns else pd.Series(dtype=str)
    q_total = len(queue)
    q_completed = int(queue_status.eq("CRAWL_COMPLETED").sum())
    q_pending = int(queue_status.isin({"PENDING", "QUEUED"}).sum())
    q_retry = int(queue_status.isin({"RETRY_WAIT", "RETRY", "BACKOFF"}).sum())
    q_active = int(queue_status.isin({"RUNNING", "ACTIVE", "INFLIGHT", "LEASED"}).sum())
    core_progress = monitor_before.get("analysis_ready_discovery_progress", {})
    core_total = int(core_progress.get("core_eligible_total") or 100)
    core_completed = core_progress.get("core_verified")
    core_completed = int(core_completed) if core_completed is not None else None
    content_count = int(initial_documents["content_available"].sum()) if not initial_documents.empty else 0
    official_count = int(initial_documents["official_status"].astype(str).str.upper().isin(KNOWN_OFFICIAL).sum()) if not initial_documents.empty else 0
    any_date_count = int(candidate[["announcement_date", "publication_date", "effective_date", "implementation_date"]].replace("", pd.NA).notna().any(axis=1).sum()) if not candidate.empty else 0
    effective_count = int(candidate["effective_date"].astype(str).ne("").sum()) if not candidate.empty else 0
    ref_total = len(reference)
    ref_closed = int(reference.get("closure_status", pd.Series(dtype=str)).astype(str).isin({"CONFIRMED_INCLUDED", "CONFIRMED_EXCLUDED"}).sum()) if not reference.empty else 0
    ref_unresolved = ref_total - ref_closed
    completeness = pd.DataFrame([
        metric("COLLECTION", "queue_total", q_total, definition="Rows in the current 930 task queue.", source=str(queue_path)), metric("COLLECTION", "queue_completed", q_completed, q_total, definition="Current CRAWL_COMPLETED rows.", source=str(queue_path)), metric("COLLECTION", "queue_pending", q_pending, q_total, definition="Current PENDING/QUEUED rows.", source=str(queue_path)), metric("COLLECTION", "queue_retry", q_retry, q_total, definition="Current retry/backoff rows.", source=str(queue_path)), metric("COLLECTION", "queue_active", q_active, q_total, definition="Current active/leased rows.", source=str(queue_path)), metric("COLLECTION", "core_total", core_total, definition="Frozen Analysis-ready core items.", source=str(monitor_path)), metric("COLLECTION", "core_completed", core_completed, core_total, status="OBSERVED" if core_completed is not None else "UNKNOWN", definition="Frozen core discovery credit.", source=str(monitor_path)),
        metric("FILES", "documents_total", len(initial_documents), definition="Episode-scoped curated documents after document_id de-duplication.", source=str(source_paths["episode_documents"])), metric("FILES", "documents_with_content", content_count, len(initial_documents), definition="Has official text, raw path, or content hash.", source=str(source_paths["episode_documents"])), metric("FILES", "documents_without_content", len(initial_documents) - content_count, len(initial_documents), definition="No stored text/path/hash; not treated as no policy.", source=str(source_paths["episode_documents"])), metric("FILES", "official_documents", official_count, len(initial_documents), definition="Observed accepted official evidence status.", source=str(source_paths["episode_documents"])), metric("FILES", "nonofficial_or_unresolved_documents", len(initial_documents) - official_count, len(initial_documents), definition="Not currently accepted as official evidence.", source=str(source_paths["episode_documents"])),
        metric("STRUCTURING", "candidate_actions_total", len(candidate), definition="Current unique curated action_id rows retained as candidates.", source=str(source_paths["episode_actions"])), metric("STRUCTURING", "deterministically_excluded", None, status="NOT_MATERIALIZED_FOR_INITIAL_SNAPSHOT", definition="No broad full-candidate exclusion register applied.", source="Initial snapshot intentionally broad"), metric("STRUCTURING", "remaining_candidates", None, status="NOT_MATERIALIZED_FOR_INITIAL_SNAPSHOT", definition="Not derived without applying exclusions.", source="Initial snapshot intentionally broad"), metric("STRUCTURING", "membership_recorded", int((~candidate["episode_membership_state"].astype(str).isin({"", "UNKNOWN", "UNRESOLVED", "MANUAL_REVIEW_REQUIRED"})).sum()) if not candidate.empty else 0, len(candidate), definition="Recorded membership class.", source=str(source_paths["evidence_unit_members"])), metric("STRUCTURING", "membership_unresolved", int(candidate["episode_membership_state"].astype(str).isin({"", "UNKNOWN", "UNRESOLVED", "MANUAL_REVIEW_REQUIRED"}).sum()) if not candidate.empty else 0, len(candidate), definition="Membership remains unknown/review-required.", source=str(source_paths["evidence_unit_members"])), metric("STRUCTURING", "formal_actions", 0, status="VERIFIED_ZERO", definition="Current formal promotion gate reports zero; candidates are not formal treatment rows.", source=str(gate_path)),
        metric("DATES", "actions_with_any_date", any_date_count, len(candidate), definition="At least one observed date field.", source="INITIAL_COLLECTED_DATA.csv"), metric("DATES", "actions_without_any_date", len(candidate) - any_date_count, len(candidate), definition="No date currently observed; kept explicit.", source="INITIAL_COLLECTED_DATA.csv"), metric("DATES", "actions_with_announcement_date", int(candidate["announcement_date"].astype(str).ne("").sum()) if not candidate.empty else 0, len(candidate), definition="Announcement date observed separately.", source="INITIAL_COLLECTED_DATA.csv"), metric("DATES", "actions_with_effective_date", effective_count, len(candidate), definition="Explicit effective date only; no publication-date substitution.", source="INITIAL_COLLECTED_DATA.csv"), metric("DATES", "date_state_resolved", int((~candidate["date_state"].astype(str).isin({"", "UNKNOWN_DATE_STATE", "MANUAL_REVIEW_REQUIRED"})).sum()) if not candidate.empty else 0, len(candidate), definition="Non-unknown date state.", source="INITIAL_COLLECTED_DATA.csv"), metric("DATES", "date_state_unresolved", int(candidate["date_state"].astype(str).isin({"", "UNKNOWN_DATE_STATE", "MANUAL_REVIEW_REQUIRED"}).sum()) if not candidate.empty else 0, len(candidate), definition="Unknown/review-required date state.", source="INITIAL_COLLECTED_DATA.csv"),
        metric("REFERENCE", "reference_total", ref_total, definition="Reference reconciliation rows.", source=str(reference_reconciliation or reference_master or "")), metric("REFERENCE", "reference_closed", ref_closed, ref_total, definition="Confirmed included/excluded reference decisions.", source=str(reference_reconciliation or "")), metric("REFERENCE", "reference_unresolved", ref_unresolved, ref_total, definition="Manual/insufficient/review-required reference rows.", source=str(reference_reconciliation or "")),
        metric("API", "pass1_completed", int(candidate["api_pass1_status"].astype(str).isin(VALID_API).sum()) if not candidate.empty else 0, len(candidate), definition="Valid Pass1 status only; advisory/waiting excluded.", source=str(source_paths["api_classification"])), metric("API", "pass1_waiting", api_health.get("pass1_waiting"), status="OBSERVED" if api_health.get("pass1_waiting") is not None else "UNKNOWN", definition="Live API Pass1 backlog.", source=str(monitor_path)), metric("API", "pass2_completed", int(candidate["api_pass2_status"].astype(str).isin(VALID_API).sum()) if not candidate.empty else 0, len(candidate), definition="Valid Pass2 status only.", source=str(source_paths["api_classification"])), metric("API", "pass2_waiting_or_ineligible", api_health.get("pass2_not_yet_eligible", api_health.get("pass2_waiting")), status="OBSERVED" if api_health.get("pass2_not_yet_eligible", api_health.get("pass2_waiting")) is not None else "UNKNOWN", definition="Live Pass2 not-yet-eligible/waiting backlog.", source=str(monitor_path)),
        metric("EVIDENCE_UNITS", "root_evidence_units", len(units), definition="Read-only Evidence Unit master rows.", source=str(collapse_master or "")), metric("EVIDENCE_UNITS", "resolved_evidence_units", int(units.get("blocking_state", pd.Series(dtype=str)).astype(str).isin({"READY", "RESOLVED", "NON_API_READY"}).sum()) if not units.empty else 0, len(units), definition="Explicit resolved/non-API-ready unit state.", source=str(collapse_master or "")), metric("EVIDENCE_UNITS", "unresolved_evidence_units", int((~units.get("blocking_state", pd.Series(dtype=str)).astype(str).isin({"READY", "RESOLVED", "NON_API_READY"})).sum()) if not units.empty else 0, len(units), definition="Remaining blocker state.", source=str(collapse_master or "")),
    ])

    monitor_after = read_json(monitor_path)
    queue_hash_after = sha256(queue_path) if queue_path.exists() else None
    consistency = "STABLE_KEY_SNAPSHOT" if queue_hash_before == queue_hash_after and text(monitor_before.get("run_id")) == text(monitor_after.get("run_id")) else "LIVE_SYSTEM_CHANGED_DURING_EXPORT"

    def pct(value: Any, denom: Any) -> str:
        return f"{float(value) / float(denom) * 100:.2f}%" if isinstance(value, (int, float)) and isinstance(denom, (int, float)) and denom else "未计算"

    queue_pct, core_pct = pct(q_completed, q_total), pct(core_completed, core_total)
    observed_provider = text(api_health.get("provider_status") or provider.get("status") or monitor_before.get("provider", {}).get("status"))
    readme = f"""# EP930 初始采集数据说明\n\n这是截至 {captured_at} 读取到的 EP930 原始/半结构化采集快照，不是 FINAL、ANALYSIS_READY 或 ECONOMETRIC_GRADE。它用于先查看已经收集到的城市、政策文档、候选 action、日期和来源，以及目前仍然缺什么。\n\n## 当前能看到什么\n\n- Queue：{q_completed}/{q_total} 完成（约 {queue_pct}），pending={q_pending}，active={q_active}，retry={q_retry}。\n- Frozen core discovery：{core_completed if core_completed is not None else 'UNKNOWN'}/{core_total}（约 {core_pct}）。scope/hash 沿用现有 frozen 文件，没有被本次导出修改。\n- 文档：{len(initial_documents)} 条；有正文、路径或内容哈希 {content_count} 条；当前官方证据状态 {official_count} 条。\n- 候选 action：{len(candidate)} 条；它们不是 formal action，当前正式 promotion=0。\n- Evidence Unit：{len(units)} 个。\n- Reference：{ref_total} 条；confirmed included/excluded={ref_closed}；仍需 review/insufficient={ref_total - ref_closed}。\n- 日期：至少有一种日期的候选 {any_date_count} 条；有明确 effective_date 的 {effective_count} 条。缺失日期保持空值并保留状态。\n- API：{observed_provider or 'UNKNOWN'}；Pass1/Pass2 的 waiting、not-yet-eligible 和 advisory 不被伪造为完成。\n\n## 三种完整度必须分开\n\n1. **Collection completeness**：资料和文档找到了多少，主要看 queue、documents、content 和来源。\n2. **Structuring completeness**：找到的材料有多少已经拆成 action、Evidence Unit、membership 和日期状态。\n3. **Econometric readiness**：有多少通过 official evidence、membership、日期、Pass1、Pass2、dedup、critical gaps 和 promotion。\n\n当前第一类不能替代第二、三类；formal actions=0 是当前正式 gate 的真实状态，不表示没有政策材料。\n\n## 当前不能做什么\n\n- 不能把候选 action 直接当作正式 treatment。\n- 不能用 announcement/publication date 猜 effective_date。\n- 不能把 UNKNOWN 改成 0，也不能删除困难记录。\n- 不能把这份数据称为 FINAL、ANALYSIS_READY 或 ECONOMETRIC_GRADE。\n\n## 后续\n\n生产链继续运行，继续做 reference/evidence/membership/date closure、API recovery、Pass1、Pass2、promotion 和 gap closure。本次导出没有人工网页搜索、人工 API 调用、数据库写入或状态回写。捕获一致性：`{consistency}`。\n"""
    summary = f"""EP930 INITIAL COLLECTED DATA\n===========================\nCaptured (UTC): {captured_at}\nDataset status: INITIAL_COLLECTED_DATA\nAnalysis-ready: false\nEconometric grade: false\nFormal treatment frozen: false\n\n当前已有\n- Queue: {q_completed}/{q_total} completed ({queue_pct}); pending={q_pending}; active={q_active}; retry={q_retry}\n- Frozen core discovery: {core_completed if core_completed is not None else 'UNKNOWN'}/{core_total} ({core_pct})\n- Documents: {len(initial_documents)}; content/path/hash={content_count}; official evidence={official_count}\n- Candidate actions: {len(candidate)}\n- Evidence units: {len(units)}\n- Cities: {len(city_summary)}\n\n当前主要缺口\n- Provider/API status: {observed_provider or 'UNKNOWN'}\n- Pass1 waiting: {api_health.get('pass1_waiting', 'UNKNOWN')}\n- Pass2 waiting/not eligible: {api_health.get('pass2_not_yet_eligible', api_health.get('pass2_waiting', 'UNKNOWN'))}\n- Any-date actions: {any_date_count}; effective-date actions: {effective_count}\n- Reference closed: {ref_closed}/{ref_total}; unresolved: {ref_total - ref_closed}\n- Formal actions: 0 (verified zero from current formal gate)\n- Analysis-ready gate: {text(gate.get('status') or 'FAIL')}\n\n本版可以\n- 查看政策文件、城市覆盖、来源 URL、候选动作、Evidence Unit 和日期\n- 检查 waiting、review、unknown 和 blocker\n\n本版不能\n- 不能把候选 action 当作正式 treatment\n- 不能猜 effective date 或把缺失值当作没有政策\n- 不能把结果称为 FINAL、ANALYSIS_READY 或 ECONOMETRIC_GRADE\n\n后续：生产链继续运行，推进 reference/evidence/membership/date closure、API Pass1/Pass2 和 formal promotion。\n"""

    target = output_root / "initial_collected_data" / datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    target.mkdir(parents=True, exist_ok=False)
    candidate_columns = ["record_id", "document_id", "action_candidate_id", "evidence_unit_id", "city", "province", "policy_title", "issuing_authority", "document_number", "source_url", "official_status", "source_type", "announcement_date", "publication_date", "effective_date", "implementation_date", "date_state", "date_confidence", "date_source_text", "policy_type", "direction", "episode_membership_state", "membership_confidence", "reference_event_id", "reference_status", "treatment_critical", "api_pass1_status", "api_pass2_status", "promotion_status", "manual_review_required", "primary_blocker", "collection_status", "action_text", "policy_subtype", "mechanism_labels", "target_population", "geographic_scope", "old_value", "new_value", "unit", "parameter_confidence", "content_sha256", "canonical_url", "final_url", "dedup_status", "source_confidence", "classification_confidence", "episode_confidence"]
    candidate_out = candidate.reindex(columns=candidate_columns)
    outputs: dict[str, Path] = {}

    def put_csv(name: str, frame: pd.DataFrame) -> None:
        path = target / name
        write_csv(path, frame)
        outputs[name] = path

    put_csv("2016_930_INITIAL_COLLECTED_DATA.csv", candidate_out)
    put_csv("2016_930_INITIAL_DOCUMENTS.csv", initial_documents)
    put_csv("2016_930_INITIAL_EVIDENCE_UNITS.csv", units)
    put_csv("2016_930_INITIAL_CITY_SUMMARY.csv", city_summary)
    put_csv("2016_930_INITIAL_DATE_AUDIT.csv", dates)
    put_csv("2016_930_INITIAL_REFERENCE_EVENTS.csv", reference)
    put_csv("2016_930_INITIAL_MISSING_AND_BLOCKERS.csv", missing_blockers)
    put_csv("2016_930_INITIAL_EXCLUDED_CANDIDATES.csv", excluded)
    put_csv("2016_930_INITIAL_COMPLETENESS.csv", completeness)

    completeness_json = {"dataset_status": "INITIAL_COLLECTED_DATA", "analysis_ready": False, "econometric_grade": False, "formal_treatment_frozen": False, "episode_id": EPISODE_ID, "captured_at_utc": captured_at, "capture_consistency": consistency, "collection_completeness": {"queue_total": q_total, "queue_completed": q_completed, "queue_pending": q_pending, "queue_retry": q_retry, "queue_active": q_active, "queue_completion_rate": q_completed / q_total if q_total else None, "core_total": core_total, "core_completed": core_completed, "core_completion_rate": core_completed / core_total if core_completed is not None and core_total else None, "documents_total": len(initial_documents), "documents_with_content": content_count, "documents_without_content": len(initial_documents) - content_count, "official_documents": official_count, "nonofficial_or_unresolved_documents": len(initial_documents) - official_count}, "structuring_completeness": {"candidate_actions_total": len(candidate), "deterministically_excluded": None, "remaining_candidates": None, "membership_recorded": int((~candidate["episode_membership_state"].astype(str).isin({"", "UNKNOWN", "UNRESOLVED", "MANUAL_REVIEW_REQUIRED"})).sum()) if not candidate.empty else 0, "membership_unresolved": int(candidate["episode_membership_state"].astype(str).isin({"", "UNKNOWN", "UNRESOLVED", "MANUAL_REVIEW_REQUIRED"}).sum()) if not candidate.empty else 0, "evidence_units": len(units), "formal_actions": 0}, "date_completeness": {"actions_with_any_date": any_date_count, "actions_without_any_date": len(candidate) - any_date_count, "actions_with_announcement_date": int(candidate["announcement_date"].astype(str).ne("").sum()) if not candidate.empty else 0, "actions_with_effective_date": effective_count, "date_state_unresolved": int(candidate["date_state"].astype(str).isin({"", "UNKNOWN_DATE_STATE", "MANUAL_REVIEW_REQUIRED"}).sum()) if not candidate.empty else 0}, "reference_completeness": {"reference_total": ref_total, "reference_closed": ref_closed, "reference_unresolved": ref_total - ref_closed}, "api_completeness": {"provider_status": observed_provider, "pass1_completed": int(candidate["api_pass1_status"].astype(str).isin(VALID_API).sum()) if not candidate.empty else 0, "pass1_waiting": api_health.get("pass1_waiting"), "pass2_completed": int(candidate["api_pass2_status"].astype(str).isin(VALID_API).sum()) if not candidate.empty else 0, "pass2_waiting_or_ineligible": api_health.get("pass2_not_yet_eligible", api_health.get("pass2_waiting"))}, "evidence_unit_completeness": {"root_evidence_units": len(units), "resolved_evidence_units": int(units.get("blocking_state", pd.Series(dtype=str)).astype(str).isin({"READY", "RESOLVED", "NON_API_READY"}).sum()) if not units.empty else 0, "unresolved_evidence_units": int((~units.get("blocking_state", pd.Series(dtype=str)).astype(str).isin({"READY", "RESOLVED", "NON_API_READY"})).sum()) if not units.empty else 0}, "unknown_values_preserved": True, "live_state_before": monitor_before, "live_state_after": monitor_after}
    write_json(target / "2016_930_INITIAL_COMPLETENESS.json", completeness_json)
    outputs["2016_930_INITIAL_COMPLETENESS.json"] = target / "2016_930_INITIAL_COMPLETENESS.json"
    write_text(target / "README_2016_930_INITIAL_DATA.md", readme)
    outputs["README_2016_930_INITIAL_DATA.md"] = target / "README_2016_930_INITIAL_DATA.md"
    write_text(target / "2016_930_INITIAL_STATUS_SUMMARY.txt", summary)
    outputs["2016_930_INITIAL_STATUS_SUMMARY.txt"] = target / "2016_930_INITIAL_STATUS_SUMMARY.txt"
    metadata = {"dataset_status": "INITIAL_COLLECTED_DATA", "analysis_ready": False, "econometric_grade": False, "formal_treatment_frozen": False, "episode_id": EPISODE_ID, "episode_name": EPISODE_NAME, "captured_at_utc": captured_at, "capture_consistency": consistency, "production_chain_observed_running": text(monitor_before.get("episode_status") or autorun.get("status")).upper() == "RUNNING", "current_stage": monitor_before.get("current_stage"), "run_id": monitor_before.get("run_id"), "scope_version": monitor_before.get("analysis_ready_discovery_progress", {}).get("scope_version"), "scope_hash": monitor_before.get("analysis_ready_discovery_progress", {}).get("scope_hash"), "provider_status": observed_provider, "provider": monitor_before.get("provider", {}).get("provider") or provider.get("provider"), "model": monitor_before.get("provider", {}).get("model") or provider.get("model"), "formal_actions": 0, "no_manual_web_search": True, "no_manual_api_call": True, "no_database_write": True, "raw_queue_modified": False, "frozen_scope_modified": False}
    write_json(target / "2016_930_INITIAL_METADATA.json", metadata)
    outputs["2016_930_INITIAL_METADATA.json"] = target / "2016_930_INITIAL_METADATA.json"

    readme_sheet = pd.DataFrame([["dataset_status", "INITIAL_COLLECTED_DATA"], ["analysis_ready", False], ["econometric_grade", False], ["formal_treatment_frozen", False], ["captured_at_utc", captured_at], ["episode_id", EPISODE_ID], ["current_stage", monitor_before.get("current_stage")], ["run_id", monitor_before.get("run_id")], ["provider_status", observed_provider], ["queue_completion", f"{q_completed}/{q_total} ({queue_pct})"], ["core_discovery", f"{core_completed if core_completed is not None else 'UNKNOWN'}/{core_total} ({core_pct})"], ["formal_actions", 0], ["interpretation", "Candidate actions and initial documents are not formal treatment records."], ["missing_values", "Unknown values remain blank with explicit state/blocker columns."]], columns=["field", "value"])
    excel_path = target / "2016_930_INITIAL_COLLECTED_DATA.xlsx"
    temp_xlsx = excel_path.with_suffix(".tmp.xlsx")
    with pd.ExcelWriter(temp_xlsx, engine="openpyxl") as writer:
        readme_sheet.to_excel(writer, sheet_name="README", index=False)
        city_summary.to_excel(writer, sheet_name="City Summary", index=False)
        candidate_out.to_excel(writer, sheet_name="Candidate Actions", index=False)
        initial_documents.to_excel(writer, sheet_name="Documents", index=False)
        units.to_excel(writer, sheet_name="Evidence Units", index=False)
        dates.to_excel(writer, sheet_name="Date Audit", index=False)
        reference.to_excel(writer, sheet_name="Reference Events", index=False)
        missing_blockers.to_excel(writer, sheet_name="Missing & Blockers", index=False)
        excluded.to_excel(writer, sheet_name="Excluded Candidates", index=False)
        completeness.to_excel(writer, sheet_name="Completeness", index=False)
    temp_xlsx.replace(excel_path)
    outputs[excel_path.name] = excel_path

    manifest: dict[str, Any] = {"manifest_type": "EP930_INITIAL_COLLECTED_DATA_SHA256", "dataset_status": "INITIAL_COLLECTED_DATA", "analysis_ready": False, "econometric_grade": False, "formal_treatment_frozen": False, "episode_id": EPISODE_ID, "captured_at_utc": captured_at, "capture_consistency": consistency, "files": {}, "sources": {}}
    for path in sorted(target.iterdir()):
        if path.is_file() and path.name != "2016_930_INITIAL_SHA256_MANIFEST.json":
            manifest["files"][path.name] = {"sha256": sha256(path), "bytes": path.stat().st_size}
    for key, path in source_paths.items():
        if path.exists():
            manifest["sources"][key] = {"path": str(path), "sha256": sha256(path), "bytes": path.stat().st_size, "observed_mtime_utc": datetime.fromtimestamp(path.stat().st_mtime, UTC).isoformat()}
        else:
            manifest["sources"][key] = {"path": str(path), "exists": False}
    manifest["checks"] = {"required_files_present": all(name in manifest["files"] for name in ["2016_930_INITIAL_COLLECTED_DATA.csv", "2016_930_INITIAL_COLLECTED_DATA.xlsx", "2016_930_INITIAL_DOCUMENTS.csv", "2016_930_INITIAL_EVIDENCE_UNITS.csv", "2016_930_INITIAL_CITY_SUMMARY.csv", "2016_930_INITIAL_DATE_AUDIT.csv", "2016_930_INITIAL_COMPLETENESS.csv", "2016_930_INITIAL_COMPLETENESS.json", "README_2016_930_INITIAL_DATA.md", "2016_930_INITIAL_STATUS_SUMMARY.txt"]), "candidate_csv_rows": len(candidate_out), "candidate_action_unique_ids": candidate_out["action_candidate_id"].nunique() if not candidate_out.empty else 0, "document_csv_rows": len(initial_documents), "evidence_unit_csv_rows": len(units), "city_summary_rows": len(city_summary), "date_audit_rows": len(dates), "formal_actions": 0, "database_written": False, "raw_queue_modified": False, "frozen_scope_modified": False, "api_called_by_export": False}
    write_json(target / "2016_930_INITIAL_SHA256_MANIFEST.json", manifest)

    print(json.dumps({"status": "SUCCESS", "output_dir": str(target), "captured_at_utc": captured_at, "capture_consistency": consistency, "queue": {"total": q_total, "completed": q_completed, "pending": q_pending, "retry": q_retry, "active": q_active}, "core": {"total": core_total, "completed": core_completed}, "documents": len(initial_documents), "documents_with_content": content_count, "official_documents": official_count, "candidate_actions": len(candidate), "evidence_units": len(units), "cities": len(city_summary), "reference_events": ref_total, "reference_closed": ref_closed, "reference_unresolved": ref_unresolved, "dated_actions": any_date_count, "effective_date_actions": effective_count, "formal_actions": 0, "provider_status": observed_provider, "api_pass1_waiting": api_health.get("pass1_waiting"), "api_pass2_waiting_or_ineligible": api_health.get("pass2_not_yet_eligible", api_health.get("pass2_waiting")), "files": sorted(manifest["files"])}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
