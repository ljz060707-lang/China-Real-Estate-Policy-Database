"""Deterministic policy-relevance gates for formal record promotion.

The crawl/archive layers deliberately retain every fetched page.  This module
only decides whether a fetched version may enter the formal policy record
layer, and records demotions as an appendable audit table rather than deleting
evidence.
"""

from __future__ import annotations

import json
import os
import tempfile
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import polars as pl

from policydb.ingest.excel import RECORD_COLUMNS
from policydb.parquet_store import atomic_write_parquet, read_parquet_snapshot
from policydb.settings import Settings

# Keep these terms explicit and reviewable.  They are intentionally broad
# enough for the five registered source roles, but a source role alone never
# makes a document relevant.
REAL_ESTATE_TERMS = (
    "\u4f4f\u623f",
    "\u623f\u5730\u4ea7",
    "\u623f\u4ea7",
    "\u5546\u54c1\u623f",
    "\u4f4f\u5b85",
    "\u571f\u5730",
    "\u81ea\u7136\u8d44\u6e90",
    "\u56fd\u571f\u7a7a\u95f4",
    "\u89c4\u5212",
    "\u4f4f\u623f\u516c\u79ef\u91d1",
    "\u516c\u79ef\u91d1",
    "\u4fdd\u969c\u6027\u4f4f\u623f",
    "\u79df\u8d41\u4f4f\u623f",
    "\u623f\u5c4b\u79df\u8d41",
    "\u4e0d\u52a8\u4ea7",
    "\u9884\u552e",
    "\u5f81\u6536",
    "\u68da\u6539",
    "\u65e7\u6539",
    "\u57ce\u5e02\u66f4\u65b0",
    "\u5371\u65e7\u623f",
    "\u623f\u5c4b\u5f81\u6536",
    "\u5efa\u8bbe\u5de5\u7a0b",
    "\u623f\u4ef7",
    "\u697c\u5e02",
    "\u4f4f\u623f\u4fdd\u969c",
)

NON_POLICY_TERMS = (
    "\u4eba\u4e8b\u4efb\u514d",
    "\u5e72\u90e8\u4efb\u514d",
    "\u4efb\u514d",
    "\u8058\u4efb",
    "\u514d\u804c",
    "\u4eba\u4e8b",
    "\u62db\u8058",
    "\u91d1\u878d\u5e7f\u544a",
    "\u5e7f\u544a\u76d1\u7ba1",
    "\u5e7f\u544a\u76d1\u6d4b",
    "\u5e7f\u544a\u5de5\u4f5c",
    "\u5e7f\u544a\u8bbe\u8ba1",
)

POLICY_ACTION_TERMS = (
    "\u653f\u7b56",
    "\u901a\u77e5",
    "\u529e\u6cd5",
    "\u610f\u89c1",
    "\u89c4\u5b9a",
    "\u65b9\u6848",
    "\u7ec6\u5219",
    "\u516c\u544a",
    "\u5b9e\u65bd",
    "\u7ba1\u7406",
    "\u76d1\u7ba1",
    "\u51b3\u5b9a",
    "\u89c4\u5212",
)

RELEVANCE_AUDIT_NAME = "policy_relevance_audit.parquet"
RECENT_REJECTS_NAME = "RECENT_30D_RELEVANCE_REJECTS.parquet"


@dataclass(frozen=True)
class RelevanceDecision:
    status: str
    reason_codes: tuple[str, ...]
    positive_terms: tuple[str, ...]
    negative_terms: tuple[str, ...]
    action_terms: tuple[str, ...]
    evidence_excerpt: str

    @property
    def accepted(self) -> bool:
        return self.status == "PASS"


def _text(value: object) -> str:
    return str(value or "").strip()


def _matches(text: str, terms: Iterable[str]) -> tuple[str, ...]:
    folded = text.casefold()
    return tuple(term for term in terms if term.casefold() in folded)


def assess_document_relevance(
    title: object,
    body: object,
    *,
    source_role: object | None = None,
) -> RelevanceDecision:
    """Classify a document without using the source role as a proxy.

    ``RELEVANCE_REVIEW`` is intentionally not eligible for formal promotion:
    mixed positive/negative evidence needs a human or a later deterministic
    rule.  Crawl and archive evidence remain untouched in all cases.
    """

    title_text = _text(title)
    body_text = _text(body)
    evidence = f"{title_text}\n{body_text[:4000]}".strip()
    positives = _matches(evidence, REAL_ESTATE_TERMS)
    negatives = _matches(evidence, NON_POLICY_TERMS)
    actions = _matches(evidence, POLICY_ACTION_TERMS)
    role = _text(source_role)
    reasons: list[str] = []
    if positives and negatives:
        reasons.append("mixed_real_estate_and_non_policy_evidence")
        status = "RELEVANCE_REVIEW"
    elif negatives and not positives:
        reasons.append("explicit_non_policy_document")
        status = "REJECT_NON_POLICY"
    elif not positives:
        reasons.append("no_real_estate_policy_evidence")
        status = "OUT_OF_SCOPE"
    elif not actions:
        # A positive topic mention without a policy/action signal can be an
        # agency landing page or a generic notice.  Keep it out of the formal
        # layer until the text contains a policy action/context term.
        reasons.append("real_estate_term_without_policy_action")
        status = "RELEVANCE_REVIEW"
    else:
        reasons.append("real_estate_policy_and_action_evidence")
        status = "PASS"
    if role:
        reasons.append(f"source_role={role}")
    return RelevanceDecision(
        status=status,
        reason_codes=tuple(reasons),
        positive_terms=positives,
        negative_terms=negatives,
        action_terms=actions,
        evidence_excerpt=evidence[:600],
    )


def _joined_versions(settings: Settings, *, run_ids: Iterable[str] | None = None) -> pl.DataFrame:
    path = settings.curated / "policy_document_versions.parquet"
    if not path.exists():
        return pl.DataFrame()
    versions = read_parquet_snapshot(path)
    item_path = settings.curated / "crawl_items.parquet"
    if item_path.exists() and "crawl_item_id" in versions.columns:
        items = read_parquet_snapshot(item_path)
        columns = [c for c in ("item_id", "run_id", "city_id", "candidate_date", "candidate_date_source") if c in items.columns]
        if "item_id" in columns:
            versions = versions.join(items.select(columns), left_on="crawl_item_id", right_on="item_id", how="left")
    selected = {str(value) for value in (run_ids or ()) if value}
    if selected and "run_id" in versions.columns:
        versions = versions.filter(pl.col("run_id").cast(pl.String).is_in(selected))
    return versions


def _append_note(old: object, note: str) -> str:
    previous = _text(old)
    return f"{previous};{note}" if previous else note


def _atomic_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, indent=2, default=str)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _audit_rows(versions: pl.DataFrame, settings: Settings) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    sources: dict[str, str] = {}
    try:
        from policydb.crawl.registry import load_registry

        sources = {
            str(source.source_id): _text(source.agency_type or source.source_role)
            for source in load_registry(settings)
        }
    except (FileNotFoundError, ValueError):
        pass
    audit_rows: list[dict[str, Any]] = []
    rejects: list[dict[str, Any]] = []
    for row in versions.iter_rows(named=True):
        decision = assess_document_relevance(
            row.get("title"),
            row.get("extracted_text"),
            source_role=sources.get(_text(row.get("source_id"))),
        )
        item = {
            "document_version_id": _text(row.get("document_version_id")),
            "record_id": _text(row.get("record_id")) or None,
            "run_id": _text(row.get("run_id")) or None,
            "city_id": _text(row.get("city_id")) or None,
            "source_id": _text(row.get("source_id")) or None,
            "title": _text(row.get("title")),
            "canonical_url": _text(row.get("canonical_url")) or None,
            "relevance_status": decision.status,
            "reason_codes": json.dumps(decision.reason_codes, ensure_ascii=False),
            "positive_terms": json.dumps(decision.positive_terms, ensure_ascii=False),
            "negative_terms": json.dumps(decision.negative_terms, ensure_ascii=False),
            "action_terms": json.dumps(decision.action_terms, ensure_ascii=False),
            "evidence_excerpt": decision.evidence_excerpt,
            "audited_at": datetime.now(UTC).replace(microsecond=0),
        }
        audit_rows.append(item)
        if not decision.accepted:
            rejects.append(item)
    return audit_rows, rejects


def _write_audit(settings: Settings, rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    path = settings.curated / RELEVANCE_AUDIT_NAME
    old = read_parquet_snapshot(path) if path.exists() else pl.DataFrame()
    new = pl.DataFrame(rows, infer_schema_length=None)
    combined = pl.concat([old, new], how="diagonal_relaxed") if old.height or old.columns else new
    combined = combined.unique(subset=["document_version_id"], keep="last", maintain_order=True)
    atomic_write_parquet(combined, path, {"module": "ingest.relevance", "operation": "audit"}, key_columns=("document_version_id",))


def _apply_rejections(settings: Settings, rejects: list[dict[str, Any]]) -> int:
    record_ids = {str(row["record_id"]) for row in rejects if row.get("record_id")}
    path = settings.curated / "records.parquet"
    if not record_ids or not path.exists():
        return 0
    records = read_parquet_snapshot(path)
    if "record_id" not in records.columns:
        return 0
    changed = 0
    rows: list[dict[str, Any]] = []
    reject_by_id = {str(row["record_id"]): row for row in rejects if row.get("record_id")}
    for row in records.iter_rows(named=True):
        record_id = str(row.get("record_id"))
        if record_id not in record_ids:
            rows.append(row)
            continue
        decision = reject_by_id[record_id]
        updated = dict(row)
        updated["record_type"] = "non_policy_evidence"
        updated["status"] = "excluded_non_policy"
        updated["manual_review_status"] = "rejected"
        updated["notes"] = _append_note(
            row.get("notes"),
            f"relevance_gate={decision['relevance_status']}:{decision['reason_codes']}",
        )
        updated["updated_at"] = datetime.now(UTC).replace(microsecond=0)
        rows.append(updated)
        changed += 1
    if changed:
        frame = pl.DataFrame(rows, infer_schema_length=None)
        # Preserve every existing column and use RECORD_COLUMNS when available;
        # the current store is allowed to carry additive audit columns.
        if set(RECORD_COLUMNS).issubset(frame.columns):
            frame = frame.select(RECORD_COLUMNS + [c for c in frame.columns if c not in RECORD_COLUMNS])
        atomic_write_parquet(frame, path, {"module": "ingest.relevance", "operation": "demote_non_policy"}, key_columns=("record_id",))
    return changed


def audit_recent_relevance(
    settings: Settings | None = None,
    *,
    run_ids: Iterable[str] | None = None,
    apply: bool = True,
) -> dict[str, Any]:
    """Audit a recent run scope and write a non-destructive reject register."""

    settings = settings or Settings.discover()
    versions = _joined_versions(settings, run_ids=run_ids)
    audit_rows, rejects = _audit_rows(versions, settings)
    if apply:
        _write_audit(settings, audit_rows)
    demoted = _apply_rejections(settings, rejects) if apply else 0
    output_root = settings.outputs / "recent_30d"
    output_root.mkdir(parents=True, exist_ok=True)
    reject_frame = pl.DataFrame(rejects, infer_schema_length=None) if rejects else pl.DataFrame(
        schema={
            "document_version_id": pl.String,
            "record_id": pl.String,
            "run_id": pl.String,
            "city_id": pl.String,
            "source_id": pl.String,
            "title": pl.String,
            "canonical_url": pl.String,
            "relevance_status": pl.String,
            "reason_codes": pl.String,
            "positive_terms": pl.String,
            "negative_terms": pl.String,
            "action_terms": pl.String,
            "evidence_excerpt": pl.String,
            "audited_at": pl.Datetime(time_unit="us", time_zone="UTC"),
        }
    )
    reject_path = output_root / RECENT_REJECTS_NAME
    previous_rejects = read_parquet_snapshot(reject_path) if reject_path.exists() else pl.DataFrame()
    if previous_rejects.height or previous_rejects.columns:
        reject_frame = pl.concat([previous_rejects, reject_frame], how="diagonal_relaxed")
        if "document_version_id" in reject_frame.columns:
            reject_frame = reject_frame.unique(subset=["document_version_id"], keep="last", maintain_order=True)
    atomic_write_parquet(
        reject_frame,
        reject_path,
        {"module": "ingest.relevance", "operation": "recent_rejects"},
        key_columns=("document_version_id",),
    )
    return {
        "selected_versions": versions.height,
        "audited_versions": len(audit_rows),
        "rejected_versions": len(rejects),
        "demoted_records": demoted,
        "reject_path": str(reject_path),
        "apply": apply,
    }


def backfill_publication_dates(settings: Settings | None = None, *, apply: bool = True) -> dict[str, Any]:
    """Fill only null publication dates from an explicitly stored record date."""

    settings = settings or Settings.discover()
    path = settings.curated / "records.parquet"
    if not path.exists():
        return {"selected": 0, "backfilled": 0, "apply": apply}
    records = read_parquet_snapshot(path)
    if "publication_date" not in records.columns or "record_date" not in records.columns:
        return {"selected": records.height, "backfilled": 0, "apply": apply}
    missing = records.filter(pl.col("publication_date").is_null() & pl.col("record_date").is_not_null())
    if apply and missing.height:
        missing_condition = pl.col("publication_date").is_null() & pl.col("record_date").is_not_null()
        records = records.with_columns(
            pl.when(missing_condition)
            .then(pl.col("record_date"))
            .otherwise(pl.col("publication_date"))
            .alias("publication_date")
        )
        if "notes" in records.columns:
            records = records.with_columns(
                pl.when(missing_condition)
                .then(pl.col("notes").fill_null("") + ";publication_date_fallback=record_date")
                .otherwise(pl.col("notes"))
                .alias("notes")
            )
        atomic_write_parquet(records, path, {"module": "ingest.relevance", "operation": "publication_date_backfill"}, key_columns=("record_id",))
    return {"selected": missing.height, "backfilled": missing.height if apply else 0, "apply": apply}


__all__ = [
    "RECENT_REJECTS_NAME",
    "RelevanceDecision",
    "assess_document_relevance",
    "audit_recent_relevance",
    "backfill_publication_dates",
]
