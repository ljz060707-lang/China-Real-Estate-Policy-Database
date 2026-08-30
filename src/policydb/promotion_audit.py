"""Deterministic, row-level promotion gate traces for bounded rehearsals."""
from __future__ import annotations

from collections.abc import Iterable
from typing import Any

import polars as pl

_GATES = (
    "extraction_gate",
    "verification_gate",
    "official_evidence_gate",
    "policy_type_gate",
    "direction_gate",
    "geography_gate",
    "date_gate",
    "dedup_gate",
    "manual_review_gate",
    "database_gate",
)


def _status(value: bool) -> str:
    return "PASS" if value else "FAIL"


def _nonempty(value: object) -> bool:
    return value is not None and str(value).strip() not in {"", "NONE", "NULL"}


def _unknown(value: object) -> bool:
    return str(value or "").strip().upper() in {"", "UNKNOWN", "UNRESOLVED", "NONE", "NULL"}


def build_promotion_gate_trace(
    actions: pl.DataFrame,
    documents: pl.DataFrame,
    *,
    verified_action_ids: Iterable[str] | None = None,
    dedup_action_ids: Iterable[str] | None = None,
    database_action_ids: Iterable[str] | None = None,
    manual_review_ids: Iterable[str] | None = None,
) -> pl.DataFrame:
    """Return one deterministic gate row per action candidate.

    ``database_action_ids=None`` is intentionally represented as ``PENDING``:
    a pre-import trace can select candidates without claiming that a database
    write already occurred.  A post-import trace must pass the persisted IDs.
    """

    doc_by_id = (
        {str(row.get("document_id")): row for row in documents.iter_rows(named=True)}
        if not documents.is_empty() and "document_id" in documents.columns
        else {}
    )
    verified = {str(value) for value in verified_action_ids or ()}
    deduped = {str(value) for value in dedup_action_ids or ()}
    database = None if database_action_ids is None else {str(value) for value in database_action_ids}
    manual = {str(value) for value in manual_review_ids or ()}
    rows: list[dict[str, Any]] = []
    for action in actions.iter_rows(named=True) if not actions.is_empty() else []:
        action_id = str(action.get("action_id") or "")
        document_id = str(action.get("document_id") or "")
        document = doc_by_id.get(document_id, {})
        direction = action.get("direction") or action.get("action_direction")
        effective_basis = action.get("effective_date_basis") or document.get("effective_date_basis")
        effective_date = action.get("effective_date") or document.get("effective_date")
        gates = {
            "extraction_gate": bool(action_id and document_id and _nonempty(action.get("action_text"))),
            "verification_gate": action_id in verified or (
                bool(document.get("is_formal_eligible")) and not _unknown(document.get("official_url"))
            ),
            "official_evidence_gate": bool(document.get("is_formal_eligible")) and _nonempty(
                action.get("official_url") or document.get("official_url")
            ),
            "policy_type_gate": not _unknown(action.get("policy_type")),
            "direction_gate": not _unknown(direction),
            "geography_gate": not _unknown(action.get("geographic_scope")),
            "date_gate": _nonempty(effective_basis) and effective_date is not None,
            "dedup_gate": action_id in deduped,
            "manual_review_gate": action_id not in manual and not bool(action.get("manual_review_required")),
        }
        if database is None:
            database_value = "PENDING"
        else:
            database_value = _status(action_id in database)
        first_failed = next((name for name in _GATES if name != "database_gate" and not gates[name]), None)
        if first_failed:
            promotion = "FAIL"
            reason = f"{first_failed} failed"
        elif database is None:
            promotion = "PENDING"
            reason = "database write not yet attempted"
        elif action_id not in database:
            promotion = "FAIL"
            first_failed = "database_gate"
            reason = "action id absent from persisted episode action table"
        else:
            promotion = "PASS"
            reason = None
        row = {
            "action_id": action_id,
            "document_id": document_id,
            **{name: _status(value) for name, value in gates.items()},
            "database_gate": database_value,
            "promotion_gate": promotion,
            "first_failed_gate": first_failed,
            "reason": reason,
            "eligible_for_import": first_failed is None and promotion in {"PENDING", "PASS"},
        }
        rows.append(row)
    schema = {name: pl.String for name in _GATES}
    schema.update(
        {
            "action_id": pl.String,
            "document_id": pl.String,
            "promotion_gate": pl.String,
            "first_failed_gate": pl.String,
            "reason": pl.String,
            "eligible_for_import": pl.Boolean,
        }
    )
    return pl.DataFrame(rows, schema=schema) if rows else pl.DataFrame(schema=schema)


__all__ = ["build_promotion_gate_trace"]
