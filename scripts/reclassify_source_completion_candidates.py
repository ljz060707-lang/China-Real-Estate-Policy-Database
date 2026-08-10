"""Apply deterministic evidence reclassification to an existing bounded run.

This script consumes an existing run's applied candidates only.  It performs
no search and no AI call; all writes go through the source candidate service
and retain the original probe evidence and classification fields.
"""

from __future__ import annotations

import argparse
import json
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path

import polars as pl

from policydb.parquet_store import read_parquet_snapshot
from policydb.settings import Settings
from policydb.source_candidate_triage import prefilter_candidate_frame
from policydb.source_jurisdiction import load_jurisdiction_mappings, mapping_for_candidate
from policydb.source_slots import (
    list_candidates,
    reclassify_candidate_after_probe,
    upsert_candidates,
)


def run(existing_run: Path, *, output: Path | None = None) -> dict:
    settings = Settings.discover()
    applied_path = existing_run / "applied_candidates.parquet"
    if not applied_path.exists():
        raise FileNotFoundError(applied_path)
    applied = read_parquet_snapshot(applied_path)
    mappings = load_jurisdiction_mappings(settings)
    prefetched = prefilter_candidate_frame(settings, applied, mappings=mappings)
    current = list_candidates(settings=settings)
    current_by_id = {str(row["candidate_id"]): row for row in current.iter_rows(named=True)}
    reclassification: list[dict] = []
    mapping_ids: set[str] = set()
    bundle_ids: set[str] = set()
    reason_counts: Counter[str] = Counter()
    for proposal in prefetched.iter_rows(named=True):
        candidate_id = str(proposal.get("candidate_id") or "")
        current_row = current_by_id.get(candidate_id)
        if not current_row:
            reason_counts["candidate_not_found"] += 1
            reclassification.append({"candidate_id": candidate_id, "reclassified": False, "reason_code": "candidate_not_found"})
            continue
        mapping = mapping_for_candidate(
            current_row,
            current_row,
            settings=settings,
            mappings=mappings,
        )
        update = {**current_row, **{key: value for key, value in proposal.items() if value is not None}}
        if mapping.get("status") == "PASS":
            mapping_ids.add(str(mapping.get("mapping_id")))
            bundle_ids.add(str(mapping.get("source_bundle_id")))
            mapping_obj = next((item for item in mappings if item.mapping_id == mapping.get("mapping_id")), None)
            update.update(
                {
                    "source_bundle_id": mapping.get("source_bundle_id"),
                    "jurisdiction_mapping_id": mapping.get("mapping_id"),
                    "jurisdiction_evidence_id": mapping.get("evidence_id"),
                    "jurisdiction_mapping_status": mapping.get("status"),
                    "jurisdiction_mapping_reason_code": mapping.get("reason_code"),
                    "homepage_url": mapping_obj.homepage_url if mapping_obj else None,
                    "list_page_urls": list(mapping_obj.list_page_urls) if mapping_obj else None,
                    "authority_level": mapping_obj.authority_level if mapping_obj else None,
                    "authority_name": mapping_obj.authority_name if mapping_obj else None,
                    "approval_status": mapping_obj.approval_status if mapping_obj else None,
                }
            )
            upsert_candidates([update], settings)
            result = reclassify_candidate_after_probe(
                candidate_id,
                settings=settings,
                run_id="SOURCE_RECLASSIFICATION_20260805",
            )
        else:
            # Keep the historical candidate row for auditability, but make the
            # deterministic prefilter decision authoritative for future
            # selection.  This is an official candidate-registry write; it is
            # deliberately not a direct Parquet mutation and never changes a
            # candidate to verified or enabled.
            update.update(
                {
                    "prefilter_status": proposal.get("prefilter_status") or "rejected",
                    "prefilter_reasons": proposal.get("prefilter_reasons"),
                    "prefilter_reason_codes": proposal.get("prefilter_reason_codes"),
                    "selected_top3": False,
                    "search_evidence_only": True,
                    "page_type": proposal.get("page_type") or "rejected_detail_or_legal_page",
                    "candidate_kind": "rejected_deterministic_prefilter",
                    "entry_eligible": False,
                    "verification_status": "rejected_deterministic_prefilter",
                    "is_verified": False,
                    "source_bundle_id": None,
                    "jurisdiction_mapping_id": None,
                    "jurisdiction_evidence_id": None,
                    "jurisdiction_mapping_status": mapping.get("status"),
                    "jurisdiction_mapping_reason_code": mapping.get("reason_code"),
                    "homepage_url": None,
                    "list_page_urls": None,
                    "authority_level": None,
                    "authority_name": None,
                    "approval_status": None,
                }
            )
            upsert_candidates([update], settings)
            result = {
                "candidate_id": candidate_id,
                "reclassified": False,
                "reason_code": "prefilter_rejected",
                "prefilter_reasons": proposal.get("prefilter_reasons"),
            }
        if not result.get("reclassified"):
            reason_counts[str(result.get("reason_code") or "unknown")] += 1
        reclassification.append(result)
    same_bundle_groups = 0
    if prefetched.height and "source_bundle_id" in prefetched.columns:
        same_bundle_groups = sum(
            count > 1
            for count in prefetched.filter(pl.col("source_bundle_id").is_not_null())
            .group_by("source_bundle_id")
            .len()
            .get_column("len")
            .to_list()
        )
    report = {
        "migration_batch_id": "SOURCE_RECLASSIFICATION_20260805",
        "created_at": datetime.now(UTC).isoformat(),
        "source_run": str(existing_run),
        "candidates_checked": int(applied.height),
        "candidates_reclassified": sum(bool(row.get("reclassified")) for row in reclassification),
        "jurisdiction_mappings_applied": len(mapping_ids),
        "source_bundles_created": len(bundle_ids),
        "duplicates_merged": same_bundle_groups,
        "candidates_still_rejected": sum(not bool(row.get("reclassified")) for row in reclassification),
        "reason_counts": dict(reason_counts),
        "candidate_results": reclassification,
        "search_calls": 0,
        "ai_calls": 0,
        "manual_parquet_edits": False,
    }
    target = output or settings.outputs / "acceptance" / "source_candidate_reclassification_20260805.json"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(report, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")
    return report


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--existing-run", type=Path, required=True)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    print(json.dumps(run(args.existing_run, output=args.output), ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    main()
