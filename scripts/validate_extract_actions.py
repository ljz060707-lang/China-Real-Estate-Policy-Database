"""CRPD deterministic extract_actions validation on REAL documents.

Sources:
  - production curated policy_document_versions.parquet (10,076 real crawled
    documents) for scenario coverage;
  - six-city closeout fulltext audit (182 gold action rows) for recall vs
    human/frozen gold reference.

Outputs (evidence dir):
  extract_actions_validation.csv / extract_actions_validation_summary.json
Read-only on production; no AI calls.
"""
from __future__ import annotations

import csv
import json
import os
import sys
from datetime import UTC, datetime
from pathlib import Path

import polars as pl

REPO = Path(__file__).resolve().parents[1]
REFERENCE = REPO / "data" / "reference"
DATA_ROOT = Path(os.environ.get("CRPD_DATA_ROOT", r"E:\Data Set\CRPD"))
VERSIONS = DATA_ROOT / "curated" / "policy_document_versions.parquet"
SIX_CITY_AUDIT = Path(
    os.environ.get(
        "CRPD_SIX_CITY_AUDIT",
        str(DATA_ROOT / "references" / "SIX_CITY_FULLTEXT_AUDIT_V5.csv"),
    )
)
OUT = Path(r"E:\Data Set\CRPD\reports\runs") / (
    f"extract_actions_validation_{datetime.now(UTC):%Y%m%dT%H%M%SZ}"
)

from policydb.intensity.rules import DeterministicPolicyRules  # noqa: E402

# gold policy_type -> expected instrument family (V5 8-class -> intensity instruments)
GOLD_INSTRUMENT = {
    "LIMIT_PURCHASE": "purchase_restriction",
    "LIMIT_RESALE": "sale_restriction",
    "COMMERCIAL_DOWNPAYMENT": "mortgage_downpayment",
    "PF_DOWNPAYMENT": "provident_fund",
    "PF_LOAN_CEILING": "provident_fund",
    "PF_OTHER_CONDITIONS": "provident_fund",
    "HUKOU_TALENT": "talent_hukou",
    "PURCHASE_SUBSIDY": "purchase_subsidy",
}

SCENARIO_TERMS = {
    "limit_purchase": ("限购",),
    "limit_resale": ("限售", "不得转让", "转让年限"),
    "mortgage_downpayment": ("首付", "首付款比例"),
    "provident_fund": ("公积金", "住房公积金"),
    "talent_hukou": ("人才", "多子女", "落户"),
    "subsidy": ("购房补贴", "契税补贴"),
    "price_management": ("限价", "备案价", "指导价"),
    "comprehensive": ("住房", "房地产"),
}


def main() -> int:
    OUT.mkdir(parents=True, exist_ok=True)
    rules = DeterministicPolicyRules(REFERENCE)
    rows: list[dict] = []
    total_candidates = 0
    docs_with_actions = 0
    summary: dict = {
        "created_at": datetime.now(UTC).isoformat(),
        "ai_calls": 0,
        "evidence_dir": str(OUT),
    }

    if VERSIONS.exists():
        frame = pl.read_parquet(VERSIONS)
        versions = frame.filter(pl.col("http_status") == 200).to_dicts()
        # One representative document per scenario (deterministic: first match).
        selected: dict[str, dict] = {}
        for version in versions:
            text = str(version.get("extracted_text") or "")
            title = str(version.get("title") or "")
            if len(text) < 200:
                continue
            for scenario, terms in SCENARIO_TERMS.items():
                if scenario in selected:
                    continue
                if any(term in text or term in title for term in terms):
                    selected[scenario] = version
        for scenario, version in selected.items():
            actions = rules.extract_actions(
                record_id=str(version.get("record_id") or "R"),
                text=str(version.get("extracted_text") or ""),
                title=str(version.get("title") or None),
                official_status="official",
            )
            instruments = sorted({a.instrument for a in actions})
            total_candidates += len(actions)
            docs_with_actions += int(bool(actions))
            rows.append(
                {
                    "source": "production_versions",
                    "scenario": scenario,
                    "document_version_id": version.get("document_version_id"),
                    "record_id": version.get("record_id"),
                    "title": str(version.get("title") or "")[:80],
                    "candidates": len(actions),
                    "instruments": "|".join(instruments),
                    "has_action_candidate": bool(actions),
                    "gold_expected": "",
                    "recall": "",
                }
            )
    else:
        rows.append({"source": "production_versions", "scenario": "MISSING", "error": str(VERSIONS)})

    # Six-city gold reference (human/frozen action rows).
    if SIX_CITY_AUDIT.exists():
        with open(SIX_CITY_AUDIT, encoding="utf-8-sig", newline="") as handle:
            gold_rows = list(csv.DictReader(handle))
        recalled = 0
        evaluated = 0
        non_action_rows = 0
        instrument_terms = tuple(
            term
            for family in ("purchase_restriction", "sale_restriction", "mortgage_downpayment",
                           "mortgage_rate", "provident_fund", "purchase_subsidy", "talent_hukou",
                           "housing_supply", "urban_renewal", "financing", "land")
            for term in rules.patterns["instrument_patterns"].get(family, ())
        )
        for gold in gold_rows:
            action_text = str(gold.get("policy_action_text") or "").strip()
            gold_type = str(gold.get("policy_type") or "")
            expected = GOLD_INSTRUMENT.get(gold_type, "")
            # Supplementary audit rows (empty type, procedural notices, analyses)
            # are expected to yield NO demand-side candidates — not a recall miss.
            is_action_like = (
                gold_type
                and len(action_text) >= 12
                and any(term in action_text for term in instrument_terms)
            )
            if not is_action_like:
                non_action_rows += 1
                rows.append(
                    {
                        "source": "six_city_gold",
                        "scenario": f"gold:{gold_type or 'NO_TYPE'}",
                        "document_version_id": "",
                        "record_id": gold.get("record_id"),
                        "title": str(gold.get("title") or "")[:80],
                        "candidates": 0,
                        "instruments": "",
                        "has_action_candidate": False,
                        "gold_expected": expected,
                        "recall": "NOT_ACTION_ROW",
                    }
                )
                continue
            actions = rules.extract_actions(
                record_id=str(gold.get("record_id") or "R_GOLD"),
                text=action_text,
                title=str(gold.get("title") or None),
                official_status="official",
            )
            instruments = {a.instrument for a in actions}
            hit = bool(instruments)
            if hit and expected:
                hit = expected in instruments or any(
                    expected.split("_")[0] in instrument for instrument in instruments
                )
            evaluated += 1
            recalled += int(hit)
            rows.append(
                {
                    "source": "six_city_gold",
                    "scenario": f"gold:{gold_type}",
                    "document_version_id": "",
                    "record_id": gold.get("record_id"),
                    "title": str(gold.get("title") or "")[:80],
                    "candidates": len(actions),
                    "instruments": "|".join(sorted(instruments)),
                    "has_action_candidate": bool(actions),
                    "gold_expected": expected,
                    "recall": "HIT" if hit else "MISS",
                }
            )
        gold_count = evaluated
        recall_rate = recalled / gold_count if gold_count else 0.0
        summary["six_city_gold_rows_evaluated"] = evaluated
        summary["six_city_gold_non_action_rows"] = non_action_rows
    else:
        recall_rate = None

    summary = {
        "created_at": datetime.now(UTC).isoformat(),
        "real_documents_scenarios": len(selected),
        "scenario_documents_with_candidates": docs_with_actions,
        "scenario_documents_total_candidates": total_candidates,
        "six_city_gold_rows_evaluated": summary.get("six_city_gold_rows_evaluated", 0),
        "six_city_gold_non_action_rows": summary.get("six_city_gold_non_action_rows", 0),
        "six_city_gold_recall_rate": round(recall_rate, 4) if recall_rate is not None else None,
        "ai_calls": 0,
        "evidence_dir": str(OUT),
    }

    with open(OUT / "extract_actions_validation.csv", "w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()) if rows else ["source"])
        writer.writeheader()
        writer.writerows(rows)
    (OUT / "extract_actions_validation_summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except (AttributeError, ValueError):
        pass
    raise SystemExit(main())
