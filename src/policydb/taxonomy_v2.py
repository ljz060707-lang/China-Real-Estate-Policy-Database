from __future__ import annotations

import csv
import json
from datetime import UTC, datetime
import polars as pl
import yaml

from policydb.intensity.storage import atomic_write_parquet
from policydb.settings import Settings
from policydb.transform.normalization import stable_id

VERSION = "2.0.0"

INSTRUMENT_MAP = {
    "purchase_restriction": ("D", "D01", "regulation"),
    "sale_restriction": ("D", "D03", "regulation"),
    "mortgage_downpayment": ("D", "D04", "credit_finance"),
    "mortgage_rate": ("D", "D05", "credit_finance"),
    "provident_fund": ("D", "D06", "credit_finance"),
    "purchase_subsidy": ("D", "D08", "subsidy"),
    "talent_hukou": ("D", "D09", "administrative_service"),
    "housing_supply": ("S", "S06", "public_provision"),
    "urban_renewal": ("H", "H09", "public_spending"),
    "financing": ("F", "F03", "credit_finance"),
    "land": ("S", "S01", "land_planning"),
}

KEYWORD_RULES = [
    ("F", "F02", "credit_finance", ("白名单",)),
    ("F", "F06", "regulation", ("预售资金",)),
    ("F", "F07", "credit_finance", ("保交房", "保交楼", "停工项目")),
    ("F", "F08", "credit_finance", ("债务重组", "风险化解")),
    ("H", "H02", "public_provision", ("保障性租赁住房",)),
    ("H", "H03", "public_provision", ("配售型保障性住房",)),
    ("H", "H10", "public_spending", ("城中村改造",)),
    ("H", "H11", "public_spending", ("老旧小区改造",)),
    ("H", "H12", "public_spending", ("危旧房", "危房改造")),
    ("H", "H13", "public_spending", ("加装电梯",)),
    ("G", "G01", "regulation", ("限价", "价格备案", "参考价")),
    ("G", "G04", "regulation", ("中介监管", "房地产中介")),
    ("G", "G08", "regulation", ("物业管理",)),
    ("S", "S03", "land_planning", ("容积率", "规划用途")),
    ("S", "S08", "public_provision", ("收购存量住房", "商品房收储")),
    ("D", "D07", "tax", ("契税", "购房税费")),
    ("D", "D10", "subsidy", ("以旧换新", "房票")),
]


def load_taxonomy(settings: Settings | None = None) -> dict:
    settings = settings or Settings.discover()
    return yaml.safe_load(
        (settings.root / "data/reference/policy_taxonomy_v2.yaml").read_text(encoding="utf-8")
    )


def classify_action(instrument: str, text: str) -> tuple[str, str, str, float, str]:
    for primary, secondary, mechanism, keywords in KEYWORD_RULES:
        matched = next((word for word in keywords if word in text), None)
        if matched:
            return primary, secondary, mechanism, 0.95, f"keyword:{matched}"
    mapped = INSTRUMENT_MAP.get(instrument)
    if mapped:
        return *mapped, 0.90, f"instrument:{instrument}"
    return "", "", "", 0.0, "unmapped"


def materialize_action_classifications(settings: Settings | None = None) -> dict:
    settings = settings or Settings.discover()
    actions_path = settings.curated / "policy_actions.parquet"
    if not actions_path.exists():
        return {"actions": 0, "classified": 0, "coverage": 0.0}
    rows = []
    now = datetime.now(UTC).isoformat()
    for action in pl.read_parquet(actions_path).iter_rows(named=True):
        primary, secondary, mechanism, confidence, method = classify_action(
            str(action.get("instrument") or ""), str(action.get("clause_text") or "")
        )
        rows.append(
            {
                "classification_id": stable_id(
                    action["action_id"], VERSION, prefix="CLASS"
                ),
                "action_id": action["action_id"],
                "record_id": action["record_id"],
                "primary_category": primary or None,
                "secondary_category": secondary or None,
                "instrument_type": mechanism or None,
                "direction": action.get("direction") or "uncertain",
                "classification_source": "deterministic_rule",
                "confidence": confidence,
                "evidence_text": action.get("evidence_text") or action.get("clause_text"),
                "evidence_start": action.get("evidence_start"),
                "evidence_end": action.get("evidence_end"),
                "method_version": VERSION,
                "decision_reason": method,
                "review_status": "pending" if confidence < 0.9 else "auto_verified",
                "created_at": now,
                "updated_at": now,
            }
        )
    frame = pl.DataFrame(rows)
    atomic_write_parquet(frame, settings.curated / "policy_classifications.parquet")
    classified = frame.filter(pl.col("primary_category").is_not_null()).height
    return {
        "actions": frame.height,
        "classified": classified,
        "coverage": classified / frame.height if frame.height else 0.0,
    }


def _topic_rule(topic: str) -> tuple[str, str, str, bool]:
    candidates: list[tuple[str, str, str]] = []
    for primary, secondary, instrument, keywords in KEYWORD_RULES:
        if any(word in topic for word in keywords):
            candidates.append((primary, secondary, instrument))
    for legacy, mapped in (
        ("公积金", ("D", "D06", "credit_finance")),
        ("四限", ("D", "D01", "regulation")),
        ("限购", ("D", "D01", "regulation")),
        ("限售", ("D", "D03", "regulation")),
        ("首付", ("D", "D04", "credit_finance")),
        ("房贷利率", ("D", "D05", "credit_finance")),
        ("购房补贴", ("D", "D08", "subsidy")),
        ("人才", ("D", "D09", "administrative_service")),
        ("落户", ("D", "D09", "administrative_service")),
        ("供给", ("S", "S12", "public_provision")),
        ("土地", ("S", "S01", "land_planning")),
        ("融资", ("F", "F03", "credit_finance")),
        ("保障房", ("H", "H05", "public_provision")),
        ("城市更新", ("H", "H09", "public_spending")),
        ("旧改", ("H", "H11", "public_spending")),
        ("市场秩序", ("G", "G03", "regulation")),
        ("商品房销售", ("G", "G02", "regulation")),
        ("物业", ("G", "G08", "regulation")),
    ):
        if legacy in topic:
            candidates.append(mapped)
    candidates = list(dict.fromkeys(candidates))
    if len(candidates) == 1:
        return *candidates[0], False
    return "", "", "", True


def build_cicc_mapping(settings: Settings | None = None) -> dict:
    settings = settings or Settings.discover()
    records = pl.read_parquet(settings.curated / "records.parquet")
    counts = (
        records.filter(pl.col("legacy_category").is_not_null())
        .group_by(["source_sheet", "legacy_category"])
        .len()
        .sort("len", descending=True)
    )
    mappings = []
    for row in counts.iter_rows(named=True):
        topic = str(row["legacy_category"])
        primary, secondary, instrument, needs_review = _topic_rule(topic)
        mappings.append(
            {
                "source_sheet": row["source_sheet"],
                "source_topic": topic,
                "primary_category": primary or None,
                "secondary_category": secondary or None,
                "instrument_type": instrument or None,
                "mapping_confidence": 0.95 if not needs_review else 0.0,
                "mapping_version": VERSION,
                "needs_ai_review": needs_review,
                "record_count": row["len"],
            }
        )
    output = settings.root / "data/reference/cicc_topic_mapping.yaml"
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        yaml.safe_dump({"version": VERSION, "mappings": mappings}, allow_unicode=True, sort_keys=False),
        encoding="utf-8",
    )
    output_dir = settings.root / "outputs/taxonomy"
    output_dir.mkdir(parents=True, exist_ok=True)
    unresolved = [row for row in mappings if row["needs_ai_review"]]
    with (output_dir / "unmapped_topics.csv").open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(mappings[0]) if mappings else [])
        writer.writeheader()
        writer.writerows(unresolved)
    mapped_records = sum(row["record_count"] for row in mappings if not row["needs_ai_review"])
    total_records = sum(row["record_count"] for row in mappings)
    report = {
        "created_at": datetime.now(UTC).isoformat(),
        "distinct_topics": len(mappings),
        "mapped_topics": len(mappings) - len(unresolved),
        "record_weighted_mapping_rate": mapped_records / total_records if total_records else 0.0,
        "unmapped_topics": len(unresolved),
    }
    (output_dir / "cicc_topic_mapping_report.md").write_text(
        "# 中金 topic 映射报告\n\n```json\n"
        + json.dumps(report, ensure_ascii=False, indent=2)
        + "\n```\n\n复合或语义模糊 topic 保留原值并进入 AI 复核，不强制猜测。\n",
        encoding="utf-8",
    )
    return report
