from __future__ import annotations

import json
import re
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Literal

import duckdb
import polars as pl
from pydantic import BaseModel, Field

from policydb.classify.rules import classify
from policydb.query.database import build_database
from policydb.settings import Settings
from policydb.transform.normalization import normalize_title, stable_id

Diagnosis = Literal[
    "segmentation_error",
    "parser_error",
    "attachment_missing",
    "dynamic_page_missing",
    "source_content_missing",
    "true_information_missing",
]
AUTOMATION_STATUSES = {
    "pending_diagnosis",
    "auto_repaired_segmentation",
    "auto_reparsed",
    "auto_recovered_official",
    "auto_recovered_secondary",
    "auto_verified",
    "manual_review_required",
    "rejected",
}


class DiagnosisResult(BaseModel):
    diagnosis: Diagnosis
    confidence: float = Field(ge=0, le=1)
    evidence: list[str]
    repairable: bool


class IndependentVerification(BaseModel):
    field_evidence_valid: bool
    segmentation_complete: bool
    city_scope_supported: bool
    classification_supported: bool
    direction_supported: bool
    strength_supported: bool
    source_refetch_required: bool = False
    confidence: float = Field(ge=0, le=1)
    conflicts: list[str] = Field(default_factory=list)


def diagnose_document_problem(
    *,
    text: str | None,
    source_url: str | None = None,
    parsed: dict | None = None,
) -> DiagnosisResult:
    parsed = parsed or {}
    if parsed.get("parse_status") == "parser_error":
        return DiagnosisResult(
            diagnosis="parser_error",
            confidence=0.99,
            evidence=[str(parsed.get("parser_error") or "parser reported an error")],
            repairable=True,
        )
    if parsed.get("attachments") and not (text or parsed.get("full_text")):
        return DiagnosisResult(
            diagnosis="attachment_missing",
            confidence=0.95,
            evidence=["正文为空但页面存在附件链接或PDF内嵌附件"],
            repairable=True,
        )
    if parsed.get("dynamic_page_hint") and len(text or parsed.get("full_text") or "") < 80:
        return DiagnosisResult(
            diagnosis="dynamic_page_missing",
            confidence=0.92,
            evidence=["页面包含动态加载标记且静态DOM正文过短"],
            repairable=True,
        )
    repairs = parsed.get("repair_actions") or []
    if repairs:
        return DiagnosisResult(
            diagnosis="segmentation_error",
            confidence=min(0.99, 0.85 + len(repairs) * 0.02),
            evidence=[f"检测到{len(repairs)}处相邻块或跨页句子拼接"],
            repairable=True,
        )
    if not (text or "").strip() and source_url:
        return DiagnosisResult(
            diagnosis="source_content_missing",
            confidence=0.9,
            evidence=["当前记录没有正文，但保留了可回溯来源URL"],
            repairable=True,
        )
    if not (text or "").strip():
        return DiagnosisResult(
            diagnosis="true_information_missing",
            confidence=0.85,
            evidence=["本地无正文、无可解析快照且无来源URL"],
            repairable=False,
        )
    return DiagnosisResult(
        diagnosis="source_content_missing",
        confidence=0.72,
        evidence=["文本存在，但待审核字段缺少可直接引用的来源证据"],
        repairable=True,
    )


def deterministic_verdict(
    *,
    official_status: str,
    title_conflict: bool,
    city_conflict: bool,
    date_conflict: bool,
    completeness_score: float,
    rule_model_agreement: bool,
    first_confidence: float,
    second_review: IndependentVerification | None = None,
) -> tuple[str, float, list[str]]:
    conflicts = [
        name
        for name, value in {
            "title_conflict": title_conflict,
            "city_conflict": city_conflict,
            "date_conflict": date_conflict,
        }.items()
        if value
    ]
    official = official_status in {"official", "official_reprint"}
    base = (
        0.25 * float(official)
        + 0.25 * completeness_score
        + 0.25 * float(rule_model_agreement)
        + 0.25 * first_confidence
    )
    if conflicts:
        return "manual_review_required", min(base, 0.69), conflicts
    if official and completeness_score >= 0.85 and rule_model_agreement and base >= 0.9:
        return "auto_verified", round(base, 4), []
    if base >= 0.7 and second_review:
        supported = all(
            (
                second_review.field_evidence_valid,
                second_review.segmentation_complete,
                second_review.city_scope_supported,
                second_review.classification_supported,
                second_review.direction_supported,
                second_review.strength_supported,
            )
        )
        final = (base + second_review.confidence) / 2
        if supported and not second_review.conflicts and final >= 0.9:
            return "auto_verified", round(final, 4), []
        if final >= 0.7 and not second_review.conflicts:
            return "pending_diagnosis", round(final, 4), ["第二轮证据强度不足0.90"]
        return "manual_review_required", round(final, 4), second_review.conflicts
    return "manual_review_required", round(base, 4), ["综合置信度低于0.70"]


def ensure_automation_schema(settings: Settings) -> None:
    with duckdb.connect(str(settings.database)) as con:
        additions = {
            "diagnosis": "VARCHAR",
            "automation_status": "VARCHAR DEFAULT 'pending_diagnosis'",
            "diagnostic_confidence": "DOUBLE",
            "completeness_score": "DOUBLE",
            "diagnosis_evidence": "VARCHAR",
            "repair_action": "VARCHAR",
            "verification_round_1": "VARCHAR",
            "verification_round_2": "VARCHAR",
            "recovered_source_url": "VARCHAR",
        }
        existing = {row[0] for row in con.execute("DESCRIBE manual_review_tasks").fetchall()}
        for column, data_type in additions.items():
            if column not in existing:
                con.execute(f"ALTER TABLE manual_review_tasks ADD COLUMN {column} {data_type}")


def _cell_row(value: str | None) -> int | None:
    match = re.search(r"(?:ROW:|[A-Z]+)(\d+)", value or "")
    return int(match.group(1)) if match else None


def _t2_index(settings: Settings) -> dict[str, dict]:
    files = list((settings.root / "data" / "staging" / "excel").glob("*T2_城市房地产政策现状.parquet"))
    if not files:
        return {}
    return {row["source_cell"]: row for row in pl.read_parquet(files[0]).iter_rows(named=True)}


def _existing_t4_links(settings: Settings) -> dict[int, str]:
    path = settings.curated / "policy_features.parquet"
    if not path.exists():
        return {}
    frame = (
        pl.read_parquet(path)
        .filter(
            (pl.col("source_sheet") == "T4 2023年城市需求支持政策")
            & pl.col("record_id").is_not_null()
        )
        .with_columns(
            pl.col("source_cell").str.extract(r"(\d+)", 1).cast(pl.Int64).alias("source_row")
        )
        .drop_nulls("source_row")
        .unique("source_row", keep="first")
    )
    links = dict(zip(frame["source_row"].to_list(), frame["record_id"].to_list(), strict=True))
    overlay_path = settings.curated / "auto_t4_links.parquet"
    if overlay_path.exists():
        overlay = pl.read_parquet(overlay_path).unique("source_row", keep="last")
        links.update(
            zip(overlay["source_row"].to_list(), overlay["record_id"].to_list(), strict=True)
        )
    return links


@dataclass
class _Decision:
    diagnosis: str
    automation_status: str
    confidence: float
    evidence: list[str]
    action: str
    suggested_value: str | None = None


def _apply_curated_repairs(
    settings: Settings,
    tasks: list[dict],
    decisions: dict[str, _Decision],
) -> dict:
    changed = Counter()
    title_updates: dict[str, str] = {}
    t4_updates: dict[int, str] = {}
    topic_updates: dict[str, list[dict]] = {}
    records_path = settings.curated / "records.parquet"
    records_frame = pl.read_parquet(records_path) if records_path.exists() else pl.DataFrame()
    for task in tasks:
        decision = decisions[task["task_id"]]
        if (
            task["review_type"] == "missing_title"
            and decision.automation_status == "auto_reparsed"
            and task.get("record_id")
            and decision.suggested_value
        ):
            title_updates[task["record_id"]] = decision.suggested_value
        if (
            task.get("field_name", "").startswith("policy_features")
            and decision.automation_status == "auto_reparsed"
            and decision.suggested_value
        ):
            row = _cell_row(task.get("source_cell"))
            if row:
                t4_updates[row] = decision.suggested_value
        if (
            task["review_type"] == "low_confidence"
            and decision.automation_status == "auto_reparsed"
            and task.get("record_id")
        ):
            record = records_frame.filter(pl.col("record_id") == task["record_id"])
            if record.height:
                text = " ".join(
                    str(record[0, column] or "")
                    for column in ("title", "summary", "full_text")
                )
                topic_updates[task["record_id"]] = [
                    item for item in classify(text) if item["topic"] != "其他"
                ]

    if title_updates and records_path.exists():
        records = records_frame
        records = records.with_columns(
            pl.col("record_id")
            .replace_strict(title_updates, default=pl.col("title"))
            .alias("title"),
            pl.col("record_id")
            .replace_strict(
                {key: normalize_title(value) for key, value in title_updates.items()},
                default=pl.col("title_normalized"),
            )
            .alias("title_normalized"),
        )
        records.write_parquet(records_path, compression="zstd")
        changed["titles"] = len(title_updates)

    features_path = settings.curated / "policy_features.parquet"
    if t4_updates and features_path.exists():
        features = pl.read_parquet(features_path).with_columns(
            pl.col("source_cell").str.extract(r"(\d+)", 1).cast(pl.Int64).alias("_source_row")
        )
        overlay_rows = []
        now = datetime.now(UTC).isoformat()
        for row, record_id in t4_updates.items():
            cells = features.filter(
                (pl.col("source_sheet") == "T4 2023年城市需求支持政策")
                & (pl.col("_source_row") == row)
                & pl.col("record_id").is_null()
            )["source_cell"].to_list()
            changed["t4_features"] += len(cells)
            overlay_rows.extend(
                {
                    "source_cell": cell,
                    "source_row": row,
                    "record_id": record_id,
                    "classification_source": "deterministic_auto_match",
                    "confidence": 0.9,
                    "created_at": now,
                    "updated_at": now,
                }
                for cell in cells
            )
        overlay_path = settings.curated / "auto_t4_links.parquet"
        if overlay_rows:
            overlay = pl.DataFrame(overlay_rows)
            if overlay_path.exists():
                overlay = pl.concat(
                    [pl.read_parquet(overlay_path), overlay], how="diagonal_relaxed"
                )
            overlay.unique("source_cell", keep="last").write_parquet(
                overlay_path, compression="zstd"
            )
        changed["t4_rows"] = len(t4_updates)

    terms_path = settings.curated / "record_terms.parquet"
    if topic_updates and terms_path.exists():
        terms = pl.read_parquet(terms_path).with_columns(
            pl.when(
                pl.col("record_id").is_in(list(topic_updates))
                & (pl.col("taxonomy_name") == "topic")
                & (pl.col("term_name") == "其他")
            )
            .then(pl.lit("superseded_auto"))
            .otherwise(pl.col("review_status"))
            .alias("review_status")
        )
        rows = []
        for record_id, topics in topic_updates.items():
            for item in topics:
                rows.append(
                    {
                        "record_id": record_id,
                        "term_id": stable_id("topic", item["topic"], prefix="TERM"),
                        "taxonomy_name": "topic",
                        "term_name": item["topic"],
                        "classification_source": "rule",
                        "confidence": item["confidence"],
                        "evidence_excerpt": item["evidence_excerpt"],
                        "review_status": "auto_verified",
                    }
                )
        if rows:
            terms = pl.concat([terms, pl.DataFrame(rows)], how="diagonal_relaxed").unique(
                ["record_id", "term_id", "classification_source"], keep="last"
            )
            terms.write_parquet(terms_path, compression="zstd")
            changed["topic_relations"] = len(rows)

    if changed:
        log_path = settings.root / "data" / "logs" / "auto_review_history.csv"
        log_path.parent.mkdir(parents=True, exist_ok=True)
        header = not log_path.exists()
        with log_path.open("a", encoding="utf-8", newline="") as handle:
            if header:
                handle.write("operation_time,operation,count\n")
            now = datetime.now(UTC).isoformat()
            for operation, count in changed.items():
                handle.write(f"{now},{operation},{count}\n")
        build_database(settings)
    return dict(changed)


def automate_review_tasks(
    settings: Settings | None = None,
    *,
    apply_repairs: bool = True,
) -> dict:
    """Diagnose existing tasks without creating more tasks or touching Raw data."""
    settings = settings or Settings.discover()
    ensure_automation_schema(settings)
    t2 = _t2_index(settings)
    existing_t4_links = _existing_t4_links(settings)
    with duckdb.connect(str(settings.database), read_only=True) as con:
        columns = [row[0] for row in con.execute("DESCRIBE manual_review_tasks").fetchall()]
        tasks = [
            dict(zip(columns, row, strict=True))
            for row in con.execute(
                "SELECT * FROM manual_review_tasks WHERE status='pending'"
            ).fetchall()
        ]
        candidate_rows = con.execute(
            """SELECT source_row,candidate_record_id,match_score,evidence
               FROM t4_match_candidates WHERE review_status='pending'
               ORDER BY source_row,match_score DESC"""
        ).fetchall()
        records = {
            row[0]: row[1:]
            for row in con.execute(
                "SELECT record_id,title,summary,full_text,primary_source_url,official_status FROM records"
            ).fetchall()
        }
    candidates: dict[int, list[tuple]] = defaultdict(list)
    for row in candidate_rows:
        candidates[int(row[0])].append(row[1:])
    first_unmatched_by_row: set[int] = set()
    decisions: dict[str, _Decision] = {}
    for task in tasks:
        review_type = task["review_type"]
        field = task.get("field_name") or ""
        row_number = _cell_row(task.get("source_cell"))
        decision: _Decision
        if review_type == "unexplained_t2":
            cell = t2.get(task.get("source_cell") or "", {})
            value = str(cell.get("cell_value") or "").strip()
            field_name = str(cell.get("original_field_name") or "").strip()
            structural = bool(
                cell.get("is_merged")
                and (
                    not value
                    or value == field_name
                    or value == "目录"
                    or field_name.endswith("政策现状")
                )
            )
            decision = _Decision(
                "segmentation_error",
                "rejected" if structural else "auto_repaired_segmentation",
                0.98 if structural else 0.9,
                ["合并单元格的空占位/标题不属于独立政策事实"]
                if structural
                else ["保留原始单元格，由长表规则重组，不再逐格人工审核"],
                "exclude_layout_cell" if structural else "retain_staging_recompose_long_table",
            )
        elif review_type == "missing_source":
            record = records.get(task.get("record_id"), (None, None, None, None, "unknown"))
            diagnosed = diagnose_document_problem(text=record[2], source_url=record[3])
            decision = _Decision(
                diagnosed.diagnosis,
                "pending_diagnosis" if diagnosed.repairable else "manual_review_required",
                diagnosed.confidence,
                diagnosed.evidence,
                "official_source_recovery_queue" if diagnosed.repairable else "manual_evidence_required",
            )
        elif review_type == "missing_title":
            record = records.get(task.get("record_id"), (None, None, None, None, "unknown"))
            first_line = str(record[2] or "").splitlines()[0].strip()
            usable = 8 <= len(first_line) <= 100 and not first_line.endswith(("。", "；"))
            decision = _Decision(
                "segmentation_error",
                "auto_reparsed" if usable else "manual_review_required",
                0.93 if usable else 0.6,
                ["正文首行提供可引用标题证据"] if usable else ["正文中无稳定标题行"],
                "restore_title_from_first_line" if usable else "manual_title_required",
                first_line if usable else None,
            )
        elif review_type == "invalid_url":
            has_value = bool(str(task.get("old_value") or "").strip())
            decision = _Decision(
                "source_content_missing",
                "pending_diagnosis",
                0.92 if not has_value else 0.75,
                ["空链接并非格式错误，转入来源恢复队列"]
                if not has_value
                else ["链接格式异常，需先规范化再抓取"],
                "official_source_recovery_queue" if not has_value else "normalize_then_refetch",
            )
        elif review_type == "low_confidence":
            record = records.get(task.get("record_id"), (None, None, None, None, "unknown"))
            text = " ".join(str(value or "") for value in record[:3])
            topics = [item for item in classify(text) if item["topic"] != "其他"]
            decision = _Decision(
                "parser_error" if not topics else "segmentation_error",
                "auto_reparsed" if topics else "manual_review_required",
                0.91 if topics else 0.65,
                [f"规则证据支持：{item['topic']}={item['evidence_excerpt']}" for item in topics]
                or ["扩展规则仍无明确主题证据"],
                "rule_reclassify_then_glm_verify"
                if topics
                else "independent_classification_required",
                json.dumps([item["topic"] for item in topics], ensure_ascii=False) if topics else None,
            )
        elif review_type == "unmatched_t4" and row_number in existing_t4_links:
            decision = _Decision(
                "segmentation_error",
                "auto_verified",
                0.99,
                ["该T4来源行已由唯一高置信候选关联，重复字段任务自动合并"],
                "reuse_verified_t4_row_link",
                existing_t4_links[row_number],
            )
        elif review_type == "unmatched_t4" and row_number:
            row_candidates = candidates.get(row_number, [])
            best = row_candidates[0] if row_candidates else None
            runner_up = row_candidates[1][1] if len(row_candidates) > 1 else 0.0
            unique_high = bool(best and best[1] >= 90 and best[1] - runner_up >= 3)
            if field.startswith("policy_features"):
                if row_number in first_unmatched_by_row and not unique_high:
                    decision = _Decision(
                        "segmentation_error",
                        "rejected",
                        0.99,
                        ["同一T4行只保留一个候选匹配审核单元，避免按特征重复审核"],
                        "collapse_duplicate_feature_task",
                    )
                else:
                    first_unmatched_by_row.add(row_number)
                    decision = _Decision(
                        "segmentation_error",
                        "auto_reparsed" if unique_high else "manual_review_required" if best and best[1] < 70 else "pending_diagnosis",
                        (best[1] / 100) if best else 0.5,
                        [str(best[2])] if best else ["没有标题+城市+日期候选"],
                        "link_t4_row_to_record" if unique_high else "second_independent_match",
                        str(best[0]) if unique_high else None,
                    )
            else:
                decision = _Decision(
                    "segmentation_error",
                    "auto_verified" if unique_high else "manual_review_required" if best and best[1] < 70 else "pending_diagnosis",
                    (best[1] / 100) if best else 0.5,
                    [str(best[2])] if best else ["没有匹配候选"],
                    "verify_t4_candidate" if unique_high else "second_independent_match",
                    str(best[0]) if unique_high else None,
                )
        elif review_type == "other" and field == "policy_applicable_cities.city_id":
            decision = _Decision(
                "true_information_missing",
                "rejected",
                0.99,
                ["省级政策不应复制为逐城市事实；保留省级记录并从城市面板排除"],
                "reject_overinferred_city_relation",
            )
        elif review_type == "other" and field == "policy_sources.canonical_source":
            decision = _Decision(
                "source_content_missing",
                "auto_recovered_secondary",
                0.9,
                ["已有来源关系作为线索保留，但不提升为官方canonical source"],
                "retain_secondary_source_search_official",
            )
        else:
            decision = _Decision(
                "true_information_missing",
                "manual_review_required",
                float(task.get("confidence") or 0.5),
                ["确定性规则无法安全处理"],
                "manual_fallback",
            )
        decisions[task["task_id"]] = decision

    with duckdb.connect(str(settings.database)) as con:
        for task_id, decision in decisions.items():
            if decision.automation_status not in AUTOMATION_STATUSES:
                raise ValueError(decision.automation_status)
            con.execute(
                """UPDATE manual_review_tasks SET diagnosis=?,automation_status=?,
                   diagnostic_confidence=?,diagnosis_evidence=?,repair_action=?,
                   suggested_value=COALESCE(?,suggested_value),updated_at=current_timestamp
                   WHERE task_id=?""",
                [
                    decision.diagnosis,
                    decision.automation_status,
                    decision.confidence,
                    json.dumps(decision.evidence, ensure_ascii=False),
                    decision.action,
                    decision.suggested_value,
                    task_id,
                ],
            )
    repairs = _apply_curated_repairs(settings, tasks, decisions) if apply_repairs else {}
    status_counts = Counter(item.automation_status for item in decisions.values())
    diagnosis_counts = Counter(item.diagnosis for item in decisions.values())
    automatic_statuses = {
        "auto_repaired_segmentation",
        "auto_reparsed",
        "auto_recovered_official",
        "auto_recovered_secondary",
        "auto_verified",
        "rejected",
    }
    processed = sum(
        count for status, count in status_counts.items() if status in automatic_statuses
    )
    manual = status_counts.get("manual_review_required", 0)
    return {
        "task_count": len(decisions),
        "status": dict(sorted(status_counts.items())),
        "diagnosis": dict(sorted(diagnosis_counts.items())),
        "processed_count": processed,
        "automatic_processing_rate": round(processed / len(decisions), 4) if decisions else 1.0,
        "manual_review_count": manual,
        "manual_review_rate": round(manual / len(decisions), 4) if decisions else 0.0,
        "repairs": repairs,
    }
