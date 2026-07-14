from __future__ import annotations

import csv
import hashlib
import json
import re
import shutil
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urlsplit

import duckdb
import polars as pl

from policydb.query.database import build_database
from policydb.settings import Settings

TASK_STATUSES = {"pending", "approved", "corrected", "rejected", "ignored"}
REVIEW_TYPES = {
    "missing_title",
    "missing_source",
    "invalid_url",
    "low_confidence",
    "unmatched_t4",
    "unexplained_t2",
    "duplicate_record",
    "other",
}
CORRECTION_FIELDS = [
    "record_id",
    "field_name",
    "old_value",
    "new_value",
    "decision",
    "reviewer",
    "review_time",
    "evidence_url",
    "review_note",
]
HISTORY_FIELDS = ["task_id", "operation_time", "before_value", "after_value", "reviewer"]


def _now() -> datetime:
    return datetime.now(UTC)


def _text(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _task_id(
    review_type: str,
    record_id: str | None,
    field_name: str | None,
    source_sheet: str | None,
    source_cell: str | None,
) -> str:
    identity = "|".join(
        value or "" for value in (review_type, record_id, field_name, source_sheet, source_cell)
    )
    return f"REV_{hashlib.sha256(identity.encode('utf-8')).hexdigest()[:24].upper()}"


def ensure_review_schema(settings: Settings | None = None) -> Settings:
    settings = settings or Settings.discover()
    settings.database.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(settings.database)) as con:
        con.execute(
            """CREATE TABLE IF NOT EXISTS manual_review_tasks (
                task_id VARCHAR PRIMARY KEY,
                record_id VARCHAR,
                review_type VARCHAR NOT NULL CHECK (review_type IN (
                    'missing_title','missing_source','invalid_url','low_confidence',
                    'unmatched_t4','unexplained_t2','duplicate_record','other'
                )),
                field_name VARCHAR,
                source_sheet VARCHAR,
                source_cell VARCHAR,
                old_value VARCHAR,
                suggested_value VARCHAR,
                confidence DOUBLE,
                status VARCHAR NOT NULL DEFAULT 'pending' CHECK (status IN (
                    'pending','approved','corrected','rejected','ignored'
                )),
                review_note VARCHAR,
                evidence_url VARCHAR,
                created_at TIMESTAMP WITH TIME ZONE NOT NULL,
                updated_at TIMESTAMP WITH TIME ZONE NOT NULL
            )"""
        )
    _ensure_csv(settings.manual_corrections, CORRECTION_FIELDS)
    _ensure_csv(settings.review_history, HISTORY_FIELDS)
    return settings


def _ensure_csv(path: Path, fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and path.stat().st_size > 0:
        return
    with path.open("w", encoding="utf-8", newline="") as handle:
        csv.DictWriter(handle, fieldnames=fields).writeheader()


def _valid_url(value: object) -> bool:
    text = _text(value)
    if not text or " " in text:
        return False
    try:
        parts = urlsplit(text)
    except ValueError:
        return False
    return parts.scheme in {"http", "https"} and bool(parts.netloc)


def _make_task(
    review_type: str,
    *,
    record_id: str | None = None,
    field_name: str | None = None,
    source_sheet: str | None = None,
    source_cell: str | None = None,
    old_value: object = None,
    suggested_value: object = None,
    confidence: float | None = None,
    evidence_url: object = None,
) -> dict:
    now = _now()
    return {
        "task_id": _task_id(review_type, record_id, field_name, source_sheet, source_cell),
        "record_id": record_id,
        "review_type": review_type,
        "field_name": field_name,
        "source_sheet": source_sheet,
        "source_cell": source_cell,
        "old_value": _text(old_value),
        "suggested_value": _text(suggested_value),
        "confidence": confidence,
        "status": "pending",
        "review_note": None,
        "evidence_url": _text(evidence_url),
        "created_at": now,
        "updated_at": now,
    }


def generate_review_tasks(settings: Settings | None = None) -> dict:
    settings = ensure_review_schema(settings)
    tasks: list[dict] = []
    with duckdb.connect(str(settings.database)) as con:
        records = con.execute(
            """SELECT record_id,title,full_text,primary_source_url,source_sheet,source_row
               FROM records"""
        ).fetchall()
        for record_id, title, full_text, url, source_sheet, source_row in records:
            source_cell = f"ROW:{source_row}" if source_row is not None else None
            if not _text(title):
                tasks.append(
                    _make_task(
                        "missing_title",
                        record_id=record_id,
                        field_name="records.title",
                        source_sheet=source_sheet,
                        source_cell=source_cell,
                        old_value=title,
                        evidence_url=url,
                    )
                )
            if not _text(full_text):
                tasks.append(
                    _make_task(
                        "missing_source",
                        record_id=record_id,
                        field_name="records.full_text",
                        source_sheet=source_sheet,
                        source_cell=source_cell,
                        old_value=full_text,
                        evidence_url=url,
                    )
                )
            if not _valid_url(url):
                tasks.append(
                    _make_task(
                        "invalid_url",
                        record_id=record_id,
                        field_name="records.primary_source_url",
                        source_sheet=source_sheet,
                        source_cell=source_cell,
                        old_value=url,
                    )
                )

        low_confidence = con.execute(
            """SELECT rt.record_id,rt.term_id,rt.taxonomy_name,rt.term_name,rt.confidence,
                      rt.evidence_excerpt,r.source_sheet,r.primary_source_url
               FROM record_terms rt LEFT JOIN records r USING(record_id)
               WHERE rt.confidence < 0.65"""
        ).fetchall()
        for row in low_confidence:
            record_id, term_id, taxonomy, term_name, confidence, evidence, sheet, url = row
            tasks.append(
                _make_task(
                    "low_confidence",
                    record_id=record_id,
                    field_name=f"record_terms.term_name:{term_id}",
                    source_sheet=sheet,
                    source_cell=term_id,
                    old_value=term_name,
                    suggested_value=term_name,
                    confidence=confidence,
                    evidence_url=url or evidence,
                )
            )

        unmatched = con.execute(
            """SELECT feature_name,feature_value,source_sheet,source_cell
               FROM policy_features WHERE record_id IS NULL"""
        ).fetchall()
        for _feature_name, feature_value, sheet, cell in unmatched:
            tasks.append(
                _make_task(
                    "unmatched_t4",
                    field_name="policy_features.record_id",
                    source_sheet=sheet,
                    source_cell=cell,
                    old_value=feature_value,
                    suggested_value=None,
                )
            )

        duplicate_rows = con.execute(
            """SELECT record_id,content_hash,source_sheet,source_row,primary_source_url
               FROM records WHERE content_hash IN (
                   SELECT content_hash FROM records WHERE content_hash IS NOT NULL
                   GROUP BY content_hash HAVING count(*) > 1
               )"""
        ).fetchall()
        for record_id, content_hash, sheet, source_row, url in duplicate_rows:
            tasks.append(
                _make_task(
                    "duplicate_record",
                    record_id=record_id,
                    field_name="records.content_hash",
                    source_sheet=sheet,
                    source_cell=f"ROW:{source_row}" if source_row is not None else None,
                    old_value=content_hash,
                    evidence_url=url,
                )
            )

        mapped_cells = {
            value[0]
            for value in con.execute(
                "SELECT DISTINCT source_cell FROM city_policy_rules "
                "WHERE source_sheet='T2 城市房地产政策现状'"
            ).fetchall()
            if value[0]
        }
        available = {
            row[0]
            for row in con.execute(
                "SELECT table_name FROM information_schema.tables"
            ).fetchall()
        }
        if "policy_applicable_cities" in available:
            for relation_id, record_id, city_id, evidence, confidence in con.execute(
                """SELECT policy_applicable_city_id,record_id,city_id,evidence,confidence
                   FROM policy_applicable_cities WHERE needs_review"""
            ).fetchall():
                tasks.append(
                    _make_task(
                        "other",
                        record_id=record_id,
                        field_name="policy_applicable_cities.city_id",
                        source_sheet="105城市适用范围",
                        source_cell=relation_id,
                        old_value=city_id,
                        confidence=confidence,
                        evidence_url=evidence,
                    )
                )
        if "policy_sources" in available:
            for source_relation_id, record_id, url, status in con.execute(
                """SELECT policy_source_id,record_id,source_url,official_status
                   FROM policy_sources WHERE needs_review"""
            ).fetchall():
                tasks.append(
                    _make_task(
                        "other",
                        record_id=record_id,
                        field_name="policy_sources.canonical_source",
                        source_sheet="Excel来源注册表",
                        source_cell=source_relation_id,
                        old_value=status,
                        evidence_url=url,
                    )
                )
        if "llm_extractions" in available:
            for extraction_id, confidence, status in con.execute(
                """SELECT extraction_id,confidence,status FROM llm_extractions
                   WHERE needs_review"""
            ).fetchall():
                tasks.append(
                    _make_task(
                        "other",
                        field_name="llm_extractions.output_json",
                        source_sheet="GLM结构化提取",
                        source_cell=extraction_id,
                        old_value=status,
                        confidence=confidence,
                    )
                )
        if "t4_match_candidates" in available:
            for row_number, candidate_id, _title, score, evidence in con.execute(
                """SELECT source_row,candidate_record_id,policy_title_raw,match_score,evidence
                   FROM t4_match_candidates WHERE review_status='pending'"""
            ).fetchall():
                tasks.append(
                    _make_task(
                        "unmatched_t4",
                        field_name="t4_match_candidates.candidate_record_id",
                        source_sheet="T4 2023年城市需求支持政策",
                        source_cell=f"ROW:{row_number}:{candidate_id}",
                        old_value=candidate_id,
                        suggested_value=candidate_id,
                        confidence=(score or 0) / 100,
                        evidence_url=evidence,
                    )
                )
        existing_ids = {
            value[0] for value in con.execute("SELECT task_id FROM manual_review_tasks").fetchall()
        }

    t2_files = list(
        (settings.root / "data" / "staging" / "excel").glob(
            "*T2_城市房地产政策现状.parquet"
        )
    )
    if t2_files:
        staging = pl.read_parquet(t2_files[0]).select(
            "source_cell", "cell_value", "original_field_name", "source_sheet_name"
        )
        for item in staging.iter_rows(named=True):
            if item["source_cell"] in mapped_cells:
                continue
            tasks.append(
                _make_task(
                    "unexplained_t2",
                    field_name="city_policy_rules.rule_text",
                    source_sheet=item["source_sheet_name"],
                    source_cell=item["source_cell"],
                    old_value=item["cell_value"],
                    suggested_value=item["original_field_name"],
                )
            )

    discovered = Counter(task["review_type"] for task in tasks)
    new_tasks = [task for task in tasks if task["task_id"] not in existing_ids]
    if new_tasks:
        columns = list(new_tasks[0])
        placeholders = ",".join("?" for _ in columns)
        with duckdb.connect(str(settings.database)) as con:
            con.executemany(
                f"INSERT OR IGNORE INTO manual_review_tasks ({','.join(columns)}) "
                f"VALUES ({placeholders})",
                [[task[column] for column in columns] for task in new_tasks],
            )
    created = Counter(task["review_type"] for task in new_tasks)
    return {
        "discovered": dict(sorted(discovered.items())),
        "created": dict(sorted(created.items())),
        "discovered_total": len(tasks),
        "created_total": len(new_tasks),
    }


def review_stats(settings: Settings | None = None) -> dict:
    settings = ensure_review_schema(settings)
    with duckdb.connect(str(settings.database), read_only=True) as con:
        status_rows = con.execute(
            "SELECT status,count(*) FROM manual_review_tasks GROUP BY status"
        ).fetchall()
        type_rows = con.execute(
            "SELECT review_type,count(*) FROM manual_review_tasks GROUP BY review_type"
        ).fetchall()
    statuses = {status: count for status, count in status_rows}
    return {
        "pending": statuses.get("pending", 0),
        "completed": sum(count for status, count in status_rows if status != "pending"),
        "status": statuses,
        "review_type": {review_type: count for review_type, count in type_rows},
    }


def list_review_tasks(
    settings: Settings | None = None,
    *,
    review_type: str | None = None,
    status: str | None = "pending",
    limit: int = 1000,
    offset: int = 0,
) -> pl.DataFrame:
    settings = ensure_review_schema(settings)
    clauses, params = ["1=1"], []
    if review_type and review_type != "all":
        clauses.append("t.review_type=?")
        params.append(review_type)
    if status == "completed":
        clauses.append("t.status<>'pending'")
    elif status and status != "all":
        clauses.append("t.status=?")
        params.append(status)
    sql = f"""SELECT t.*,r.title,r.record_date,r.summary,r.primary_source_url
              FROM manual_review_tasks t LEFT JOIN records r ON t.record_id=r.record_id
              WHERE {' AND '.join(clauses)}
              ORDER BY CASE WHEN t.status='pending' THEN 0 ELSE 1 END,
                       t.review_type,t.created_at,t.task_id
              LIMIT {max(1, int(limit))} OFFSET {max(0, int(offset))}"""
    with duckdb.connect(str(settings.database), read_only=True) as con:
        return con.execute(sql, params).pl()


def review_task_count(
    settings: Settings | None = None,
    *,
    review_type: str | None = None,
    status: str | None = "pending",
) -> int:
    settings = ensure_review_schema(settings)
    clauses, params = ["1=1"], []
    if review_type and review_type != "all":
        clauses.append("review_type=?")
        params.append(review_type)
    if status == "completed":
        clauses.append("status<>'pending'")
    elif status and status != "all":
        clauses.append("status=?")
        params.append(status)
    with duckdb.connect(str(settings.database), read_only=True) as con:
        return int(
            con.execute(
                f"SELECT count(*) FROM manual_review_tasks WHERE {' AND '.join(clauses)}",
                params,
            ).fetchone()[0]
        )


def save_review_decision(
    task_id: str,
    decision: str,
    *,
    new_value: str | None = None,
    reviewer: str = "User",
    review_note: str | None = None,
    evidence_url: str | None = None,
    settings: Settings | None = None,
) -> dict:
    settings = ensure_review_schema(settings)
    if decision not in TASK_STATUSES - {"pending"}:
        raise ValueError(f"Unsupported review decision: {decision}")
    if decision == "corrected" and new_value is None:
        raise ValueError("new_value is required for corrected decisions")
    now = _now()
    with duckdb.connect(str(settings.database)) as con:
        task = con.execute(
            "SELECT * FROM manual_review_tasks WHERE task_id=?", [task_id]
        ).fetchone()
        if task is None:
            raise KeyError(task_id)
        columns = [item[0] for item in con.description]
        task_row = dict(zip(columns, task, strict=True))
        effective_value = new_value if decision == "corrected" else task_row["old_value"]
        effective_evidence = evidence_url or task_row["evidence_url"]
        con.execute(
            """UPDATE manual_review_tasks
               SET status=?,suggested_value=?,review_note=?,evidence_url=?,updated_at=?
               WHERE task_id=?""",
            [
                decision,
                effective_value,
                review_note,
                effective_evidence,
                now,
                task_id,
            ],
        )

    correction_field = task_row["field_name"] or "other"
    if not task_row["record_id"] and task_row["source_cell"]:
        correction_field = (
            f"{correction_field}@{task_row['source_sheet']}!{task_row['source_cell']}"
        )
    _append_csv(
        settings.manual_corrections,
        CORRECTION_FIELDS,
        {
            "record_id": task_row["record_id"] or "",
            "field_name": correction_field,
            "old_value": task_row["old_value"] or "",
            "new_value": effective_value or "",
            "decision": decision,
            "reviewer": reviewer,
            "review_time": now.isoformat(),
            "evidence_url": effective_evidence or "",
            "review_note": review_note or "",
        },
    )
    _append_csv(
        settings.review_history,
        HISTORY_FIELDS,
        {
            "task_id": task_id,
            "operation_time": now.isoformat(),
            "before_value": json.dumps(
                {"status": task_row["status"], "value": task_row["old_value"]},
                ensure_ascii=False,
            ),
            "after_value": json.dumps(
                {"status": decision, "value": effective_value}, ensure_ascii=False
            ),
            "reviewer": reviewer,
        },
    )
    return {"task_id": task_id, "status": decision, "new_value": effective_value}


def _append_csv(path: Path, fields: list[str], row: dict) -> None:
    _ensure_csv(path, fields)
    with path.open("a", encoding="utf-8", newline="") as handle:
        csv.DictWriter(handle, fieldnames=fields).writerow(row)


def _write_parquet_atomic(frame: pl.DataFrame, path: Path) -> None:
    temporary = path.with_suffix(f"{path.suffix}.tmp")
    frame.write_parquet(temporary, compression="zstd")
    temporary.replace(path)


def _snapshot(paths: list[Path], settings: Settings) -> Path:
    stamp = _now().strftime("%Y%m%dT%H%M%SZ")
    destination = settings.curated / "history" / stamp
    destination.mkdir(parents=True, exist_ok=True)
    for path in paths:
        if path.exists():
            shutil.copy2(path, destination / path.name)
    return destination


def apply_corrections(settings: Settings | None = None) -> dict:
    settings = ensure_review_schema(settings)
    with settings.manual_corrections.open(encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    latest: dict[tuple[str, str], dict] = {}
    for row in rows:
        latest[(row["record_id"], row["field_name"])] = row

    records_path = settings.curated / "records.parquet"
    terms_path = settings.curated / "record_terms.parquet"
    features_path = settings.curated / "policy_features.parquet"
    rules_path = settings.curated / "city_policy_rules.parquet"
    t4_candidates_path = settings.curated / "t4_match_candidates.parquet"
    records = pl.read_parquet(records_path)
    terms = pl.read_parquet(terms_path)
    features = pl.read_parquet(features_path)
    rules = pl.read_parquet(rules_path)
    t4_candidates = (
        pl.read_parquet(t4_candidates_path) if t4_candidates_path.exists() else None
    )
    changed = Counter()

    for correction in latest.values():
        decision = correction["decision"]
        if decision not in {"approved", "corrected"}:
            continue
        record_id = correction["record_id"] or None
        full_field = correction["field_name"]
        base_field, _, locator = full_field.partition("@")
        new_value = correction["new_value"]

        record_columns = {
            "records.title": "title",
            "records.full_text": "full_text",
            "records.primary_source_url": "primary_source_url",
        }
        if decision == "corrected" and record_id and base_field in record_columns:
            column = record_columns[base_field]
            current = records.filter(pl.col("record_id") == record_id).select(column)
            if current.height and current.item() != new_value:
                records = records.with_columns(
                    pl.when(pl.col("record_id") == record_id)
                    .then(pl.lit(new_value))
                    .otherwise(pl.col(column))
                    .alias(column)
                )
                changed["records"] += 1
        elif base_field.startswith("record_terms.term_name:") and record_id:
            term_id = base_field.split(":", 1)[1]
            matches = (pl.col("record_id") == record_id) & (pl.col("term_id") == term_id)
            matching_terms = terms.filter(matches)
            replacement = new_value if decision == "corrected" else None
            term_value_matches = (
                pl.lit(True) if replacement is None else pl.col("term_name") == replacement
            )
            already_applied = bool(
                matching_terms.height
                and matching_terms.select(
                    (
                        (pl.col("classification_source") == "manual")
                        & (pl.col("confidence") == 1.0)
                        & (pl.col("review_status") == decision)
                        & term_value_matches
                    ).all()
                ).item()
            )
            if matching_terms.height and not already_applied:
                terms = terms.with_columns(
                    pl.when(matches & pl.lit(replacement is not None))
                    .then(pl.lit(replacement))
                    .otherwise(pl.col("term_name"))
                    .alias("term_name"),
                    pl.when(matches)
                    .then(pl.lit("manual"))
                    .otherwise(pl.col("classification_source"))
                    .alias("classification_source"),
                    pl.when(matches)
                    .then(pl.lit(1.0))
                    .otherwise(pl.col("confidence"))
                    .alias("confidence"),
                    pl.when(matches)
                    .then(pl.lit(decision))
                    .otherwise(pl.col("review_status"))
                    .alias("review_status"),
                )
                changed["record_terms"] += 1
        elif decision == "corrected" and base_field == "policy_features.record_id" and locator:
            sheet, _, cell = locator.rpartition("!")
            matches = (pl.col("source_sheet") == sheet) & (pl.col("source_cell") == cell)
            current = features.filter(matches).select("record_id")
            if current.height and current.item() != new_value:
                features = features.with_columns(
                    pl.when(matches)
                    .then(pl.lit(new_value))
                    .otherwise(pl.col("record_id"))
                    .alias("record_id")
                )
                changed["policy_features"] += 1
        elif decision == "corrected" and base_field == "city_policy_rules.rule_text" and locator:
            sheet, _, cell = locator.rpartition("!")
            matches = (pl.col("source_sheet") == sheet) & (pl.col("source_cell") == cell)
            if rules.filter(matches).height:
                current = rules.filter(matches).select("rule_text").item()
                if current != new_value:
                    rules = rules.with_columns(
                        pl.when(matches)
                        .then(pl.lit(new_value))
                        .otherwise(pl.col("rule_text"))
                        .alias("rule_text")
                    )
                    changed["city_policy_rules"] += 1
            else:
                addition = pl.DataFrame(
                    [
                        {
                            "jurisdiction_id": None,
                            "policy_dimension": "manual_review",
                            "population_group": None,
                            "housing_count": None,
                            "loan_status": None,
                            "rule_text": new_value,
                            "effective_date": None,
                            "source_cell": cell,
                            "source_sheet": sheet,
                        }
                    ],
                    schema=rules.schema,
                )
                rules = pl.concat([rules, addition])
                changed["city_policy_rules"] += 1
        elif (
            decision in {"approved", "corrected"}
            and base_field == "t4_match_candidates.candidate_record_id"
            and locator
            and t4_candidates is not None
        ):
            row_match = re.search(r"ROW:(\d+)", locator)
            if row_match:
                source_row = int(row_match.group(1))
                candidate_id = new_value
                feature_matches = (
                    pl.col("source_sheet") == "T4 2023年城市需求支持政策"
                ) & (
                    pl.col("source_cell").str.extract(r"(\d+)", 1).cast(pl.Int64)
                    == source_row
                )
                if features.filter(feature_matches).height:
                    features = features.with_columns(
                        pl.when(feature_matches)
                        .then(pl.lit(candidate_id))
                        .otherwise(pl.col("record_id"))
                        .alias("record_id")
                    )
                    changed["policy_features"] += 1
                candidate_matches = (
                    (pl.col("source_row") == source_row)
                    & (pl.col("candidate_record_id") == candidate_id)
                )
                t4_candidates = t4_candidates.with_columns(
                    pl.when(candidate_matches)
                    .then(pl.lit(decision))
                    .otherwise(pl.col("review_status"))
                    .alias("review_status")
                )
                changed["t4_match_candidates"] += 1

    with duckdb.connect(str(settings.database), read_only=True) as con:
        reviewed_ids = {
            row[0]
            for row in con.execute(
                """SELECT record_id FROM manual_review_tasks WHERE record_id IS NOT NULL
                   GROUP BY record_id HAVING count(*) FILTER(WHERE status='pending')=0"""
            ).fetchall()
        }
    if reviewed_ids:
        needs_update = records.filter(
            pl.col("record_id").is_in(reviewed_ids)
            & (pl.col("manual_review_status") != "reviewed")
        ).height
        if needs_update:
            records = records.with_columns(
                pl.when(pl.col("record_id").is_in(reviewed_ids))
                .then(pl.lit("reviewed"))
                .otherwise(pl.col("manual_review_status"))
                .alias("manual_review_status")
            )
            changed["records"] += needs_update

    frames = {
        "records": (records_path, records),
        "record_terms": (terms_path, terms),
        "policy_features": (features_path, features),
        "city_policy_rules": (rules_path, rules),
        "t4_match_candidates": (t4_candidates_path, t4_candidates),
    }
    affected = [frames[name][0] for name in changed if name in frames]
    history_path = None
    if affected:
        history_path = _snapshot(affected, settings)
        for name in changed:
            if name in frames:
                path, frame = frames[name]
                _write_parquet_atomic(frame, path)
        build_database(settings)
    return {
        "applied": dict(changed),
        "applied_total": sum(changed.values()),
        "history_path": str(history_path) if history_path else None,
        "database_rebuilt": bool(affected),
    }
