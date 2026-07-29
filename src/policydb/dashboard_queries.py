"""Small, parameterised DuckDB queries shared by the Streamlit dashboard."""

from __future__ import annotations

from datetime import date

import polars as pl

PRIMARY_LABELS = {
    "D": "需求侧政策",
    "S": "供给侧政策",
    "F": "房地产金融与风险",
    "H": "住房保障与城市更新",
    "G": "市场监管与制度治理",
}


def _where(filters: dict) -> tuple[str, list[object]]:
    clauses = ["record_date >= ?"]
    params: list[object] = [filters.get("start_date") or date(2018, 1, 1)]
    for field in ("primary_category_code", "secondary_category_code", "province", "district"):
        if value := filters.get(field):
            clauses.append(f"{field}=?")
            params.append(value)
    if cities := filters.get("cities"):
        clauses.append("city IN (" + ",".join("?" for _ in cities) + ")")
        params.extend(cities)
    if end_date := filters.get("end_date"):
        clauses.append("record_date<=?")
        params.append(end_date)
    if keyword := filters.get("keyword"):
        clauses.append("(title ILIKE ? OR clause_text ILIKE ? OR evidence_excerpt ILIKE ?)")
        params.extend([f"%{keyword}%"] * 3)
    for field in ("direction", "instrument_type", "original_issuer", "target_actor", "official_status", "review_status"):
        if value := filters.get(field):
            clauses.append(f"{field}=?")
            params.append(value)
    if filters.get("has_pdf") is not None:
        clauses.append("has_pdf=?")
        params.append(filters["has_pdf"])
    if filters.get("full_text") is not None:
        clauses.append("text_completeness<>?" if filters["full_text"] else "text_completeness=?")
        params.append("missing_text")
    if minimum := filters.get("minimum_intensity"):
        clauses.append("COALESCE(policy_intensity,0)>=?")
        params.append(minimum)
    return " AND ".join(clauses), params


def filter_options(db) -> dict[str, list[str]]:
    fields = {
        "provinces": "province",
        "directions": "direction",
        "instruments": "instrument_type",
        "issuers": "original_issuer",
        "targets": "target_actor",
        "statuses": "official_status",
        "reviews": "review_status",
    }
    options = {}
    for key, field in fields.items():
        options[key] = db._query(
            f"SELECT DISTINCT {field} AS option_value FROM v_policy_action_center "
            f"WHERE {field} IS NOT NULL ORDER BY 1"
        )["option_value"].to_list()
    options["primary"] = list(PRIMARY_LABELS)
    options["secondary"] = db._query(
        "SELECT DISTINCT primary_category_code,secondary_category_code "
        "FROM v_policy_action_center WHERE secondary_category_code IS NOT NULL "
        "ORDER BY 1,2"
    ).to_dicts()
    return options


def cities_for_province(db, province: str | None) -> list[str]:
    sql = "SELECT DISTINCT city FROM v_policy_action_center WHERE city IS NOT NULL"
    params: list[object] = []
    if province:
        sql += " AND province=?"
        params.append(province)
    return db._query(sql + " ORDER BY 1", params)["city"].to_list()


def districts_for_cities(db, cities: list[str]) -> list[str]:
    if not cities:
        return []
    sql = "SELECT DISTINCT district FROM v_policy_action_center WHERE district IS NOT NULL AND city IN ("
    sql += ",".join("?" for _ in cities) + ") ORDER BY 1"
    return db._query(sql, cities)["district"].to_list()


def policy_metrics(db, filters: dict) -> dict:
    where, params = _where(filters)
    return db._query(
        "SELECT count(DISTINCT record_id) policy_count,count(*) action_count,"
        "count(DISTINCT city) city_count,"
        "avg(CASE WHEN has_pdf THEN 1.0 ELSE 0.0 END) pdf_share,"
        "count(DISTINCT record_id) FILTER(WHERE record_date>=current_date-INTERVAL 30 DAY) recent_count,"
        "avg(CASE WHEN official_status IN ('official','official_reprint') THEN 1.0 ELSE 0.0 END) official_share,"
        "count(*) FILTER(WHERE review_status NOT IN ('auto_verified','approved')) review_count "
        "FROM v_policy_action_center WHERE " + where,
        params,
    ).row(0, named=True)


def policy_trend(db, filters: dict, grain: str = "month") -> pl.DataFrame:
    period = {"month": "month", "quarter": "quarter", "year": "year"}.get(grain, "month")
    where, params = _where(filters)
    return db._query(
        f"SELECT date_trunc('{period}',record_date)::DATE period,count(*) action_count "
        "FROM v_policy_action_center WHERE " + where + " GROUP BY 1 ORDER BY 1",
        params,
    )


def policy_distribution(db, filters: dict) -> pl.DataFrame:
    where, params = _where(filters)
    return db._query(
        "SELECT primary_category_code,count(*) action_count FROM v_policy_action_center WHERE "
        + where
        + " AND primary_category_code IS NOT NULL GROUP BY 1 ORDER BY 1",
        params,
    )


def policy_direction_distribution(db, filters: dict) -> pl.DataFrame:
    where, params = _where(filters)
    return db._query(
        "SELECT direction,count(*) action_count FROM v_policy_action_center WHERE "
        + where
        + " AND direction IS NOT NULL GROUP BY 1 ORDER BY 2 DESC",
        params,
    )


def policy_city_ranking(db, filters: dict) -> pl.DataFrame:
    where, params = _where(filters)
    return db._query(
        "SELECT city,count(DISTINCT record_id) policy_count FROM v_policy_action_center WHERE "
        + where
        + " AND city IS NOT NULL GROUP BY 1 ORDER BY 2 DESC LIMIT 30",
        params,
    )


def policy_list(db, filters: dict, *, page: int, page_size: int, sort_by: str) -> tuple[pl.DataFrame, int]:
    where, params = _where(filters)
    order = {
        "发布日期": "record_date DESC NULLS LAST",
        "政策强度": "policy_intensity DESC NULLS LAST,record_date DESC NULLS LAST",
    }.get(sort_by, "record_date DESC NULLS LAST")
    total = db._query(
        "SELECT count(DISTINCT record_id) FROM v_policy_action_center WHERE " + where,
        params,
    ).item()
    frame = db._query(
        "SELECT record_id,max(record_date) record_date,max(title) title,max(province) province,"
        "max(city) city,min(primary_category_code) primary_category_code,"
        "min(secondary_category_code) secondary_category_code,min(direction) direction,"
        "max(policy_intensity) policy_intensity,bool_or(has_pdf) has_pdf "
        "FROM v_policy_action_center WHERE " + where + " GROUP BY record_id "
        f"ORDER BY {order} LIMIT ? OFFSET ?",
        [*params, page_size, (page - 1) * page_size],
    )
    return frame, int(total or 0)


def policy_detail(db, record_id: str) -> tuple[dict | None, pl.DataFrame, pl.DataFrame]:
    actions = db._query(
        "SELECT action_id,clause_text,primary_category_code,secondary_category_code,"
        "direction,target_actor,policy_intensity,evidence_excerpt,confidence,review_status,"
        "original_issuer,publication_issuer,applicable_jurisdiction,duplicate_cluster_id,"
        "version_status,primary_source_url,archive_relative_path "
        "FROM v_policy_action_center WHERE record_id=? ORDER BY action_id",
        [record_id],
    )
    files = db._query(
        "SELECT archive_relative_path,content_type,size_bytes,archive_status,sha256_actual "
        "FROM policy_files WHERE record_id=? ORDER BY content_type",
        [record_id],
    )
    return db.get(record_id), actions, files


def export_policy_list(db, filters: dict) -> pl.DataFrame:
    return policy_list(db, filters, page=1, page_size=10_000, sort_by="发布日期")[0]
