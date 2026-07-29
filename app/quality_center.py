from __future__ import annotations

import streamlit as st

from app.ui import safe_dataframe, safe_pandas


def _query_or_empty(db, sql: str):
    try:
        return db._query(sql)
    except Exception:
        return db._query("SELECT NULL::VARCHAR AS message WHERE false")


def render_quality_center(db) -> None:
    quality = db._query("SELECT * FROM v_data_quality").row(0, named=True)
    review = _query_or_empty(
        db,
        "SELECT count(*) FILTER(WHERE status='pending') manual_count,"
        "count(*) FILTER(WHERE status IN ('approved','corrected','rejected','ignored')) resolved_count "
        "FROM manual_review_tasks",
    )
    review_row = review.row(0, named=True) if review.height else {"manual_count": 0, "resolved_count": 0}
    severe = int(quality["missing_full_text_count"] + quality["missing_url_count"])
    for column, (label, value) in zip(
        st.columns(4),
        [
            ("严重问题", severe),
            ("可自动修复", quality["pending_review_count"]),
            ("需人工托底", review_row["manual_count"] or 0),
            ("已解决", review_row["resolved_count"] or 0),
        ],
        strict=True,
    ):
        column.metric(label, value)
    tabs = st.tabs(["正文与PDF", "重复与版本", "分类与方向", "地区与主体", "时间与有效性", "覆盖完整性"])
    with tabs[0]:
        frame = db._query(
            "SELECT record_id,record_date,title,text_completeness,has_pdf,archive_relative_path "
            "FROM v_policy_action_center WHERE text_completeness='missing_text' OR NOT has_pdf "
            "ORDER BY record_date DESC NULLS LAST LIMIT 500"
        )
        st.caption("正文和附件缺失不会自动补造；可由来源恢复任务处理。")
        st.dataframe(safe_pandas(frame), hide_index=True, width="stretch", height=420)
    with tabs[1]:
        audit = _query_or_empty(db, "SELECT * FROM v_dedup_audit ORDER BY dedup_level,decision")
        if audit.is_empty():
            st.info("尚无可展示的去重审计记录。")
        else:
            st.dataframe(safe_pandas(audit), hide_index=True, width="stretch", height=420)
    with tabs[2]:
        frame = _query_or_empty(
            db,
            "SELECT primary_category_code,secondary_category_code,direction,review_status,count(*) action_count "
            "FROM v_policy_action_center GROUP BY ALL ORDER BY action_count DESC LIMIT 500",
        )
        st.dataframe(safe_pandas(frame), hide_index=True, width="stretch", height=420)
    with tabs[3]:
        frame = db._query(
            "SELECT record_id,record_date,title,province,city,original_issuer,applicable_jurisdiction "
            "FROM v_policy_action_center WHERE province IS NULL OR city IS NULL OR original_issuer IS NULL "
            "ORDER BY record_date DESC NULLS LAST LIMIT 500"
        )
        st.dataframe(safe_pandas(frame), hide_index=True, width="stretch", height=420)
    with tabs[4]:
        frame = db._query(
            "SELECT record_id,record_date,NULL::DATE AS effective_date,title,manual_review_status AS version_status "
            "FROM records WHERE record_date IS NULL "
            "ORDER BY title LIMIT 500"
        )
        st.dataframe(safe_pandas(frame), hide_index=True, width="stretch", height=420)
    with tabs[5]:
        coverage = _query_or_empty(
            db,
            "SELECT coverage_status,count(*) month_count,count(DISTINCT city_id) city_count "
            "FROM v_city_month_coverage GROUP BY 1 ORDER BY month_count DESC",
        )
        if coverage.is_empty():
            st.warning("覆盖矩阵尚未就绪；未扫描、部分扫描和失败不会显示为零政策。")
        else:
            safe_dataframe(coverage)
            st.caption("覆盖状态只描述来源扫描证据，不替代政策数量。")
