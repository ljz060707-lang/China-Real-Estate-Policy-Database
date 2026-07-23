from __future__ import annotations

import plotly.express as px
import streamlit as st

from app.theme import style_plotly_figure
from app.ui import safe_dataframe, safe_pandas


def _options(db, field: str) -> list[str]:
    return db._query(
        f"SELECT DISTINCT {field} FROM v_policy_action_center "
        f"WHERE {field} IS NOT NULL ORDER BY 1"
    )[field].to_list()


def render_policy_center(db) -> None:
    filters = st.columns([1, 1.3, 1, 1, 1.2])
    primary = filters[0].selectbox("一级分类", ["全部", *_options(db, "primary_category")])
    secondary = filters[1].selectbox("二级分类", ["全部", *_options(db, "secondary_category")])
    instrument = filters[2].selectbox("政策工具", ["全部", *_options(db, "instrument_type")])
    direction = filters[3].selectbox("方向", ["全部", *_options(db, "direction")])
    official_only = filters[4].checkbox("仅官方/官方转载")
    detail_filters = st.columns([1.5, 1, 1, 1, 1])
    keyword = detail_filters[0].text_input("标题或正文关键词")
    start_date = detail_filters[1].date_input("开始日期", value=None)
    end_date = detail_filters[2].date_input("结束日期", value=None)
    archived = detail_filters[3].selectbox("原文归档", ["全部", "已归档", "未归档"])
    review = detail_filters[4].selectbox("审核状态", ["全部", *_options(db, "review_status")])
    clauses = ["1=1"]
    params: list[object] = []
    for field, value in (
        ("primary_category", primary),
        ("secondary_category", secondary),
        ("instrument_type", instrument),
        ("direction", direction),
        ("review_status", review),
    ):
        if value != "全部":
            clauses.append(f"{field}=?")
            params.append(value)
    if official_only:
        clauses.append("official_status IN ('official','official_reprint')")
    if keyword:
        clauses.append("(title ILIKE ? OR clause_text ILIKE ?)")
        params.extend([f"%{keyword}%", f"%{keyword}%"])
    if start_date:
        clauses.append("record_date>=?")
        params.append(start_date)
    if end_date:
        clauses.append("record_date<=?")
        params.append(end_date)
    if archived != "全部":
        clauses.append("has_archived_file=?")
        params.append(archived == "已归档")
    where = " AND ".join(clauses)
    metrics = db._query(
        "SELECT count(DISTINCT record_id) policies,count(*) actions,"
        "avg(confidence) confidence,avg(CASE WHEN has_archived_file THEN 1.0 ELSE 0.0 END) archive_share,"
        "count(*) FILTER(WHERE review_status NOT IN ('auto_verified','approved')) review_count "
        "FROM v_policy_action_center WHERE " + where,
        params,
    ).row(0, named=True)
    for column, (label, value) in zip(
        st.columns(5),
        [
            ("政策文件", metrics["policies"]),
            ("政策动作", metrics["actions"]),
            ("平均分类置信度", f"{float(metrics['confidence'] or 0):.1%}"),
            ("动作关联归档率", f"{float(metrics['archive_share'] or 0):.1%}"),
            ("待复核动作", metrics["review_count"]),
        ],
        strict=True,
    ):
        column.metric(label, value)
    distribution = db._query(
        "SELECT primary_category,count(*) action_count FROM v_policy_action_center WHERE "
        + where
        + " GROUP BY 1 ORDER BY 2 DESC",
        params,
    )
    trend = db._query(
        'SELECT year(record_date) AS "year",month(record_date) AS "month",count(*) action_count '
        "FROM v_policy_action_center WHERE "
        + where
        + " AND record_date IS NOT NULL GROUP BY 1,2 ORDER BY 1,2",
        params,
    )
    left, right = st.columns(2)
    with left:
        figure = px.bar(
            safe_pandas(distribution),
            x="primary_category",
            y="action_count",
            title="一级政策板块分布",
            color_discrete_sequence=["#82318E"],
        )
        st.plotly_chart(style_plotly_figure(figure), width="stretch")
    with right:
        trend_pd = safe_pandas(trend)
        if trend_pd.empty:
            st.info("当前筛选条件没有时间序列数据。")
        else:
            trend_pd["period"] = (
                trend_pd["year"].astype(str)
                + "-"
                + trend_pd["month"].astype(str).str.zfill(2)
            )
            figure = px.line(
                trend_pd,
                x="period",
                y="action_count",
                title="政策动作月度趋势",
                color_discrete_sequence=["#5B2C83"],
            )
            st.plotly_chart(style_plotly_figure(figure), width="stretch")
    page = int(st.number_input("结果页码", min_value=1, value=1, step=1))
    rows = db._query(
        "SELECT action_id,record_id,record_date,title,primary_category,secondary_category,"
        "instrument_type,direction,official_status,has_pdf,has_archived_file,"
        "confidence,review_status,policy_intensity FROM v_policy_action_center WHERE "
        + where
        + " ORDER BY record_date DESC NULLS LAST LIMIT 100 OFFSET ?",
        [*params, (page - 1) * 100],
    )
    safe_dataframe(rows, height=440)
    st.download_button(
        "下载当前页 CSV",
        rows.write_csv().encode("utf-8-sig"),
        "policy_center.csv",
    )
    if rows.is_empty():
        return
    record_id = st.selectbox("查看政策详情", rows["record_id"].unique().to_list())
    policy = db.get(record_id)
    actions = db._query(
        "SELECT action_id,clause_text,primary_category,secondary_category,direction,"
        "evidence_text,confidence,review_status FROM v_policy_action_center WHERE record_id=?",
        [record_id],
    )
    files = db._query(
        "SELECT archive_relative_path,content_type,sha256_actual,archive_status "
        "FROM policy_files WHERE record_id=?",
        [record_id],
    )
    with st.expander("原文、动作、证据与档案", expanded=True):
        st.write(policy.get("summary") or "暂无摘要")
        st.text_area("政策原文", policy.get("full_text") or "暂无原文", height=260, disabled=True)
        st.subheader("政策动作与证据")
        safe_dataframe(actions)
        st.subheader("PDF和附件档案")
        safe_dataframe(files)
