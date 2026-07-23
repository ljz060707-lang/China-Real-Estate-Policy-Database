from __future__ import annotations

import plotly.express as px
import streamlit as st

from app.theme import style_plotly_figure
from app.ui import safe_pandas


def render_overview(db) -> None:
    quality = db._query("SELECT * FROM v_data_quality").row(0, named=True)
    metrics = db._query(
        "SELECT max(record_date) latest_date,count(DISTINCT city) city_count,"
        "avg(CASE WHEN official_status IN ('official','official_reprint') THEN 1.0 ELSE 0.0 END) official_share "
        "FROM v_policy_action_center"
    ).row(0, named=True)
    for column, (label, value) in zip(
        st.columns(4),
        [
            ("政策文件", quality["record_count"]),
            ("最新日期", str(metrics["latest_date"] or "—")),
            ("覆盖城市", metrics["city_count"]),
            ("官方来源占比", f"{float(metrics['official_share'] or 0):.1%}"),
        ],
        strict=True,
    ):
        column.metric(label, value)
    st.caption(f"待审核 {quality['pending_review_count']} · 缺正文 {quality['missing_full_text_count']} · 缺链接 {quality['missing_url_count']}")
    frame = safe_pandas(
        db._query(
            "SELECT year(record_date) AS record_year,count(DISTINCT record_id) policy_count "
            "FROM v_policy_action_center WHERE record_date IS NOT NULL GROUP BY 1 ORDER BY 1"
        )
    )
    if not frame.empty:
        chart = px.line(frame, x="record_year", y="policy_count", markers=True, title="政策文件年度趋势", color_discrete_sequence=["#4A148C"])
        st.plotly_chart(style_plotly_figure(chart), width="stretch")
