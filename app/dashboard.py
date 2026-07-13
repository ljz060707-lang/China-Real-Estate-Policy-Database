from __future__ import annotations

import sys
from pathlib import Path

import plotly.express as px
import streamlit as st

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))
from app.review_center import render_review_center  # noqa: E402
from app.theme import (  # noqa: E402
    apply_academic_theme,
    render_page_header,
    render_sidebar_brand,
    style_plotly_figure,
)
from policydb import PolicyDB  # noqa: E402

st.set_page_config(page_title="中国房地产政策数据库", layout="wide")
apply_academic_theme()
render_sidebar_brand()
db = PolicyDB.open(ROOT)
page = st.sidebar.radio(
    "页面",
    [
        "数据总览",
        "政策体系",
        "政策检索",
        "时间趋势",
        "地区比较",
        "专题页面",
        "数据质量",
        "人工审核中心",
    ],
)

PAGE_HEADERS = {
    "数据总览": ("数据总览", "覆盖政策记录、地域范围、来源质量与人工审核状态。"),
    "政策体系": ("七大政策体系", "按部门职责与研究用途浏览七大库及其细分类；一条政策可归入多个体系。"),
    "政策检索": ("政策检索", "按关键词、地区和官方来源快速定位政策记录。"),
    "时间趋势": ("时间趋势", "观察政策发布频率及其随时间的结构变化。"),
    "地区比较": ("地区比较", "比较不同城市的政策数量与研究覆盖情况。"),
    "专题页面": ("专题研究", "面向供给侧、城市更新、白名单等专题提取研究样本。"),
    "数据质量": ("数据质量", "集中查看缺失、重复、来源和待审核问题。"),
}
if page in PAGE_HEADERS:
    render_page_header(*PAGE_HEADERS[page])

if page == "数据总览":
    quality = db._query("SELECT * FROM v_data_quality").row(0, named=True)
    latest = str(db._query("SELECT max(record_date) latest FROM records").item())
    city_count = db._query(
        "SELECT count(DISTINCT jurisdiction_id) FROM record_jurisdictions"
    ).item()
    official_share = (
        db._query(
            "SELECT avg(CASE WHEN official_status IN ('official','official_reprint') "
            "THEN 1.0 ELSE 0.0 END) FROM records"
        ).item()
        or 0
    )
    cards = [
        ("政策总量", quality["record_count"]),
        ("最新日期", latest),
        ("覆盖城市", city_count),
        ("官方来源占比", f"{official_share:.1%}"),
        ("待审核", quality["pending_review_count"]),
    ]
    for column, (label, value) in zip(st.columns(5), cards, strict=True):
        column.metric(label, value)
elif page == "政策体系":
    summary = db._query("SELECT * FROM v_policy_library_summary").to_pandas()
    collections = summary[["collection_code", "collection_name"]].drop_duplicates()
    selected_name = st.selectbox("政策库", collections["collection_name"].tolist())
    selected_code = collections.loc[
        collections["collection_name"] == selected_name, "collection_code"
    ].iloc[0]
    subset = summary[
        (summary["collection_code"] == selected_code)
        & summary["subcollection_code"].notna()
    ].copy()
    total = db._query(
        "SELECT count(DISTINCT record_id) FROM v_policy_collection_long "
        "WHERE collection_code=?",
        [selected_code],
    ).item()
    pending = db._query(
        "SELECT count(DISTINCT record_id) FROM v_policy_collection_long "
        "WHERE collection_code=? AND review_status IN ('pending','unreviewed')",
        [selected_code],
    ).item()
    confidence = db._query(
        "SELECT avg(confidence) FROM v_policy_collection_long WHERE collection_code=?",
        [selected_code],
    ).item()
    for column, (label, value) in zip(
        st.columns(4),
        [
            ("政策记录", total),
            ("细分类", int(subset["subcollection_code"].nunique())),
            ("平均置信度", f"{float(confidence or 0):.1%}"),
            ("待人工确认", pending),
        ],
        strict=True,
    ):
        column.metric(label, value)
    if not subset.empty:
        figure = px.bar(
            subset.sort_values("record_count", ascending=True),
            x="record_count",
            y="subcollection_name",
            orientation="h",
            title=f"{selected_name}：细分类记录数",
            color_discrete_sequence=["#82318E"],
        )
        st.plotly_chart(style_plotly_figure(figure), use_container_width=True)
    options = ["全部"] + subset["subcollection_name"].dropna().tolist()
    selected_subcollection = st.selectbox("细分类筛选", options)
    sql = (
        "SELECT record_id,record_date,title,subcollection_name,official_status,"
        "confidence,classification_source,review_status,source_sheet,evidence_excerpt "
        "FROM v_policy_collection_long WHERE collection_code=?"
    )
    params: list[object] = [selected_code]
    if selected_subcollection != "全部":
        sql += " AND subcollection_name=?"
        params.append(selected_subcollection)
    sql += " ORDER BY record_date DESC NULLS LAST LIMIT 500"
    frame = db._query(sql, params)
    st.dataframe(frame.to_pandas(), use_container_width=True)
    st.download_button(
        "下载当前结果 CSV",
        frame.write_csv().encode("utf-8-sig"),
        f"{selected_code}.csv",
    )
elif page == "政策检索":
    keyword = st.text_input("关键词")
    region = st.text_input("省/市/区县")
    official = st.checkbox("仅官方")
    frame = db.search(
        keyword=keyword or None, region=region or None, official_only=official, limit=500
    )
    st.dataframe(frame.to_pandas(), use_container_width=True)
    st.download_button("下载 CSV", frame.write_csv().encode("utf-8-sig"), "policy_search.csv")
elif page == "时间趋势":
    frame = db._query(
        "SELECT year(record_date) AS \"year\",month(record_date) AS \"month\","
        "count(*) AS \"count\" "
        "FROM records WHERE record_date IS NOT NULL GROUP BY ALL ORDER BY 1,2"
    ).to_pandas()
    frame["period"] = frame["year"].astype(str) + "-" + frame["month"].astype(str).str.zfill(2)
    figure = px.line(
        frame,
        x="period",
        y="count",
        title="月度政策数量",
        color_discrete_sequence=["#82318E"],
    )
    figure.update_traces(line={"width": 2.4}, marker={"size": 4})
    st.plotly_chart(style_plotly_figure(figure), use_container_width=True)
elif page == "地区比较":
    frame = db._query(
        "SELECT city_name,count(*) policy_count FROM v_policy_master "
        "WHERE city_name IS NOT NULL GROUP BY 1 ORDER BY 2 DESC LIMIT 40"
    ).to_pandas()
    figure = px.bar(
        frame,
        x="city_name",
        y="policy_count",
        title="地区政策数量排名",
        color_discrete_sequence=["#82318E"],
    )
    figure.update_traces(marker_line_width=0)
    st.plotly_chart(style_plotly_figure(figure), use_container_width=True)
elif page == "专题页面":
    topic = st.selectbox(
        "专题",
        [
            "需求侧政策",
            "供给侧政策",
            "城市更新",
            "项目白名单",
            "PSL专项贷款",
            "中央会议表述",
            "公积金政策",
            "限购限售",
        ],
    )
    views = {
        "供给侧政策": "v_supply_side_measures",
        "城市更新": "v_urban_renewal_policies",
        "项目白名单": "v_white_list_events",
        "PSL专项贷款": "v_psl_financing_events",
        "中央会议表述": "v_official_statements",
    }
    frame = db._query(f"SELECT * FROM {views.get(topic, 'v_policy_master')} LIMIT 500")
    st.dataframe(frame.to_pandas(), use_container_width=True)
elif page == "数据质量":
    st.dataframe(db._query("SELECT * FROM v_data_quality").to_pandas(), use_container_width=True)
    st.info("需要逐条处理的问题，请进入左侧的“人工审核中心”。")
else:
    render_review_center(ROOT)
