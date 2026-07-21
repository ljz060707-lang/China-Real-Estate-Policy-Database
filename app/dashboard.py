from __future__ import annotations

import sys
from pathlib import Path

import plotly.express as px
import streamlit as st
import streamlit.components.v1 as components

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))
from app.crawl_center import render_crawl_center  # noqa: E402
from app.geography_panel import (  # noqa: E402
    GeographyPanelUnavailable,
    filter_options,
    panel_health,
    query_region_panel,
    tianditu_map_html,
)
from app.quality_center import render_quality_center  # noqa: E402
from app.review_center import render_review_center  # noqa: E402
from app.settings_page import render_settings_page  # noqa: E402
from app.setup_wizard import needs_initial_setup, render_setup_wizard  # noqa: E402
from app.theme import (  # noqa: E402
    apply_academic_theme,
    render_page_header,
    render_sidebar_brand,
    style_plotly_figure,
)
from app.ui import safe_dataframe, safe_pandas  # noqa: E402
from policydb import PolicyDB  # noqa: E402

st.set_page_config(page_title="中国房地产政策数据库", layout="wide")
apply_academic_theme()
render_sidebar_brand()

if needs_initial_setup(ROOT):
    render_setup_wizard(ROOT)
    st.stop()


@st.cache_resource(show_spinner=False)
def open_database() -> PolicyDB:
    return PolicyDB.open(ROOT)


db = open_database()
page = st.sidebar.radio(
    "页面",
    [
        "数据总览",
        "政策体系",
        "105城市",
        "政策检索",
        "时间趋势",
        "地区比较",
        "专题页面",
        "数据质量",
        "人工审核中心",
        "智能抓取",
        "个人设置",
    ],
)

PAGE_HEADERS = {
    "数据总览": ("数据总览", "覆盖政策记录、地域范围、来源质量与人工审核状态。"),
    "政策体系": ("七大政策体系", "按部门职责与研究用途浏览七大库及其细分类；一条政策可归入多个体系。"),
    "105城市": ("105个大城市政策覆盖", "基于2020年第七次人口普查大城市范围，观察2018年至今城市政策覆盖与来源质量。"),
    "政策检索": ("政策检索", "按关键词、地区和官方来源快速定位政策记录。"),
    "时间趋势": ("时间趋势", "观察政策发布频率及其随时间的结构变化。"),
    "地区比较": ("地区比较", "比较不同城市的政策数量与研究覆盖情况。"),
    "专题页面": ("专题研究", "面向供给侧、城市更新、白名单等专题提取研究样本。"),
    "数据质量": ("覆盖与质量", "区分未扫描、部分覆盖、发现政策和确认零政策，并审计来源、去重与字段证据。"),
    "智能抓取": ("智能抓取", "后台执行来源发现、抓取、解析、复核和报告生成。"),
    "个人设置": ("个人设置", "安全管理模型、地图、搜索和抓取偏好。"),
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
    summary = safe_pandas(db._query("SELECT * FROM v_policy_library_summary"))
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
        st.plotly_chart(style_plotly_figure(figure), width="stretch")
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
    sql += " ORDER BY record_date DESC NULLS LAST LIMIT 200"
    frame = db._query(sql, params)
    safe_dataframe(frame, height=430)
    st.download_button(
        "下载当前结果 CSV",
        frame.write_csv().encode("utf-8-sig"),
        f"{selected_code}.csv",
    )
elif page == "105城市":
    city_reference = db._query(
        "SELECT city_id,city_name,province_name,city_tier_existing,city_scale_2020 "
        "FROM cities_105 ORDER BY province_name,city_name"
    )
    province_options = ["全部"] + sorted(
        city_reference["province_name"].unique().to_list()
    )
    province = st.selectbox("省份", province_options)
    available_cities = city_reference
    if province != "全部":
        available_cities = available_cities.filter(
            available_cities["province_name"] == province
        )
    city = st.selectbox("城市", ["全部"] + available_cities["city_name"].to_list())
    max_year = int(db._query("SELECT max(year) FROM v_city_month_policy_panel_105").item())
    year = st.selectbox("年份", ["全部"] + list(range(max_year, 2017, -1)))
    month = st.selectbox("月份", ["全部"] + list(range(1, 13)))
    tier_values = sorted(
        value for value in city_reference["city_tier_existing"].drop_nulls().unique().to_list()
    )
    tier = st.selectbox("城市能级", ["全部"] + tier_values)
    collection_summary = db._query(
        "SELECT DISTINCT collection_code,collection_name FROM v_policy_library_summary"
    )
    collection_name = st.selectbox(
        "七大政策体系", ["全部"] + collection_summary["collection_name"].to_list()
    )
    collection_code = None
    subcategory = "全部"
    if collection_name != "全部":
        collection_code = collection_summary.filter(
            collection_summary["collection_name"] == collection_name
        )[0, "collection_code"]
        subcategories = db._query(
            "SELECT DISTINCT subcollection_name FROM record_collections "
            "WHERE collection_code=? AND subcollection_name IS NOT NULL ORDER BY 1",
            [collection_code],
        )["subcollection_name"].to_list()
        subcategory = st.selectbox("二级政策类别", ["全部"] + subcategories)
    direction = st.selectbox(
        "政策方向", ["全部", "loosening", "supportive", "tightening", "neutral", "mixed", "unknown"]
    )
    official_status = st.selectbox(
        "官方状态",
        ["全部"]
        + db._query("SELECT DISTINCT official_status FROM records ORDER BY 1")[
            "official_status"
        ].to_list(),
    )
    source_type = st.selectbox(
        "来源类型",
        ["全部"]
        + db._query("SELECT DISTINCT source_type FROM source_registry ORDER BY 1")[
            "source_type"
        ].to_list(),
    )
    issuer = st.text_input("发布主体")
    clauses = ["NOT p.needs_review"]
    params: list[object] = []
    if province != "全部":
        clauses.append("p.province=?")
        params.append(province)
    if city != "全部":
        clauses.append("p.city_name=?")
        params.append(city)
    if year != "全部":
        clauses.append("year(p.record_date)=?")
        params.append(year)
    if month != "全部":
        clauses.append("month(p.record_date)=?")
        params.append(month)
    if tier != "全部":
        clauses.append("p.city_tier_existing=?")
        params.append(tier)
    if collection_code:
        clauses.append(
            "EXISTS(SELECT 1 FROM record_collections rc WHERE rc.record_id=p.record_id "
            "AND rc.collection_code=?)"
        )
        params.append(collection_code)
    if subcategory != "全部":
        clauses.append(
            "EXISTS(SELECT 1 FROM record_collections rc WHERE rc.record_id=p.record_id "
            "AND rc.subcollection_name=?)"
        )
        params.append(subcategory)
    if direction != "全部":
        clauses.append("p.direction=?")
        params.append(direction)
    if official_status != "全部":
        clauses.append("p.official_status=?")
        params.append(official_status)
    if source_type != "全部":
        clauses.append(
            "EXISTS(SELECT 1 FROM policy_sources ps JOIN source_registry sr USING(source_id) "
            "WHERE ps.record_id=p.record_id AND sr.source_type=?)"
        )
        params.append(source_type)
    if issuer:
        clauses.append(
            "EXISTS(SELECT 1 FROM record_organizations ro JOIN organizations o "
            "USING(organization_id) WHERE ro.record_id=p.record_id "
            "AND o.name_standardized ILIKE ?)"
        )
        params.append(f"%{issuer}%")
    where = " AND ".join(clauses)
    policy_count, official = db._query(
        "SELECT count(DISTINCT p.record_id),count(DISTINCT CASE WHEN p.official_status IN "
        "('official','official_reprint') THEN p.record_id END) FROM v_policy_105_cities p WHERE "
        + where,
        params,
    ).row(0)
    covered = db._query(
        "SELECT count(DISTINCT city_id) FROM v_policy_105_cities WHERE NOT needs_review"
    ).item()
    for column, (label, value) in zip(
        st.columns(5),
        [
            ("城市范围", 105),
            ("已确定政策城市", covered),
            ("筛选政策数", int(policy_count)),
            ("官方政策占比", f"{official / policy_count:.1%}" if policy_count else "—"),
            ("待审核适用关系", db._query("SELECT count(*) FROM policy_applicable_cities WHERE needs_review").item()),
        ],
        strict=True,
    ):
        column.metric(label, value)
    trend = safe_pandas(db._query(
        "SELECT year(p.record_date) AS \"year\",month(p.record_date) AS \"month\","
        "count(DISTINCT p.record_id) policy_count,"
        "count(DISTINCT CASE WHEN p.direction IN ('loosening','supportive') THEN p.record_id END) easing_count,"
        "count(DISTINCT CASE WHEN p.direction='tightening' THEN p.record_id END) tightening_count "
        "FROM v_policy_105_cities p WHERE "
        + where
        + " GROUP BY 1,2 ORDER BY 1,2",
        params,
    ))
    trend["period"] = trend["year"].astype(str) + "-" + trend["month"].astype(str).str.zfill(2)
    trend_long = trend.melt(
        id_vars="period",
        value_vars=["policy_count", "easing_count", "tightening_count"],
        var_name="指标",
        value_name="数量",
    )
    figure = px.line(
        trend_long,
        x="period",
        y="数量",
        color="指标",
        title="政策数量与方向趋势",
        color_discrete_sequence=["#82318E", "#A66BB0", "#4B1F5E"],
    )
    st.plotly_chart(style_plotly_figure(figure), width="stretch")
    ranking = db._query(
        "SELECT p.city_name,count(DISTINCT p.record_id) policy_count,"
        "avg(CASE WHEN p.official_status IN ('official','official_reprint') THEN 1.0 ELSE 0.0 END) official_share "
        "FROM v_policy_105_cities p WHERE "
        + where
        + " GROUP BY p.city_name ORDER BY policy_count DESC",
        params,
    )
    safe_dataframe(ranking, height=340)
    details = db._query(
        "SELECT p.record_id,p.record_date,p.city_name,p.province,p.title,p.direction,"
        "p.official_status,p.source_quality,p.primary_source_url,p.source_sheet "
        "FROM v_policy_105_cities p WHERE "
        + where
        + " ORDER BY p.record_date DESC NULLS LAST LIMIT 200",
        params,
    )
    safe_dataframe(details, height=430)
    source_health = db._query(
        "SELECT official_status,priority,count(*) source_count,"
        "count(*) FILTER(WHERE crawl_enabled) enabled_count "
        "FROM source_registry GROUP BY ALL ORDER BY priority,official_status"
    )
    with st.expander("来源健康状态"):
        safe_dataframe(source_health)
        latest_crawl = db._query("SELECT max(started_at) FROM crawl_runs").item()
        st.caption(f"最近抓取时间：{latest_crawl or '尚未启用来源'}")
    st.download_button(
        "下载105城市筛选结果 CSV",
        details.write_csv().encode("utf-8-sig"),
        "city_panel_105.csv",
    )
elif page == "政策检索":
    keyword = st.text_input("关键词")
    region = st.text_input("省/市/区县")
    official = st.checkbox("仅官方")
    frame = db.search(
        keyword=keyword or None,
        region=region or None,
        official_only=official,
        limit=200,
        include_full_text=False,
    )
    st.caption(f"显示前 {frame.height} 条结果；政策全文仅在选择记录后加载。")
    safe_dataframe(frame, height=430)
    st.download_button("下载 CSV", frame.write_csv().encode("utf-8-sig"), "policy_search.csv")
    if not frame.is_empty():
        record_ids = frame["record_id"].to_list()
        selected_record = st.selectbox(
            "查看政策详情",
            record_ids,
            format_func=lambda value: str(
                frame.filter(frame["record_id"] == value)[0, "title"] or value
            ),
        )
        policy = db.get(selected_record)
        if policy:
            with st.expander("摘要、全文与来源", expanded=False):
                st.write(policy.get("summary") or "暂无摘要")
                st.text_area(
                    "政策原文",
                    value=policy.get("full_text") or "暂无原文",
                    height=320,
                    disabled=True,
                )
                source_url = policy.get("primary_source_url")
                if source_url and str(source_url).startswith(("http://", "https://")):
                    st.link_button("打开原始来源", str(source_url))
elif page == "时间趋势":
    frame = safe_pandas(db._query(
        "SELECT year(record_date) AS \"year\",month(record_date) AS \"month\","
        "count(*) AS \"count\" "
        "FROM records WHERE record_date IS NOT NULL GROUP BY ALL ORDER BY 1,2"
    ))
    frame["period"] = frame["year"].astype(str) + "-" + frame["month"].astype(str).str.zfill(2)
    figure = px.line(
        frame,
        x="period",
        y="count",
        title="月度政策数量",
        color_discrete_sequence=["#82318E"],
    )
    figure.update_traces(line={"width": 2.4}, marker={"size": 4})
    st.plotly_chart(style_plotly_figure(figure), width="stretch")
elif page == "地区比较":
    try:
        health = panel_health(db)
        options = filter_options(db)
    except GeographyPanelUnavailable as exc:
        st.error(str(exc))
        st.code("uv run policydb normalize-geography\nuv run policydb build-database")
        st.stop()
    filters = st.columns([1.0, 1.2, 1.2, 1.0, 1.3])
    level = filters[0].selectbox("层级", ["全国", "省级", "地级市"], index=1)
    province_choice = filters[1].selectbox("省份", ["全部", *options["provinces"]])
    city_choice = filters[2].selectbox("城市", ["全部", *options["cities"]])
    year_choice = filters[3].selectbox("年份", ["全部", *options["years"]])
    topic_choice = filters[4].selectbox("政策类型", ["全部", *options["topics"]])
    page_size = 30
    page_number = int(st.number_input("排名页码", min_value=1, value=1, step=1))
    result = query_region_panel(
        db,
        level,
        province=None if province_choice == "全部" else province_choice,
        city=None if city_choice == "全部" else city_choice,
        year=None if year_choice == "全部" else int(year_choice),
        topic=None if topic_choice == "全部" else topic_choice,
        limit=page_size,
        offset=(page_number - 1) * page_size,
    )
    ranking = result["ranking"]
    trend = result["trend"]
    if ranking.is_empty():
        st.info("当前筛选条件没有地区政策数据。请减少筛选条件或切换统计层级。")
    else:
        rank_pd = safe_pandas(ranking)
        trend_pd = safe_pandas(trend)
        rank_tab, trend_tab, map_tab = st.tabs(["总体排名", "时间趋势", "天地图"])
        with rank_tab:
            figure = px.bar(
                rank_pd,
                x="region",
                y="policy_count",
                hover_data=["official_policy_count", "official_share"],
                title=f"{level}政策数量排名（第 {page_number} 页）",
                color_discrete_sequence=["#82318E"],
            )
            figure.update_traces(marker_line_width=0)
            st.plotly_chart(style_plotly_figure(figure), width="stretch")
            safe_dataframe(ranking, height=360)
            st.caption(f"共 {result['total']} 个地区；当前每页 {page_size} 个。")
        with trend_tab:
            if trend.is_empty():
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
                    y=["policy_count", "official_policy_count", "easing_count"],
                    title="地区政策月度趋势",
                    color_discrete_sequence=["#82318E", "#4B1F5E", "#A66BB0"],
                )
                st.plotly_chart(style_plotly_figure(figure), width="stretch")
        with map_tab:
            map_html = tianditu_map_html(ranking["region"].to_list())
            if map_html:
                components.html(map_html, height=565)
            else:
                st.info(
                    "地图未配置天地图 Key。设置 TIANDITU_TOKEN 后即可显示；"
                    "柱状排名和时间趋势不依赖地图服务。"
                )
    with st.expander("地区视图运行状态"):
        st.write(f"v_city_month_policy_panel：{health['row_count']} 行")
        st.code(
            "DESCRIBE v_city_month_policy_panel;\n"
            "SELECT COUNT(*) FROM v_city_month_policy_panel;\n"
            "SELECT * FROM v_city_month_policy_panel LIMIT 10;"
        )
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
    frame = db._query(f"SELECT * FROM {views.get(topic, 'v_policy_master')} LIMIT 200")
    safe_dataframe(frame, height=430)
elif page == "数据质量":
    render_quality_center(db)
elif page == "人工审核中心":
    render_review_center(ROOT)
elif page == "智能抓取":
    render_crawl_center(ROOT)
else:
    render_settings_page(ROOT)
