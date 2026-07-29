from __future__ import annotations

from datetime import date
from pathlib import Path

import plotly.express as px
import streamlit as st

from app.theme import style_plotly_figure
from app.ui import safe_pandas
from policydb.dashboard_queries import (
    cities_for_province,
    districts_for_cities,
    export_policy_list,
    filter_options,
    policy_city_ranking,
    policy_detail,
    policy_direction_distribution,
    policy_distribution,
    policy_list,
    policy_metrics,
    policy_trend,
)
from policydb.settings import Settings
from policydb.taxonomy_v2 import load_taxonomy

PURPLES = ["#4A148C", "#5B1AA8", "#6B21C8", "#7C3AED", "#B99AE7"]


def _taxonomy() -> tuple[dict[str, str], dict[str, str]]:
    taxonomy = load_taxonomy()
    primary = {code: value["name"] for code, value in taxonomy["primary_categories"].items()}
    secondary = {
        code: label
        for value in taxonomy["primary_categories"].values()
        for code, label in value["secondary"].items()
    }
    return primary, secondary


def _reset_filters() -> None:
    for key in tuple(st.session_state):
        if key.startswith("pc_"):
            del st.session_state[key]


def _filters(db, options: dict, primary_labels: dict[str, str], secondary_labels: dict[str, str]) -> dict:
    with st.container():
        st.markdown("#### 筛选条件")
        if st.button("清空筛选", key="pc_clear", width="stretch"):
            _reset_filters()
            st.rerun()
        st.caption("默认显示 2018 年至今的政策动作。")
        start = st.date_input("开始日期", value=st.session_state.get("pc_start", date(2018, 1, 1)), key="pc_start")
        end = st.date_input("结束日期", value=st.session_state.get("pc_end", date.today()), key="pc_end")
        province = st.selectbox("省份", ["全部", *options["provinces"]], key="pc_province")
        cities = cities_for_province(db, None if province == "全部" else province)
        selected_cities = st.multiselect("城市", cities, key="pc_cities")
        districts = districts_for_cities(db, selected_cities)
        district = st.selectbox("区县", ["全部", *districts], key="pc_district")
        primary_code = st.selectbox(
            "一级分类",
            ["全部", *options["primary"]],
            format_func=lambda value: "全部" if value == "全部" else primary_labels[value],
            key="pc_primary",
        )
        secondary_options = [
            row["secondary_category_code"]
            for row in options["secondary"]
            if primary_code == "全部" or row["primary_category_code"] == primary_code
        ]
        secondary_code = st.selectbox(
            "二级分类",
            ["全部", *secondary_options],
            format_func=lambda value: "全部" if value == "全部" else secondary_labels.get(value, value),
            key="pc_secondary",
        )
        keyword = st.text_input("关键词", placeholder="标题、动作或证据中的关键词", key="pc_keyword")
        with st.expander("更多筛选"):
            direction = st.selectbox("政策方向", ["全部", *options["directions"]], key="pc_direction")
            instrument = st.selectbox("政策工具", ["全部", *options["instruments"]], key="pc_instrument")
            issuer = st.selectbox("发布主体", ["全部", *options["issuers"]], key="pc_issuer")
            target = st.selectbox("适用主体", ["全部", *options["targets"]], key="pc_target")
            intensity = st.number_input("最低政策强度", min_value=0.0, max_value=10.0, value=0.0, step=0.1, key="pc_intensity")
            pdf = st.selectbox("是否有 PDF", ["全部", "有", "无"], key="pc_pdf")
            text = st.selectbox("正文是否完整", ["全部", "有正文", "缺正文"], key="pc_text")
            official = st.selectbox("官方状态", ["全部", *options["statuses"]], key="pc_official")
            review = st.selectbox("审核状态", ["全部", *options["reviews"]], key="pc_review")
        filters = {
            "start_date": start,
            "end_date": end,
            "province": None if province == "全部" else province,
            "cities": selected_cities,
            "district": None if district == "全部" else district,
            "primary_category_code": None if primary_code == "全部" else primary_code,
            "secondary_category_code": None if secondary_code == "全部" else secondary_code,
            "keyword": keyword.strip() or None,
            "direction": None if direction == "全部" else direction,
            "instrument_type": None if instrument == "全部" else instrument,
            "original_issuer": None if issuer == "全部" else issuer,
            "target_actor": None if target == "全部" else target,
            "minimum_intensity": intensity or None,
            "has_pdf": None if pdf == "全部" else pdf == "有",
            "full_text": None if text == "全部" else text == "有正文",
            "official_status": None if official == "全部" else official,
            "review_status": None if review == "全部" else review,
        }
        chosen = [primary_labels.get(primary_code, "") if primary_code != "全部" else "", secondary_labels.get(secondary_code, "") if secondary_code != "全部" else "", province if province != "全部" else "", *selected_cities]
        st.caption("已选：" + " · ".join(value for value in chosen if value) if any(chosen) else "已选：全部政策")
        if st.button("查询政策", type="primary", key="pc_query", width="stretch"):
            st.session_state["pc_page"] = 1
        return filters


def _charts(db, filters: dict, primary_labels: dict[str, str]) -> None:
    chart_tab, distribution_tab, direction_tab, city_tab, intensity_tab = st.tabs(
        ["趋势", "分类分布", "政策方向", "城市排名", "政策强度"]
    )
    with chart_tab:
        grain = st.segmented_control("时间粒度", ["month", "quarter", "year"], default="month", key="pc_grain")
        trend = safe_pandas(policy_trend(db, filters, grain or "month"))
        distribution = safe_pandas(policy_distribution(db, filters))
        left, right = st.columns(2)
        with left:
            if trend.empty:
                st.info("当前筛选条件下没有趋势数据。")
            else:
                figure = px.line(trend, x="period", y="action_count", markers=True, title="政策发布趋势", color_discrete_sequence=[PURPLES[0]])
                figure.update_traces(line={"width": 2.2}, marker={"size": 5})
                st.plotly_chart(style_plotly_figure(figure), width="stretch")
        with right:
            if distribution.empty:
                st.info("当前筛选条件下没有分类数据。")
            else:
                distribution["category"] = distribution["primary_category_code"].map(primary_labels)
                figure = px.bar(distribution, x="action_count", y="category", orientation="h", title="一级分类分布", color_discrete_sequence=PURPLES)
                figure.update_layout(showlegend=False)
                st.plotly_chart(style_plotly_figure(figure), width="stretch")
    with distribution_tab:
        distribution = safe_pandas(policy_distribution(db, filters))
        if distribution.empty:
            st.info("当前筛选条件下没有分类数据。")
        else:
            distribution["category"] = distribution["primary_category_code"].map(primary_labels)
            figure = px.pie(distribution, names="category", values="action_count", hole=0.58, color_discrete_sequence=PURPLES, title="一级分类分布")
            figure.update_layout(showlegend=False)
            st.plotly_chart(style_plotly_figure(figure), width="stretch")
    with direction_tab:
        frame = safe_pandas(policy_direction_distribution(db, filters))
        if frame.empty:
            st.info("当前筛选条件下没有方向数据。")
        else:
            figure = px.bar(frame, x="direction", y="action_count", title="政策方向", color_discrete_sequence=[PURPLES[1]])
            st.plotly_chart(style_plotly_figure(figure), width="stretch")
    with city_tab:
        frame = safe_pandas(policy_city_ranking(db, filters))
        if frame.empty:
            st.info("当前筛选条件下没有城市数据。")
        else:
            figure = px.bar(frame, x="policy_count", y="city", orientation="h", title="城市政策文件排名", color_discrete_sequence=[PURPLES[1]])
            st.plotly_chart(style_plotly_figure(figure), width="stretch")
    with intensity_tab:
        trend = safe_pandas(policy_trend(db, {**filters, "minimum_intensity": None}, "month"))
        if trend.empty:
            st.info("当前筛选条件下没有强度数据。")
        else:
            figure = px.line(trend, x="period", y="action_count", title="政策动作量（强度明细在列表与详情中查看）", color_discrete_sequence=[PURPLES[2]])
            st.plotly_chart(style_plotly_figure(figure), width="stretch")


def _policy_table(db, filters: dict, primary_labels: dict[str, str], secondary_labels: dict[str, str]) -> None:
    st.markdown("#### 政策列表")
    controls = st.columns([1, 1, 2])
    sort_by = controls[0].selectbox("排序", ["发布日期", "政策强度"], key="pc_sort")
    page_size = controls[1].selectbox("每页", [20, 50, 100], key="pc_page_size")
    page = int(st.session_state.get("pc_page", 1))
    rows, total = policy_list(db, filters, page=page, page_size=page_size, sort_by=sort_by)
    pages = max(1, (total + page_size - 1) // page_size)
    page = controls[2].number_input("页码", min_value=1, max_value=pages, value=min(page, pages), step=1, key="pc_page")
    rows, total = policy_list(db, filters, page=page, page_size=page_size, sort_by=sort_by)
    if rows.is_empty():
        st.info("当前筛选条件下没有政策。")
        return
    display = safe_pandas(rows).rename(
        columns={
            "record_date": "发布日期", "title": "政策标题", "province": "省份", "city": "地区",
            "primary_category_code": "一级分类", "secondary_category_code": "二级分类",
            "direction": "方向", "policy_intensity": "政策强度", "has_pdf": "PDF",
        }
    )
    display["一级分类"] = display["一级分类"].map(primary_labels).fillna("待分类")
    display["二级分类"] = display["二级分类"].map(secondary_labels).fillna("待分类")
    display["地区"] = display["地区"].fillna(display["省份"]).fillna("—")
    display["政策强度"] = display["政策强度"].round(2)
    display["PDF"] = display["PDF"].map({True: "PDF", False: "—"})
    display["操作"] = "查看"
    st.dataframe(display[["发布日期", "政策标题", "地区", "一级分类", "二级分类", "方向", "政策强度", "PDF", "操作"]], hide_index=True, width="stretch", height=430)
    record_ids = rows["record_id"].to_list()
    selected = st.selectbox(
        "查看政策",
        ["未选择", *record_ids],
        format_func=lambda value: "选择一条政策查看右侧详情" if value == "未选择" else str(rows.filter(rows["record_id"] == value)[0, "title"] or value),
        key="pc_selected",
    )
    if selected != "未选择":
        st.session_state["pc_detail_record"] = selected
    st.download_button("导出当前筛选结果 CSV", export_policy_list(db, filters).write_csv().encode("utf-8-sig"), "policy_center_export.csv", width="stretch")
    st.caption(f"共 {total:,} 个政策文件 · 第 {page}/{pages} 页")


def _detail_panel(db, root: Path, primary_labels: dict[str, str], secondary_labels: dict[str, str]) -> None:
    st.markdown("#### 政策详情")
    record_id = st.session_state.get("pc_detail_record")
    if not record_id:
        st.caption("从政策列表选择“查看”，详情会在此处加载。")
        return
    policy, actions, files = policy_detail(db, record_id)
    if not policy:
        st.warning("政策详情不可用。")
        return
    first = actions.row(0, named=True) if not actions.is_empty() else {}
    st.caption(first.get("review_status") or policy.get("status") or "待审核")
    st.markdown(f"**{policy.get('title') or '未命名政策'}**")
    st.caption(first.get("publication_issuer") or first.get("original_issuer") or "发布主体未标注")
    st.caption(f"{policy.get('record_date') or '日期未标注'} · {first.get('applicable_jurisdiction') or '适用地区未标注'}")
    st.caption(" / ".join(filter(None, [primary_labels.get(first.get("primary_category_code"), "待分类"), secondary_labels.get(first.get("secondary_category_code"), "")])) )
    st.write(policy.get("summary") or "暂无政策摘要。")
    if policy.get("primary_source_url"):
        st.link_button("打开原始网页", str(policy["primary_source_url"]), width="stretch")
    tabs = st.tabs(["政策原文", "政策动作", "分类与强度", "来源与版本"])
    with tabs[0]:
        st.caption(first.get("evidence_excerpt") or "暂无关键证据语句。")
        st.text_area("清洗后的政策正文", value=policy.get("full_text") or "暂无正文", height=300, disabled=True, key=f"pc_text_{record_id}")
        for file in files.iter_rows(named=True):
            path = root / str(file.get("archive_relative_path") or "")
            if file.get("content_type", "").lower().find("pdf") >= 0 and path.exists():
                st.download_button("下载 PDF/附件", path.read_bytes(), path.name, key=f"pc_file_{file['archive_relative_path']}", width="stretch")
    with tabs[1]:
        for action in actions.iter_rows(named=True):
            st.markdown(f"**{action.get('clause_text') or '待抽取动作'}**")
            st.caption(" · ".join(filter(None, [primary_labels.get(action.get("primary_category_code")), secondary_labels.get(action.get("secondary_category_code")), action.get("direction"), action.get("target_actor")])))
    with tabs[2]:
        st.caption("AI 分类仅在原文证据唯一匹配后展示；无证据结果保留审核状态。")
        st.dataframe(safe_pandas(actions[["primary_category_code", "secondary_category_code", "evidence_excerpt", "confidence", "policy_intensity", "review_status"]]), hide_index=True, width="stretch")
    with tabs[3]:
        st.caption(f"政策实体：{first.get('version_status') or '未建立'}")
        st.caption(f"重复簇：{first.get('duplicate_cluster_id') or '无'}")
        st.caption(f"本地归档：{first.get('archive_relative_path') or '未归档'}")
        if not files.is_empty():
            st.dataframe(safe_pandas(files), hide_index=True, width="stretch")


def render_policy_center(db, root: str | Path | None = None) -> None:
    primary_labels, secondary_labels = _taxonomy()
    options = filter_options(db)
    filter_column, main_column, detail_column = st.columns([1.2, 4.2, 1.7], gap="medium")
    with filter_column:
        filters = _filters(db, options, primary_labels, secondary_labels)
    with main_column:
        metrics = policy_metrics(db, filters)
        for column, (label, value) in zip(
            st.columns(4),
            [
                ("政策文件数", metrics["policy_count"]),
                ("政策动作数", metrics["action_count"]),
                ("覆盖城市数", metrics["city_count"]),
                ("PDF归档率", f"{float(metrics['pdf_share'] or 0):.1%}"),
            ],
            strict=True,
        ):
            column.metric(label, value)
        st.caption(f"近30天新增 {metrics['recent_count'] or 0} · 官方正文率 {float(metrics['official_share'] or 0):.1%} · 待审核动作 {metrics['review_count'] or 0}")
        _charts(db, filters, primary_labels)
        _policy_table(db, filters, primary_labels, secondary_labels)
    with detail_column:
        _detail_panel(
            db,
            Settings.discover(root).policy_archive_root,
            primary_labels,
            secondary_labels,
        )
