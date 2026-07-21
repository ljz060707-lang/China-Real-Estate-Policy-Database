from __future__ import annotations

import plotly.express as px
import streamlit as st

from app.theme import style_plotly_figure
from app.ui import safe_dataframe, safe_pandas

REQUIRED_VIEWS = {
    "v_city_month_coverage",
    "v_source_city_matrix",
    "v_source_coverage_gaps",
    "v_dedup_audit",
    "v_policy_record_confidence",
}


def _available_views(db) -> set[str]:
    return set(
        db._query(
            "SELECT table_name FROM information_schema.tables WHERE table_schema='main'"
        )["table_name"].to_list()
    )


def render_quality_center(db) -> None:
    available = _available_views(db)
    missing = sorted(REQUIRED_VIEWS - available)
    if missing:
        st.error("V2 质量视图尚未就绪。请在本地运行迁移并重建数据库。")
        st.code("uv run policydb migrate-v2 apply\nuv run policydb build-database")
        st.caption("缺失视图：" + "、".join(missing))
        return

    coverage_tab, dedup_tab, confidence_tab, anomaly_tab = st.tabs(
        ["覆盖完整性", "增量与去重", "准确性与置信度", "异常与人工复核"]
    )

    with coverage_tab:
        status = db._query(
            "SELECT coverage_status,count(*) month_count,count(DISTINCT city_id) city_count "
            "FROM v_city_month_coverage GROUP BY 1 ORDER BY month_count DESC"
        )
        total = int(status["month_count"].sum()) if status.height else 0
        complete = int(
            status.filter(
                status["coverage_status"].is_in(
                    ["complete_policy_found", "complete_confirmed_zero"]
                )
            )["month_count"].sum()
        ) if status.height else 0
        zero = int(
            status.filter(status["coverage_status"] == "complete_confirmed_zero")[
                "month_count"
            ].sum()
        ) if status.height else 0
        for column, (label, value) in zip(
            st.columns(3),
            [("城市—月份", total), ("完整覆盖", complete), ("确认零政策", zero)],
            strict=True,
        ):
            column.metric(label, value)
        if status.height:
            chart_data = safe_pandas(status)
            figure = px.bar(
                chart_data,
                x="coverage_status",
                y="month_count",
                labels={"coverage_status": "覆盖状态", "month_count": "城市—月份数"},
                color_discrete_sequence=["#82318E"],
            )
            st.plotly_chart(style_plotly_figure(figure), width="stretch")
        year = st.selectbox(
            "查看年份",
            db._query(
                "SELECT DISTINCT year(month_start)::INTEGER AS coverage_year "
                "FROM v_city_month_coverage ORDER BY coverage_year DESC"
            )["coverage_year"].to_list(),
            key="quality_year",
        )
        safe_dataframe(
            db._query(
                "SELECT city_name,province_name,month_start,coverage_status,coverage_rate,"
                "expected_source_count,scanned_source_count,error_count "
                "FROM v_city_month_coverage WHERE year(month_start)=? "
                "ORDER BY month_start DESC,province_name,city_name LIMIT 500",
                [year],
            ),
            height=420,
        )

        st.subheader("来源矩阵")
        source_counts = db._query(
            "SELECT agency_type,required_level,count(DISTINCT source_id) source_count,"
            "count(DISTINCT city_id) city_count FROM v_source_city_matrix GROUP BY ALL "
            "ORDER BY source_count DESC"
        )
        if source_counts.is_empty():
            st.warning("来源登记尚未完成城市或省份范围映射；系统不会据此推断零政策。")
        else:
            safe_dataframe(source_counts, height=260)

    with anomaly_tab:
        st.subheader("来源范围与人工复核")
        unresolved = db._query(
            "SELECT source_id,source_name,domain,scope_type,agency_type,required_level,"
            "crawl_enabled,last_error FROM source_registry "
            "WHERE scope_type='unknown' OR "
            "(scope_type IN ('municipal','county','multi_region') AND len(city_ids)=0) OR "
            "(scope_type='provincial' AND len(province_codes)=0) "
            "ORDER BY official_status='official' DESC,priority LIMIT 500"
        )
        st.caption(f"待补充来源范围：{unresolved.height} 条（本页最多显示 500 条）")
        safe_dataframe(unresolved, height=420)
        st.info("记录级字段冲突和低置信问题继续由现有“人工审核中心”处理。")

    with dedup_tab:
        audit = db._query("SELECT * FROM v_dedup_audit ORDER BY dedup_level,decision")
        if audit.is_empty():
            st.info("尚无 V2 去重决策；下一次抓取会按 L0—L7 写入可审计决策。")
        else:
            safe_dataframe(audit, height=360)
        st.caption("任何涉及关键数值冲突的高相似文本都会保留为实质变化，不会自动合并。")

    with confidence_tab:
        bands = db._query(
            "SELECT confidence_band,count(*) record_count,"
            "avg(record_confidence) mean_confidence "
            "FROM v_policy_record_confidence GROUP BY 1 ORDER BY 1"
        )
        safe_dataframe(bands, height=220)
        review = db._query(
            "SELECT record_id,record_date,title,official_status,record_confidence,"
            "minimum_field_confidence,conflict_count,confidence_band "
            "FROM v_policy_record_confidence WHERE review_required "
            "ORDER BY conflict_count DESC,record_confidence NULLS FIRST LIMIT 300"
        )
        if review.is_empty():
            st.info("当前没有字段级冲突；尚未评分的记录会显示为 hold，而不会自动通过。")
        else:
            safe_dataframe(review, height=430)
