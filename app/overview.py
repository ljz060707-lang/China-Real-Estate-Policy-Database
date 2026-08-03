from __future__ import annotations

from pathlib import Path

import plotly.express as px
import polars as pl
import streamlit as st

from app.theme import style_plotly_figure
from app.ui import safe_pandas
from policydb.dashboard_metrics import (
    city_role_matrix,
    city_year_coverage,
    cycle_history,
    document_quality,
    gap_register,
    overview_metrics,
    source_health,
)
from policydb.settings import Settings

ROOT = Path(__file__).resolve().parents[1]


def _display(value: object) -> str:
    if value is None:
        return "不可用"
    if isinstance(value, float):
        return f"{value:.1%}"
    return f"{value:,}" if isinstance(value, int) else str(value)


def _metric_card(metric: dict) -> None:
    value = metric.get("percent")
    if metric.get("denominator") is not None:
        label = f"{metric['label']} ({metric['numerator']}/{metric['denominator']})"
        shown = "不可用" if value is None else f"{value:.1%}"
    else:
        label = metric["label"]
        shown = _display(metric.get("numerator"))
    st.metric(label, shown)
    st.caption(metric.get("definition", ""))


def render_overview(db) -> None:
    del db  # Dashboard aggregates are read-only Parquet views, not a long DB transaction.
    settings = Settings.discover(ROOT)
    data = overview_metrics(settings)
    st.subheader("项目总览")
    st.caption("Bronze 快速覆盖与 Silver 清洗/验证已启用；Gold 政策强度当前仅保留占位页。")
    cards = list(data["kpis"].values())
    for offset in range(0, len(cards), 4):
        columns = st.columns(min(4, len(cards) - offset))
        for column, metric in zip(columns, cards[offset : offset + 4], strict=False):
            with column:
                _metric_card(metric)

    st.markdown("### 当前健康度")
    health_columns = st.columns(6)
    health_columns[0].metric("政策文档", _display(data.get("document_count")))
    health_columns[1].metric("open gaps", _display(data.get("open_gaps")))
    health_columns[2].metric("critical gaps", _display(data.get("critical_gaps")))
    health_columns[3].metric("PARTIAL_BUT_USABLE", _display(data.get("partial_sources")))
    health_columns[4].metric("最新文档日期", data.get("latest_document_date") or "不可用")
    health_columns[5].metric("最近进展", data.get("last_progress_at") or "不可用")

    tabs = st.tabs(["抓取进展", "城市×来源角色", "年份覆盖", "来源与缺口", "系统架构", "政策强度占位"])
    with tabs[0]:
        runtime = data.get("runtime") or {}
        st.json({key: runtime.get(key) for key in ("automation_id", "run_id", "mode", "round", "status", "current_city", "current_source", "current_step", "documents_added", "last_heartbeat_at") if key in runtime})
        history = cycle_history(settings)
        if not history.is_empty():
            st.dataframe(safe_pandas(history), hide_index=True, width="stretch")
    with tabs[1]:
        matrix = city_role_matrix(settings)
        if matrix.is_empty():
            st.info("暂无来源槽位数据。")
        else:
            st.dataframe(safe_pandas(matrix), hide_index=True, width="stretch", height=520)
            st.download_button("导出城市×来源矩阵", matrix.write_csv().encode("utf-8-sig"), "city_role_matrix.csv", "text/csv")
    with tabs[2]:
        year_frame = city_year_coverage(settings)
        if year_frame.is_empty():
            st.info("暂无 city-year 文档覆盖数据。")
        else:
            chart_frame = year_frame.group_by("year").agg(pl.len().alias("cities"), pl.col("document_count").sum()).sort("year")
            chart = px.bar(safe_pandas(chart_frame), x="year", y="cities", title="有政策文本的城市数（日期窗口动态读取）")
            st.plotly_chart(style_plotly_figure(chart), width="stretch")
            st.dataframe(safe_pandas(year_frame), hide_index=True, width="stretch")
    with tabs[3]:
        health = source_health(settings)
        gaps = gap_register(settings)
        st.markdown("#### 来源健康")
        if health.is_empty():
            st.info("暂无来源健康状态。")
        else:
            st.dataframe(safe_pandas(health), hide_index=True, width="stretch", height=320)
        st.markdown("#### 缺口")
        if gaps.is_empty():
            st.info("暂无缺口登记或数据尚未刷新。")
        else:
            st.dataframe(safe_pandas(gaps), hide_index=True, width="stretch", height=320)
            st.download_button("导出缺口 CSV", gaps.write_csv().encode("utf-8-sig"), "coverage_gaps.csv", "text/csv")
    with tabs[4]:
        st.markdown("#### Source Discovery → Verification → Bronze → Silver → Research Snapshot → Gold placeholder")
        st.code("""搜索/AI 来源发现\n        ↓\n确定性来源验证与准入\n        ↓\nBronze：快速原始抓取\n        ↓\nSilver：清洗、解析、去重、缺口登记\n        ↓\nResearch Snapshot（只读）\n        ↓\nGold：政策强度（当前禁用占位）""", language="text")
        st.caption("数据库、Curated Parquet、checkpoint、版本历史、HTTP/AI/search 审计和 Dashboard 操作队列保持分层。")
    with tabs[5]:
        quality = document_quality(settings)
        st.warning("政策强度测度：尚未启用")
        st.write("原因：政策强度指标体系仍在设计")
        st.write(f"已有可测度文档：{quality.get('total', 0)}（仅表示文档存在，不表示已测度）")
        st.write("已测度文档：0")
        st.write("下一步：配置指标体系、提示词版本和测度模型后启用")
