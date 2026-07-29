from __future__ import annotations

import json
from pathlib import Path

import plotly.express as px
import polars as pl
import streamlit as st

from app.crawl_center import render_crawl_section
from app.theme import style_plotly_figure
from app.ui import safe_pandas
from policydb.coverage import build_city_source_month_coverage
from policydb.jobs import JobManager
from policydb.schedule import schedule_status
from policydb.settings import Settings
from policydb.update.v2 import start_update


def _coverage(settings: Settings) -> pl.DataFrame:
    path = settings.root / "outputs/coverage/city_source_month_coverage.csv"
    return pl.read_csv(path) if path.exists() else pl.DataFrame()


def _start(layer: str, settings: Settings) -> None:
    try:
        result = start_update(layer, settings)
        st.success(f"后台任务已启动：{result['job_id']}")
    except Exception as exc:
        st.error(f"任务创建失败：{type(exc).__name__}")


def render_automation_center(root: str | Path) -> None:
    settings = Settings.discover(root)
    tabs = st.tabs(["运行状态", "覆盖完整性", "来源管理", "任务与报告"])
    with tabs[0]:
        status = schedule_status()
        states = JobManager(settings).list_states()
        active = [state for state in states if state.status not in {"completed", "completed_with_warnings", "failed", "cancelled"}]
        latest = states[0] if states else None
        for column, (label, value) in zip(
            st.columns(6),
            [
                ("自动更新", "已启用" if status["all_installed"] else "未完整安装"),
                ("下次运行", "由 Windows 任务计划管理" if status["all_installed"] else "尚未安装"),
                ("最近任务", str(latest.finished_at or latest.started_at) if latest else "—"),
                ("最近状态", latest.status if latest else "—"),
                ("今日新增", latest.counters.get("new_records", 0) if latest else 0),
                ("运行中任务", len(active)),
            ],
            strict=True,
        ):
            column.metric(label, value)
        actions = st.columns(3)
        for column, (label, layer) in zip(
            actions,
            [("立即运行每日更新", "daily"), ("运行周度补漏", "weekly"), ("运行月度完整性检查", "monthly")],
            strict=True,
        ):
            if column.button(label, width="stretch", disabled=settings.read_only):
                _start(layer, settings)
        if settings.read_only:
            st.info("当前为只读公开部署，仅可查看任务与报告。")
        with st.expander("高级：新建智能抓取任务"):
            render_crawl_section(settings.root, "运行状态")
    with tabs[1]:
        coverage = _coverage(settings)
        if coverage.is_empty():
            st.info("尚未生成覆盖矩阵。")
            if st.button("重建覆盖报告", disabled=settings.read_only):
                st.success(json.dumps(build_city_source_month_coverage(settings), ensure_ascii=False))
                st.rerun()
            return
        complete = coverage.filter(pl.col("coverage_status").str.starts_with("complete_")).height
        official = coverage.filter(pl.col("coverage_status") == "complete_policy_found").height
        denominator = max(coverage.height, 1)
        for column, (label, value) in zip(
            st.columns(5),
            [
                ("105城市覆盖率", f"{coverage['city_name'].n_unique() / 105:.1%}"),
                ("月份覆盖率", f"{complete / denominator:.1%}"),
                ("完整扫描窗口率", f"{complete / denominator:.1%}"),
                ("官方正文率", f"{official / denominator:.1%}"),
                ("待补扫城市数", coverage.filter(~pl.col("coverage_status").str.starts_with("complete_")).get_column("city_name").n_unique()),
            ],
            strict=True,
        ):
            column.metric(label, value)
        heat = (
            coverage.with_columns(pl.col("month").str.slice(0, 4).alias("year"))
            .group_by(["city_name", "year"])
            .agg(pl.col("coverage_status").str.starts_with("complete_").mean().alias("complete_rate"))
        )
        chart = px.density_heatmap(safe_pandas(heat), x="year", y="city_name", z="complete_rate", color_continuous_scale=[[0, "#FAF7FD"], [1, "#4A148C"]], title="城市 × 年份完整扫描率")
        st.plotly_chart(style_plotly_figure(chart), width="stretch")
        gaps = (
            coverage.filter(~pl.col("coverage_status").str.starts_with("complete_"))
            .group_by(["province_name", "city_name"])
            .len(name="缺口月份")
            .sort("缺口月份", descending=True)
        )
        st.markdown("#### 城市缺口")
        st.dataframe(safe_pandas(gaps), hide_index=True, width="stretch", height=320)
        st.download_button("下载覆盖矩阵 CSV", coverage.write_csv().encode("utf-8-sig"), "city_source_month_coverage.csv")
    with tabs[2]:
        render_crawl_section(settings.root, "来源管理")
    with tabs[3]:
        history, reports = st.tabs(["运行历史", "抓取报告"])
        with history:
            render_crawl_section(settings.root, "运行历史")
        with reports:
            render_crawl_section(settings.root, "抓取报告")
