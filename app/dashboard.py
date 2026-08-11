from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

ROOT = Path(__file__).resolve().parents[1]
for path in (ROOT, ROOT / "src"):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from app.dashboard_pages import (  # noqa: E402
    render_collection_page,
    render_overview_page,
    render_policy_center_page,
    render_quality_page,
    render_review_page,
    render_system_page,
)
from app.setup_wizard import needs_initial_setup, render_setup_wizard  # noqa: E402
from app.theme import apply_academic_theme, render_page_header, render_sidebar_brand  # noqa: E402
from policydb.dashboard_logging import log_dashboard_exception  # noqa: E402
from policydb.settings import Settings  # noqa: E402

PAGES = {
    "总览": (
        "总览",
        "实时掌握抓取位置、政策数据规模、来源门控与待补缺口。",
        render_overview_page,
    ),
    "政策中心": (
        "政策中心",
        "在可审计的正式数据与只读 Curated 索引中筛选、查阅和导出政策。",
        render_policy_center_page,
    ),
    "采集与处理": (
        "采集与处理",
        "区分实时抓取、后处理和严格完整性，监控 105 城历史回溯。",
        render_collection_page,
    ),
    "数据质量与覆盖率": (
        "数据质量与覆盖率",
        "核查城市、年份、来源、正文、归档与缺口，不用单一黑盒分数掩盖问题。",
        render_quality_page,
    ),
    "人工审核": (
        "人工审核",
        "集中处理来源冲突、字段缺失、低置信度和机器无法可靠裁决的异常。",
        render_review_page,
    ),
    "系统与设置": (
        "系统与设置",
        "查看数据库、抓取器、自动化、Provider、归档和本机运行健康度。",
        render_system_page,
    ),
}

st.set_page_config(
    page_title="中国房地产政策数据库",
    page_icon="🏛️",
    layout="wide",
    initial_sidebar_state="expanded",
)
apply_academic_theme()
render_sidebar_brand()

if needs_initial_setup(ROOT):
    render_setup_wizard(ROOT)
    st.stop()

settings = Settings.discover(ROOT)
page = st.sidebar.radio("主导航", list(PAGES), key="main_navigation")
st.sidebar.caption("实时页面每 20 秒局部刷新 · 仅监听本机")
title, subtitle, renderer = PAGES[page]
render_page_header(title, subtitle)

try:
    renderer(settings)
except Exception as exc:
    log_dashboard_exception(
        settings,
        "Dashboard renderer failed",
        component="dashboard",
        operation=page,
        data_source=str(settings.database),
        relation="dashboard_snapshot_or_renderer",
        query=renderer.__name__,
        error=exc,
    )
    st.error("Dashboard 暂时无法读取当前数据快照。抓取进程不会因此中断，请稍后刷新。")
    if st.session_state.get("developer_mode"):
        st.exception(exc)
