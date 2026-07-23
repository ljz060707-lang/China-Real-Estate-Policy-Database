from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

ROOT = Path(__file__).resolve().parents[1]
for path in (ROOT, ROOT / "src"):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from app.automation_center import render_automation_center  # noqa: E402
from app.overview import render_overview  # noqa: E402
from app.policy_center import render_policy_center  # noqa: E402
from app.quality_center import render_quality_center  # noqa: E402
from app.review_center import render_review_center  # noqa: E402
from app.settings_page import render_settings_page  # noqa: E402
from app.setup_wizard import needs_initial_setup, render_setup_wizard  # noqa: E402
from app.theme import apply_academic_theme, render_page_header, render_sidebar_brand  # noqa: E402
from policydb import PolicyDB  # noqa: E402

PAGES = {
    "数据总览": ("数据总览", "政策资料、来源与研究就绪状态。", render_overview),
    "政策中心": ("政策中心", "在一个工作台完成筛选、统计、查看原文与导出。", render_policy_center),
    "自动更新与完整性": ("自动更新与完整性", "更新任务、来源覆盖与运行报告。", render_automation_center),
    "数据质量": ("数据质量", "正文、版本、分类、地区与覆盖问题。", render_quality_center),
    "人工审核": ("人工审核", "仅处理自动诊断无法可靠完成的关键异常。", render_review_center),
    "个人设置": ("个人设置", "AI、搜索、档案、地图与本地运行设置。", render_settings_page),
}

st.set_page_config(page_title="中国房地产政策数据库", layout="wide")
apply_academic_theme()
render_sidebar_brand()

if needs_initial_setup(ROOT):
    render_setup_wizard(ROOT)
    st.stop()


@st.cache_resource(show_spinner=False)
def open_database() -> PolicyDB:
    return PolicyDB.open(ROOT)


page = st.sidebar.radio("页面", list(PAGES), key="main_navigation")
title, subtitle, renderer = PAGES[page]
render_page_header(title, subtitle)
db = open_database()
if renderer in {render_policy_center, render_automation_center, render_review_center, render_settings_page}:
    renderer(ROOT) if renderer is not render_policy_center else renderer(db, ROOT)
else:
    renderer(db)

if st.session_state.get("developer_mode"):
    with st.sidebar.expander("开发模式 · 旧版页面已隐藏"):
        st.caption("旧分类、旧视图和 CLI 仍保留用于数据血缘与兼容，不作为普通用户导航。")
