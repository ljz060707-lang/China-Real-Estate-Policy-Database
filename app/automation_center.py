from __future__ import annotations

from pathlib import Path

import streamlit as st

from app.ui import safe_pandas
from policydb.dashboard_jobs import ALLOWED_ACTIONS, enqueue_job, list_jobs
from policydb.dashboard_metrics import (
    city_role_matrix,
    overview_metrics,
    pdf_city_coverage,
    pdf_failures,
    pdf_processing_funnel,
    pdf_summary,
)
from policydb.settings import Settings
from policydb.source_discovery import REQUIRED_ROLES


def _city_options(settings: Settings) -> list[tuple[str, str]]:
    matrix = city_role_matrix(settings)
    if matrix.is_empty():
        return []
    return sorted({(str(row["city_id"]), str(row.get("city_name") or row["city_id"])) for row in matrix.to_dicts()}, key=lambda item: item[1])


def _enqueue(settings: Settings, action: str, scope: dict, confirmed: bool) -> None:
    try:
        job = enqueue_job(settings, action, scope, confirmed=confirmed)
    except Exception as exc:
        st.error(f"操作未创建：{type(exc).__name__}: {exc}")
    else:
        st.success(f"已生成受校验任务 {job['job_id']}；由本地 operations worker 执行。")


def render_automation_center(root: str | Path) -> None:
    settings = Settings.discover(root)
    data = overview_metrics(settings)
    st.subheader("抓取与补齐操作中心")
    st.caption("Dashboard 只写结构化 job request；正式 CLI/业务层由独立 worker 执行，不接受任意 shell 命令。")
    tabs = st.tabs(["当前运行", "FAST_BULK_INGEST", "逐市补齐", "运行历史", "参数与阶段", "覆盖完整性", "PDF Pipeline"])
    with tabs[0]:
        runtime = data.get("runtime") or {}
        st.json(runtime if runtime else {"status": "NO_ACTIVE_RUN"})
        st.write("Gold policy intensity calls:", int((data.get("gold") or {}).get("policy_intensity_calls", 0)))
        st.write("安全停止：使用 scripts/stop_all_cities_task.ps1 或保留 STOP_FULL_SYNC 文件。")
    with tabs[1]:
        st.markdown("#### 第一轮：105 城广度优先")
        st.write("默认预算：每来源 10 分钟、30 列表页、300 文档、1 次附件尝试；达到上限写 checkpoint 并切换城市。")
        confirm = st.checkbox("我确认这是有界的 FAST_BULK_INGEST 操作", key="fast_bulk_confirm")
        max_cities = st.number_input("本次最多城市数（空/0 表示按配置）", min_value=0, max_value=105, value=0, step=1)
        if st.button("生成 FAST_BULK_INGEST 任务", disabled=settings.read_only or not confirm, type="primary"):
            _enqueue(settings, "fast_bulk_ingest", {"cities": [], "max_cities": int(max_cities) if max_cities else None}, confirm)
    with tabs[2]:
        options = _city_options(settings)
        if not options:
            st.info("城市注册表尚不可用。")
        else:
            labels = {f"{name} ({city_id})": city_id for city_id, name in options}
            selected = st.selectbox("城市", list(labels), key="dashboard_city")
            city_id = labels[selected]
            action = st.selectbox("操作", ["city_fast_ingest", "city_complete"], format_func=lambda value: "快速补抓该城市" if value == "city_fast_ingest" else "补齐该城市全部来源角色")
            role_label = st.selectbox("来源角色（仅补齐操作可选）", ["全部角色", *REQUIRED_ROLES], key="dashboard_city_role")
            confirm_city = st.checkbox("我确认将仅对所选城市执行有界操作", key="city_confirm")
            if st.button("生成城市任务", disabled=settings.read_only or not confirm_city):
                roles = [] if role_label == "全部角色" or action == "city_fast_ingest" else [role_label]
                _enqueue(settings, action, {"cities": [city_id], "source_roles": roles}, confirm_city)
            city_rows = city_role_matrix(settings)
            if not city_rows.is_empty():
                st.dataframe(safe_pandas(city_rows.filter(city_rows["city_id"] == city_id)), hide_index=True, width="stretch")
    with tabs[3]:
        jobs = list_jobs(settings)
        if jobs:
            st.dataframe(jobs, hide_index=True, width="stretch")
        else:
            st.info("尚无 Dashboard job。")
        st.caption("任务状态文件位于 D:\\Data Set\\CRPD\\control\\dashboard_jobs；历史文件不由 Dashboard 自动删除。")
    with tabs[4]:
        st.json({"allowed_actions": sorted(ALLOWED_ACTIONS), "bronze": "enabled", "silver": "enabled", "gold": "disabled_placeholder", "source_concurrency": 2, "document_concurrency": 6})
        st.markdown("""阶段：\n\n1. ROUND_1_FAST_COVERAGE\n2. ROUND_2_ROLE_COMPLETION\n3. ROUND_3_YEAR_COMPLETION\n4. ROUND_4_DEEP_BACKFILL\n5. ROUND_5_ATTACHMENTS\n6. ROUND_6_MANUAL_REVIEW""")
    with tabs[5]:
        st.subheader("覆盖完整性")
        st.caption("各指标保留分子、分母、定义和更新时间；综合指标仅用于展示排序，不作为抓取门禁。")
        kpis = data.get("kpis") or {}
        for key in ("cities_with_documents", "source_slots", "resolved_slots", "verified_slots", "enabled_slots", "partial_but_usable", "city_year_coverage"):
            item = kpis.get(key) or {}
            numerator = item.get("numerator")
            denominator = item.get("denominator")
            percent = item.get("percent")
            suffix = f" ({percent:.1%})" if isinstance(percent, (int, float)) else ""
            st.write(f"{item.get('label', key)}：{numerator}/{denominator}{suffix}")
            st.caption(item.get("definition", ""))
    with tabs[6]:
        st.subheader("PDF 发现、归档、解析与完整性")
        summary = pdf_summary(settings)
        columns = st.columns(5)
        for column, (label, key) in zip(columns, [("库存 PDF", "inventory_files"), ("有效资产", "valid_pdf_assets"), ("已关联", "linked_policy_pdf"), ("已下载", "downloaded"), ("已解析", "parsed")], strict=True):
            column.metric(label, int(summary.get(key) or 0))
        st.caption("PDF 作为 HTML 的补充或明确的 PDF 主文档保存；OCR 当前关闭，扫描件进入 OCR_PENDING，不会伪装成已解析。")
        limit = st.number_input("本次最多处理 PDF 记录数", min_value=1, max_value=100, value=20, step=1, key="pdf_job_limit")
        action = st.selectbox("PDF 操作", ["pdf_inventory", "pdf_archive", "pdf_discover", "pdf_download", "pdf_parse", "pdf_match", "pdf_run"], format_func=lambda value: {"pdf_inventory": "只读清单扫描", "pdf_archive": "归档现有 PDF", "pdf_discover": "从已抓取页面发现 PDF", "pdf_download": "有界直连下载", "pdf_parse": "PyMuPDF 解析", "pdf_match": "确定性关联/人工队列", "pdf_run": "执行一轮有界 PDF 流程"}[value], key="pdf_action")
        confirm_pdf = st.checkbox("我确认这是有界 PDF 操作", key="pdf_confirm")
        needs_confirmation = action != "pdf_inventory"
        if st.button("生成 PDF 任务", disabled=settings.read_only or (needs_confirmation and not confirm_pdf), type="primary", key="pdf_enqueue"):
            _enqueue(settings, action, {"cities": [], "limit": int(limit)}, confirm_pdf if needs_confirmation else True)
        st.markdown("#### 处理漏斗")
        st.dataframe(safe_pandas(pdf_processing_funnel(settings)), hide_index=True, width="stretch")
        st.markdown("#### 城市 PDF 关联")
        city_pdf = pdf_city_coverage(settings)
        if city_pdf.is_empty():
            st.info("暂未形成 PDF attachment 记录。")
        else:
            st.dataframe(safe_pandas(city_pdf), hide_index=True, width="stretch")
        failures = pdf_failures(settings)
        if not failures.is_empty():
            st.markdown("#### PDF 失败/待人工原因")
            st.dataframe(safe_pandas(failures), hide_index=True, width="stretch")
