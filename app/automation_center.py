from __future__ import annotations

import json
from pathlib import Path

import polars as pl
import streamlit as st

from app.ui import safe_dataframe
from policydb.coverage import build_city_source_month_coverage
from policydb.jobs import JobManager
from policydb.schedule import schedule_status
from policydb.settings import Settings
from policydb.update.v2 import start_update


def render_automation_center(root: str | Path) -> None:
    settings = Settings.discover(root)
    status = schedule_status()
    coverage_path = settings.root / "outputs/coverage/city_source_month_coverage.csv"
    coverage = pl.read_csv(coverage_path) if coverage_path.exists() else pl.DataFrame()
    complete = (
        coverage.filter(pl.col("coverage_status").str.starts_with("complete_")).height
        if coverage.height
        else 0
    )
    active_jobs = sum(
        state.status not in {"completed", "completed_with_warnings", "failed", "cancelled"}
        for state in JobManager(settings).list_states()
    )
    for column, (label, value) in zip(
        st.columns(5),
        [
            ("Windows计划", "已安装" if status["all_installed"] else "未完整安装"),
            ("覆盖单元", coverage.height),
            ("完整窗口", complete),
            ("缺口单元", coverage.height - complete),
            ("当前后台任务", active_jobs),
        ],
        strict=True,
    ):
        column.metric(label, value)
    st.caption("未扫描、部分扫描和失败不记为零政策；只有完整分页证据才允许确认零政策。")
    if settings.read_only:
        st.warning("当前为只读公开部署，不能启动更新或修改 Windows 任务计划。")
    actions = st.columns(4)
    for column, (label, layer) in zip(
        actions[:3],
        [
            ("立即运行每日更新", "daily"),
            ("立即运行周度补漏", "weekly"),
            ("立即运行月度完整性检查", "monthly"),
        ],
        strict=True,
    ):
        if column.button(label, width="stretch", disabled=settings.read_only):
            try:
                result = start_update(layer, settings)
                st.success(f"后台任务已启动：{result['job_id']}")
            except Exception as exc:
                st.error(f"任务创建失败：{type(exc).__name__}")
    if actions[3].button("重建覆盖报告", width="stretch", disabled=settings.read_only):
        result = build_city_source_month_coverage(settings)
        st.success(json.dumps(result, ensure_ascii=False))
        st.rerun()
    st.subheader("计划任务状态")
    safe_dataframe(
        [
            {"layer": layer, **value}
            for layer, value in status["tasks"].items()
        ]
    )
    st.subheader("105 城覆盖缺口")
    if coverage.height:
        gaps = (
            coverage.filter(~pl.col("coverage_status").str.starts_with("complete_"))
            .group_by(["province_name", "city_name"])
            .len(name="gap_cells")
            .sort("gap_cells", descending=True)
        )
        safe_dataframe(gaps, height=420)
        st.download_button(
            "下载覆盖矩阵 CSV",
            coverage.write_csv().encode("utf-8-sig"),
            "city_source_month_coverage.csv",
        )
    else:
        st.info("尚未生成覆盖矩阵。点击“重建覆盖报告”或运行 coverage build。")
