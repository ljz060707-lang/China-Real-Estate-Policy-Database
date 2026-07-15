from __future__ import annotations

import getpass
import math
import os
from pathlib import Path

import streamlit as st

from policydb.review import (
    generate_review_tasks,
    list_review_tasks,
    review_stats,
    review_task_count,
    save_review_decision,
)
from policydb.settings import Settings

TYPE_LABELS = {
    "all": "全部",
    "missing_title": "标题问题",
    "missing_source": "原文/来源问题",
    "invalid_url": "链接问题",
    "low_confidence": "分类问题",
    "unmatched_t4": "T4问题",
    "unexplained_t2": "T2问题",
    "duplicate_record": "重复记录",
    "other": "其他",
}
STATUS_LABELS = {"pending": "待审核", "completed": "已完成", "all": "全部状态"}


@st.cache_data(ttl=30, max_entries=32, show_spinner=False)
def _cached_review_stats(root: str, database_stamp: int) -> dict:
    del database_stamp
    return review_stats(Settings.discover(root))


@st.cache_data(ttl=30, max_entries=64, show_spinner=False)
def _cached_review_count(
    root: str,
    review_type: str,
    status: str,
    database_stamp: int,
) -> int:
    del database_stamp
    return review_task_count(
        Settings.discover(root), review_type=review_type, status=status
    )


@st.cache_data(ttl=30, max_entries=64, show_spinner=False)
def _cached_review_tasks(
    root: str,
    review_type: str,
    status: str,
    limit: int,
    offset: int,
    database_stamp: int,
):
    del database_stamp
    return list_review_tasks(
        Settings.discover(root),
        review_type=review_type,
        status=status,
        limit=limit,
        offset=offset,
    )


def _act(
    task_id: str,
    decision: str,
    *,
    new_value: str | None,
    reviewer: str,
    review_note: str,
    evidence_url: str,
    settings: Settings,
) -> None:
    try:
        save_review_decision(
            task_id,
            decision,
            new_value=new_value,
            reviewer=reviewer.strip() or "local_user",
            review_note=review_note.strip() or None,
            evidence_url=evidence_url.strip() or None,
            settings=settings,
        )
    except (KeyError, ValueError) as error:
        st.error(str(error))
        return
    st.cache_data.clear()
    st.success("审核结果已保存")
    st.rerun()


def render_review_center(root: str | Path | None = None) -> None:
    settings = Settings.discover(root)
    read_only = os.getenv("POLICYDB_READ_ONLY", "0").lower() in {"1", "true", "yes"}
    st.title("人工审核中心")
    st.caption("Manual Review Center · 审核结果先记录，运行 review apply 后才更新 Curated 数据。")
    if read_only:
        st.info("当前为 GitHub 发布版（只读）。请在本地项目中完成审核和应用修正。")

    root_key = str(settings.root)
    database_stamp = settings.database.stat().st_mtime_ns
    stats = _cached_review_stats(root_key, database_stamp)
    type_counts = stats["review_type"]
    cards = [
        ("待审核任务", stats["pending"]),
        ("已完成", stats["completed"]),
        ("低置信分类", type_counts.get("low_confidence", 0)),
        ("链接问题", type_counts.get("invalid_url", 0)),
        ("T4 未关联", type_counts.get("unmatched_t4", 0)),
        ("T2 未解释", type_counts.get("unexplained_t2", 0)),
    ]
    for column, (label, value) in zip(st.columns(6), cards, strict=True):
        column.metric(label, int(value))

    filter_column, detail_column, action_column = st.columns([1.1, 3.2, 1.6])
    with filter_column:
        st.subheader("筛选")
        selected_label = st.selectbox("审核类型", list(TYPE_LABELS.values()))
        review_type = next(key for key, value in TYPE_LABELS.items() if value == selected_label)
        status_label = st.selectbox("状态", list(STATUS_LABELS.values()))
        status = next(key for key, value in STATUS_LABELS.items() if value == status_label)
        if st.button("重新扫描问题", use_container_width=True, disabled=read_only):
            result = generate_review_tasks(settings)
            st.cache_data.clear()
            st.success(f"扫描完成，本次新增 {result['created_total']} 条任务")
            st.rerun()
        st.caption("重复扫描不会覆盖已完成的审核结果。")

    page_size = 25
    total_tasks = _cached_review_count(
        root_key, review_type, status, database_stamp
    )
    total_pages = max(1, math.ceil(total_tasks / page_size))
    with filter_column:
        page_number = int(
            st.number_input(
                "页码",
                min_value=1,
                max_value=total_pages,
                value=1,
                step=1,
            )
        )
        st.caption(f"共 {total_tasks:,} 条 · 第 {page_number}/{total_pages} 页")

    tasks = _cached_review_tasks(
        root_key,
        review_type,
        status,
        page_size,
        (page_number - 1) * page_size,
        database_stamp,
    )
    if tasks.is_empty():
        with detail_column:
            st.info("当前筛选条件下没有审核任务。可点击“重新扫描问题”。")
        return

    rows = {row["task_id"]: row for row in tasks.iter_rows(named=True)}
    with filter_column:
        task_id = st.selectbox(
            "任务",
            list(rows),
            format_func=lambda value: (
                f"{TYPE_LABELS.get(rows[value]['review_type'], rows[value]['review_type'])} · "
                f"{rows[value].get('title') or rows[value].get('source_cell') or value}"
            ),
        )
    task = rows[task_id]

    with detail_column:
        st.subheader("当前任务")
        st.markdown(f"**{TYPE_LABELS.get(task['review_type'], task['review_type'])}**")
        meta_left, meta_right = st.columns(2)
        meta_left.text_input("record_id", task.get("record_id") or "—", disabled=True)
        meta_right.text_input("状态", task.get("status") or "—", disabled=True)
        st.text_input("标题", task.get("title") or "—", disabled=True)
        date_source_left, date_source_right = st.columns(2)
        date_source_left.text_input(
            "日期", str(task.get("record_date") or "—"), disabled=True
        )
        date_source_right.text_input(
            "来源工作表", task.get("source_sheet") or "—", disabled=True
        )
        st.text_input("来源单元格", task.get("source_cell") or "—", disabled=True)
        st.text_area("原始值", task.get("old_value") or "", height=120, disabled=True)
        st.text_area("建议值", task.get("suggested_value") or "", height=90, disabled=True)
        if task.get("summary"):
            with st.expander("上下文信息"):
                st.write(task["summary"])
        evidence = task.get("evidence_url") or task.get("primary_source_url")
        if evidence:
            st.code(str(evidence), language=None)
            if str(evidence).startswith(("http://", "https://")):
                st.link_button("打开证据链接", str(evidence))

    with action_column:
        st.subheader("审核操作")
        reviewer = st.text_input(
            "审核人", value=getpass.getuser() or "local_user", disabled=read_only
        )
        new_value = st.text_area(
            "修改后的值",
            value=task.get("suggested_value") or task.get("old_value") or "",
            height=120,
            disabled=read_only,
        )
        evidence_url = st.text_input(
            "证据链接", value=task.get("evidence_url") or "", disabled=read_only
        )
        review_note = st.text_area("审核备注", height=90, disabled=read_only)
        if st.button(
            "确认正确", type="primary", use_container_width=True, disabled=read_only
        ):
            _act(
                task_id,
                "approved",
                new_value=None,
                reviewer=reviewer,
                review_note=review_note,
                evidence_url=evidence_url,
                settings=settings,
            )
        if st.button("保存修改", use_container_width=True, disabled=read_only):
            if not new_value.strip():
                st.error("请输入修改后的值")
            else:
                _act(
                    task_id,
                    "corrected",
                    new_value=new_value,
                    reviewer=reviewer,
                    review_note=review_note,
                    evidence_url=evidence_url,
                    settings=settings,
                )
        if st.button("拒绝", use_container_width=True, disabled=read_only):
            _act(
                task_id,
                "rejected",
                new_value=None,
                reviewer=reviewer,
                review_note=review_note,
                evidence_url=evidence_url,
                settings=settings,
            )
        if st.button("暂不处理", use_container_width=True, disabled=read_only):
            _act(
                task_id,
                "ignored",
                new_value=None,
                reviewer=reviewer,
                review_note=review_note,
                evidence_url=evidence_url,
                settings=settings,
            )
