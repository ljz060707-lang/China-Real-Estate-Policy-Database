from __future__ import annotations

import math
from datetime import date
from typing import Any

import plotly.express as px
import polars as pl
import streamlit as st

from app.theme import NANJING_PURPLE, style_plotly_figure
from app.ui import (
    freshness_caption,
    render_progress_metric,
    render_status_strip,
    safe_dataframe,
    safe_pandas,
)
from policydb.config.secret_store import default_secret_store
from policydb.dashboard_formatting import (
    format_count,
    format_datetime,
    format_percentage,
    format_source_role,
    format_stage,
    format_status,
    format_value,
)
from policydb.dashboard_jobs import enqueue_job, list_jobs
from policydb.dashboard_live_state import DashboardSnapshot, load_dashboard_snapshot
from policydb.dashboard_policy_data import DashboardPolicyData
from policydb.settings import Settings

REFRESH_SECONDS = 20


def _tone(status: str | None) -> str:
    if status in {
        "fresh",
        "healthy",
        "available",
        "HEALTHY",
        "RUNNING",
        "SUCCESS",
        "READY_FOR_NEXT_STAGE",
        "COMPLETE",
        "operational",
    }:
        return "good"
    if status in {
        "warning",
        "stale",
        "WAIT_CURRENT_RUN",
        "RETRY_WAIT",
        "UNKNOWN",
        "HUMAN_REVIEW",
        "DATABASE_UPDATING",
        "INDEX_REFRESH_PENDING",
        "CURATED_FALLBACK",
    }:
        return "warn"
    if status in {
        "failed",
        "FAILED",
        "BLOCKED",
        "unavailable",
        "QUERY_UNAVAILABLE",
    }:
        return "bad"
    return ""


def _snapshot(settings: Settings) -> DashboardSnapshot:
    return load_dashboard_snapshot(settings, event_limit=20)


def _status_strip(snapshot: DashboardSnapshot) -> None:
    crawler = snapshot.crawler
    system = snapshot.system
    if system.get("snapshot_status") == "LAST_GOOD":
        st.warning(
            "数据正在更新，当前展示上一份成功快照。实时抓取状态仍以 MASTER_STATE 为准。"
        )
    period = (
        "—".join(value for value in (crawler.get("start_date"), crawler.get("end_date")) if value)
        or "暂无数据"
    )
    render_status_strip(
        [
            ("系统状态", format_status(crawler.get("status")), _tone(crawler.get("status"))),
            (
                "当前阶段",
                format_stage(crawler.get("stage")),
                _tone(crawler.get("heartbeat_status")),
            ),
            ("当前城市", format_value(crawler.get("city_name")), ""),
            ("时间分片", period, ""),
            ("来源角色", format_source_role(crawler.get("source_role")), ""),
            (
                "最近心跳",
                format_datetime(crawler.get("last_heartbeat_at"), include_seconds=False),
                _tone(crawler.get("heartbeat_status")),
            ),
        ]
    )
    if snapshot.coverage.get("enabled_unverified"):
        st.markdown(
            f'<div class="crpd-alert">来源门控需要关注：当前有 {snapshot.coverage["enabled_unverified"]} 个已启用槽位缺少同步的严格核验标记。Dashboard 仅展示该事实，不会自动修改来源注册表。</div>',
            unsafe_allow_html=True,
        )
    database = system.get("database") or {}
    if database.get("status") == "CURATED_FALLBACK":
        st.info(
            "正式数据库索引正在更新或仍引用旧数据路径；实时监控继续使用 E 盘清洗层快照，政策中心进入只读降级模式。"
        )
    elif database.get("status") in {
        "DATABASE_UPDATING",
        "INDEX_REFRESH_PENDING",
        "QUERY_UNAVAILABLE",
    }:
        st.warning(
            f"正式数据库状态：{database.get('status')}。Dashboard 保持只读，实时事件与政策检索不会写入数据库。"
        )


def _translated_events(frame: pl.DataFrame) -> pl.DataFrame:
    if frame.is_empty():
        return frame
    rows = []
    for row in frame.to_dicts():
        start, end = row.get("start_date"), row.get("end_date")
        rows.append(
            {
                "时间（北京时间）": format_datetime(row.get("created_at")),
                "城市": format_value(row.get("city_name")),
                "来源角色": format_source_role(row.get("source_role")),
                "阶段": format_stage(row.get("stage")),
                "时间范围": f"{start} 至 {end}" if start and end else "暂无数据",
                "结果摘要": format_value(row.get("message")),
            }
        )
    return pl.DataFrame(rows, infer_schema_length=None)


def _render_metric_row(snapshot: DashboardSnapshot) -> None:
    metrics = snapshot.coverage["metrics"]
    columns = st.columns(5)
    with columns[0]:
        st.metric("政策记录", format_count(snapshot.documents.get("records")))
        st.caption("正式清洗层的唯一记录数，不等同于 HTTP 抓取次数。")
    with columns[1]:
        st.metric("文档版本", format_count(snapshot.documents.get("document_versions")))
        st.caption("已落盘的 Bronze 文档版本数。")
    with columns[2]:
        render_progress_metric(metrics["city_live_progress"])
    with columns[3]:
        render_progress_metric(metrics["verified_slots"])
    with columns[4]:
        render_progress_metric(metrics["enabled_slots"])


def _render_stage_progress(snapshot: DashboardSnapshot) -> None:
    progress = snapshot.system.get("progress_snapshot") or {}
    recent = progress.get("recent_30d") or {}
    rolling = progress.get("rolling_24m") or {}
    historical = progress.get("historical") or {}
    st.subheader("分阶段抓取进度")
    columns = st.columns(3)
    with columns[0]:
        st.metric("最近 30 天", f"{recent.get('completed', 0)} / {recent.get('total', 0)}")
        st.caption(f"已检查城市：{recent.get('cities_checked', 0)} / 105；来源缺失：{recent.get('source_incomplete', 0)}")
    with columns[1]:
        completed = rolling.get("completed")
        total = rolling.get("total")
        label = f"{completed} / {total}" if completed is not None and total is not None else "暂无队列"
        st.metric("近 24 个月", label)
        st.caption(f"已检查城市：{rolling.get('cities_checked', 0)} / {rolling.get('cities_total', 105)}；来源缺失：{rolling.get('source_incomplete', 0)}")
    with columns[2]:
        st.metric("历史全量待处理", format_value(historical.get("pending_shards")))
        st.caption(f"来源不完整：{format_value(historical.get('source_incomplete'))}；可重试：{format_value(historical.get('retryable'))}")
    stage = progress.get("stage") or snapshot.crawler.get("stage")
    if stage:
        st.caption(f"权威阶段：{format_stage(stage)} · 最近真实进度：{format_datetime(progress.get('last_real_progress_at'))}")
    position = " · ".join(
        str(value)
        for value in (
            progress.get("current_city"),
            progress.get("current_source"),
            (progress.get("current_window") or {}).get("rolling_start") if str(stage or "").startswith("ROLLING") else (progress.get("current_window") or {}).get("recent_start"),
            (progress.get("current_window") or {}).get("rolling_end") if str(stage or "").startswith("ROLLING") else (progress.get("current_window") or {}).get("recent_end"),
        )
        if value
    )
    if position:
        st.caption(f"当前抓取位置：{position}")
    watchdog = snapshot.system.get("progress_watchdog") or {}
    if watchdog.get("status") == "NO_REAL_PROGRESS":
        st.warning("检测到超过阈值未产生真实业务进展；活动 worker 不会被强杀，系统将在安全边界复用恢复路径。")


@st.fragment(run_every=f"{REFRESH_SECONDS}s")
def render_overview_page(settings: Settings) -> None:
    snapshot = _snapshot(settings)
    _status_strip(snapshot)
    _render_stage_progress(snapshot)
    _render_metric_row(snapshot)

    left, right = st.columns([1.08, 1], gap="large")
    with left:
        st.subheader("当前任务")
        progress = snapshot.coverage["metrics"]["batch_shard_progress"]
        if progress.value is not None:
            st.progress(min(1.0, max(0.0, progress.value)))
        st.write(f"{progress.label}：{format_percentage(progress.numerator, progress.denominator)}")
        st.caption(progress.definition)
        current_source = format_value(
            snapshot.crawler.get("source_name")
            or format_source_role(snapshot.crawler.get("source_role"))
        )
        if len(current_source) > 64:
            current_source = current_source[:64] + "…"
        details = [
            {
                "项目": "当前来源",
                "值": current_source,
            },
            {"项目": "worker PID", "值": format_value(snapshot.crawler.get("worker_pid"))},
            {"项目": "runner PID", "值": format_value(snapshot.crawler.get("runner_pid"))},
            {
                "项目": "累计 HTTP 成功",
                "值": format_count(snapshot.crawler.get("total_fetched_requests")),
            },
            {
                "项目": "累计 HTTP 失败",
                "值": format_count(snapshot.crawler.get("total_failed_requests")),
            },
        ]
        safe_dataframe(details, height=260)
    with right:
        st.subheader("覆盖与缺口")
        coverage = snapshot.coverage
        rows = [
            {
                "指标": "已解决槽位",
                "结果": format_percentage(
                    coverage.get("resolved_slots"), coverage.get("total_slots")
                ),
            },
            {
                "指标": "严格核验槽位",
                "结果": format_percentage(
                    coverage.get("verified_slots"), coverage.get("total_slots")
                ),
            },
            {
                "指标": "已启用槽位",
                "结果": format_percentage(
                    coverage.get("enabled_slots"), coverage.get("total_slots")
                ),
            },
            {
                "指标": "有政策文档的城市",
                "结果": format_percentage(snapshot.documents.get("cities_with_documents"), 105),
            },
            {"指标": "开放缺口", "结果": format_count(coverage.get("open_gaps"))},
            {"指标": "高严重度缺口", "结果": format_count(coverage.get("critical_gaps"))},
        ]
        safe_dataframe(rows, height=260)
        st.caption("严格完整性用于审计；它不会被当作当前抓取任务的实时百分比。")

    st.subheader("数据增长与城市推进")
    trend_col, city_col = st.columns([1.15, 1], gap="large")
    with trend_col:
        records = snapshot.frames["records"]
        if not records.is_empty() and "record_date" in records.columns:
            trend = (
                records.filter(pl.col("record_date").is_not_null())
                .with_columns(pl.col("record_date").dt.truncate("1mo").alias("月份"))
                .group_by("月份")
                .agg(pl.len().alias("政策数"))
                .sort("月份")
            )
            figure = px.area(
                safe_pandas(trend),
                x="月份",
                y="政策数",
                title="Curated 政策记录（月度）",
                color_discrete_sequence=[NANJING_PURPLE],
            )
            figure.update_traces(line={"width": 1.8}, fillcolor="rgba(95,0,128,.10)")
            st.plotly_chart(style_plotly_figure(figure, height=330), width="stretch")
        else:
            st.info("暂无可绘制的政策记录趋势。")
    with city_col:
        city_frame = snapshot.frames["city_progress"]
        if city_frame.is_empty():
            st.info("暂无城市分片计划。")
        else:
            display = city_frame.with_columns(
                pl.struct(["processed_shards", "planned_shards"])
                .map_elements(
                    lambda row: format_percentage(row["processed_shards"], row["planned_shards"]),
                    return_dtype=pl.String,
                )
                .alias("分片进度"),
                pl.col("city_status")
                .map_elements(format_status, return_dtype=pl.String)
                .alias("状态"),
            ).select(pl.col("city_name").alias("城市"), "状态", "分片进度")
            safe_dataframe(display.head(20), height=330)

    st.subheader("最近 20 条流水线事件")
    safe_dataframe(_translated_events(snapshot.frames["recent_events"]), height=430)
    freshness_caption(snapshot.crawler.get("latest_event_at"), "pipeline_progress_events.parquet")


def _policy_filters(service: DashboardPolicyData) -> tuple[dict[str, Any], bool]:
    options = service.filter_options()
    with st.form("policy_search_form", border=False):
        row1 = st.columns([1, 1, 1, 1, 1.5])
        start_text = row1[0].text_input("开始日期", value="2018-01-01", placeholder="YYYY-MM-DD")
        end_text = row1[1].text_input(
            "结束日期", value=date.today().isoformat(), placeholder="YYYY-MM-DD"
        )
        province = row1[2].selectbox("省份", ["全部", *options.get("provinces", [])])
        city = row1[3].selectbox("城市", ["全部", *options.get("cities", [])])
        keyword = row1[4].text_input("关键词", placeholder="政策标题或摘要")
        with st.expander("更多筛选", expanded=False):
            row2 = st.columns(5)
            primary = row2[0].selectbox("一级分类", ["全部", *options.get("primary", [])])
            direction = row2[1].selectbox("政策方向", ["全部", *options.get("directions", [])])
            instrument = row2[2].selectbox("政策工具", ["全部", *options.get("instruments", [])])
            official = row2[3].selectbox("官方状态", ["全部", *options.get("statuses", [])])
            pdf = row2[4].selectbox("PDF", ["全部", "有 PDF", "无 PDF"])
            episodes = options.get("episodes", [])
            episode_labels = ["全部", *[f"{item['episode_id']} · {item['episode_name']}" for item in episodes]]
            episode_label = st.selectbox("历史 Episode", episode_labels)
            episode_id = None if episode_label == "全部" else episode_label.split(" · ", 1)[0]
        if "episode_id" not in locals():
            episode_id = None
        submitted = st.form_submit_button("查询政策", type="primary")
    try:
        start = date.fromisoformat(start_text)
        end = date.fromisoformat(end_text)
    except ValueError:
        st.error("日期格式应为 YYYY-MM-DD；本次查询使用 2018-01-01 至今天。")
        start, end = date(2018, 1, 1), date.today()
    # Keep the current-data default for the normal policy center, but do not
    # hide a selected historical episode behind that explicit date filter.
    # A user-entered date still takes precedence over this compatibility rule.
    if episode_id and start_text.strip() == "2018-01-01":
        start = date(2016, 1, 1)
    return {
        "start_date": start,
        "end_date": end,
        "province": None if province == "全部" else province,
        "city": None if city == "全部" else city,
        "cities": [] if city == "全部" else [city],
        "keyword": keyword.strip() or None,
        "primary_category_code": None if primary == "全部" else primary,
        "direction": None if direction == "全部" else direction,
        "instrument_type": None if instrument == "全部" else instrument,
        "official_status": None if official == "全部" else official,
        "has_pdf": None if pdf == "全部" else pdf == "有 PDF",
        "episode_id": episode_id,
    }, submitted


@st.fragment(run_every=f"{REFRESH_SECONDS}s")
def render_policy_center_page(settings: Settings) -> None:
    service = DashboardPolicyData(settings)
    if service.mode == "curated_fallback":
        st.warning(
            "正式 DuckDB 外部视图当前不可查询。政策中心正在读取同一 E 盘 Curated 数据的只读索引；数据库恢复后会自动切回正式查询。"
        )
    elif service.degraded:
        st.warning(
            "正式 DuckDB 的部分辅助查询暂时不可用；政策中心已使用 E 盘 Curated 只读索引继续展示，抓取任务不受影响。"
        )
    if service.used_last_good_snapshot:
        st.warning("Curated 政策快照正在更新，当前展示上一份成功的政策索引。")
    filters, submitted = _policy_filters(service)
    if submitted:
        st.session_state["policy_filters"] = filters
        st.session_state["policy_page"] = 1
    active_filters = st.session_state.get("policy_filters", filters)
    page_size = st.select_slider("每页记录数", options=[20, 50, 100], value=50)
    page = int(st.session_state.get("policy_page", 1))
    try:
        frame, total = service.search(active_filters, page=page, page_size=page_size)
    except Exception:
        st.error("政策索引暂时不可读。抓取任务可能正在替换数据快照，请稍后刷新。")
        return
    pages = max(1, math.ceil(total / page_size))
    page = min(page, pages)
    st.caption(
        f"共 {total:,} 条政策记录 · 第 {page} / {pages} 页 · 数据模式：{ {'duckdb': '正式 DuckDB', 'mixed': '正式 DuckDB + E 盘 Curated 只读降级', 'curated_fallback': 'E 盘 Curated 只读降级', 'unavailable': '不可用'}.get(service.display_mode, service.display_mode) }"
    )
    if frame.is_empty():
        st.info("当前筛选条件下暂无政策记录。")
        return
    columns = [
        name
        for name in (
            "record_id",
            "record_date",
            "title",
            "province",
            "city",
            "primary_category_code",
            "direction",
            "official_status",
            "has_pdf",
        )
        if name in frame.columns
    ]
    labels = {
        "record_id": "记录 ID",
        "record_date": "发布日期",
        "title": "政策标题",
        "province": "省份",
        "city": "城市",
        "primary_category_code": "一级分类",
        "direction": "政策方向",
        "official_status": "官方状态",
        "has_pdf": "PDF",
    }
    safe_dataframe(
        frame.select(columns).rename({name: labels[name] for name in columns}),
        height=430,
    )
    navigation = st.columns([1, 1, 4])
    if navigation[0].button("上一页", disabled=page <= 1):
        st.session_state["policy_page"] = page - 1
        st.rerun()
    if navigation[1].button("下一页", disabled=page >= pages):
        st.session_state["policy_page"] = page + 1
        st.rerun()
    csv = frame.select(columns).write_csv().encode("utf-8-sig")
    navigation[2].download_button(
        "导出当前页 CSV", csv, file_name="crpd_policy_page.csv", mime="text/csv"
    )

    if active_filters.get("episode_id"):
        from policydb.episode_930 import episode_930_actions_for_dashboard

        episode_actions = episode_930_actions_for_dashboard(
            service.settings,
            episode_id=str(active_filters["episode_id"]),
        )
        if not episode_actions.is_empty():
            st.subheader("Episode 动作级导出")
            st.caption(
                "该导出保留 announcement/publication/effective/implementation 日期、机制标签和官方证据；AI 字段仍是 advisory。"
            )
            export_columns = [
                column
                for column in (
                    "episode_id",
                    "city",
                    "document_id",
                    "action_id",
                    "policy_type",
                    "policy_subtype",
                    "mechanism_labels",
                    "action_direction",
                    "announcement_date",
                    "publication_date",
                    "effective_date",
                    "implementation_date",
                    "old_value",
                    "new_value",
                    "unit",
                    "official_url",
                    "source_confidence",
                    "date_confidence",
                    "classification_confidence",
                )
                if column in episode_actions.columns
            ]
            safe_dataframe(episode_actions.select(export_columns).head(100), height=280)
            navigation[2].download_button(
                "导出 Episode 动作级 CSV",
                episode_actions.select(export_columns).write_csv().encode("utf-8-sig"),
                file_name=f"{active_filters['episode_id']}_action_export.csv",
                mime="text/csv",
            )

    choices = {
        f"{row.get('record_date') or '日期未标注'} · {row.get('title') or '标题未标注'}": row[
            "record_id"
        ]
        for row in frame.to_dicts()
    }
    selected = st.selectbox("查看政策详情", ["请选择", *choices])
    if selected == "请选择":
        return
    detail = service.detail(choices[selected])
    policy = detail.get("policy") or {}
    st.subheader(format_value(policy.get("title")))
    st.caption(
        f"{format_value(policy.get('record_date'))} · {format_value(policy.get('official_status'))}"
    )
    tabs = st.tabs(["政策原文", "政策动作", "来源与版本", "附件"])
    with tabs[0]:
        st.text_area("正文", value=format_value(policy.get("full_text")), height=420, disabled=True)
    with tabs[1]:
        safe_dataframe(detail.get("actions"), height=360)
        st.caption("AI 或规则抽取结果属于可审计候选，不作为未经复核的事实标签。")
    with tabs[2]:
        safe_dataframe(detail.get("versions"), height=360)
    with tabs[3]:
        safe_dataframe(detail.get("files"), height=360)


def _city_role_matrix(slots: pl.DataFrame) -> pl.DataFrame:
    if slots.is_empty():
        return slots
    rows = []
    for row in slots.to_dicts():
        rows.append(
            {
                "省份": row.get("province_name"),
                "城市": row.get("city_name"),
                "来源角色": format_source_role(row.get("source_role")),
                "状态": format_status(row.get("status")),
                "候选": row.get("candidate_count"),
                "已核验": row.get("verified_candidate_count"),
                "已启用": row.get("enabled_source_count"),
            }
        )
    return pl.DataFrame(rows, infer_schema_length=None)


def _year_matrix(frame: pl.DataFrame) -> pl.DataFrame:
    if frame.is_empty():
        return frame
    return frame.select(
        pl.col("city_name").alias("城市"),
        pl.col("year").alias("年份"),
        pl.col("status")
        .map_elements(format_status, return_dtype=pl.String)
        .alias("严格完整性状态"),
        pl.col("overall_completion_pct").alias("严格门控完成度（%）"),
        pl.col("shard_count").alias("分片数"),
        pl.col("updated_at")
        .map_elements(format_datetime, return_dtype=pl.String)
        .alias("更新时间"),
    )


def _enqueue_operation(
    settings: Settings, action: str, scope: dict[str, Any], confirmed: bool
) -> None:
    try:
        job = enqueue_job(settings, action, scope, confirmed=confirmed)
    except Exception as exc:
        st.error(f"任务未创建：{type(exc).__name__}。请检查是否已有活动任务或参数是否有效。")
    else:
        st.success(f"已创建结构化任务 {job['job_id']}，等待独立 operations worker 执行。")


@st.fragment(run_every=f"{REFRESH_SECONDS}s")
def render_collection_page(settings: Settings) -> None:
    snapshot = _snapshot(settings)
    _status_strip(snapshot)
    episode = snapshot.system.get("episode_930_progress") or {}
    if episode:
        st.subheader("2016 年 930 楼市调控潮专项")
        episode_columns = st.columns(5)
        episode_columns[0].metric("专项状态", format_value(episode.get("status")))
        episode_columns[1].metric("当前阶段", format_value(episode.get("stage")))
        episode_columns[2].metric("文档", format_count(episode.get("documents_found")))
        episode_columns[3].metric("动作", format_count(episode.get("actions_extracted")))
        episode_columns[4].metric("正式动作", format_count(episode.get("formal_actions_promoted")))
        st.info(
            "总体状态："
            f"{format_value(episode.get('status'))} · "
            "上一 micro-batch："
            f"{format_value(episode.get('last_micro_batch_status'))} · "
            "下一批："
            f"{format_value(episode.get('next_batch_status'))}"
        )
        safe_dataframe(
            [
                {"指标": "Queue", "值": f"{format_count(episode.get('queue_completed'))} / {format_count(episode.get('queue_total'))}"},
                {"指标": "API Pass1 / Pass2", "值": f"{format_count(episode.get('api_pass1_success'))} / {format_count(episode.get('api_pass2_success'))}"},
                {"指标": "API失败 / Deferred", "值": f"{format_count(episode.get('api_failed'))} / {format_count(episode.get('api_deferred'))}"},
                {"指标": "API当前状态 / 余额状态", "值": f"{format_value(episode.get('api_provider_status') or episode.get('api_status'))} / {format_value(episode.get('api_balance_status'))}"},
                {"指标": "API Recovery Queue", "值": format_value(episode.get('api_recovery_queue'))},
                {"指标": "生效日期证据", "值": format_value(episode.get("effective_date_metrics"))},
                {"指标": "Gap分类", "值": format_value(episode.get("gap_type_counts"))},
                {"指标": "附件状态", "值": format_value(episode.get("attachment_status"))},
                {"指标": "AUTORUN lock", "值": format_value((episode.get("autorun") or {}).get("lock_present"))},
                {"指标": "Active worker/fetch/writer", "值": f"{format_value((episode.get('autorun') or {}).get('active_worker'))} / {format_value((episode.get('autorun') or {}).get('active_fetch'))} / {format_value((episode.get('autorun') or {}).get('active_writer'))}"},
                {"指标": "Runner PID", "值": format_value((episode.get("autorun") or {}).get("runner_pid"))},
            ],
            height=320,
        )
        st.caption(
            "930 专项仅展示真实 production snapshot；heartbeat 不计为业务进度。"
            f" 最近真实进度：{format_datetime(episode.get('last_real_progress_at'))}"
        )
    tabs = st.tabs(["实时采集", "处理流水线", "城市×来源角色", "城市×年份", "运行历史", "操作中心"])
    with tabs[0]:
        progress = snapshot.coverage["metrics"]["batch_shard_progress"]
        columns = st.columns(4)
        with columns[0]:
            render_progress_metric(progress)
        columns[1].metric(
            "HTTP 成功次数", format_count(snapshot.crawler.get("total_fetched_requests"))
        )
        columns[2].metric(
            "HTTP 失败次数", format_count(snapshot.crawler.get("total_failed_requests"))
        )
        columns[3].metric("当前 worker PID", format_value(snapshot.crawler.get("worker_pid")))
        live = snapshot.system.get("automation_live_state") or {}
        st.caption(
            "MASTER_STATE authoritative state: status, stage, run_id, worker PID, and heartbeat "
            "come from the autonomous controller; pipeline_progress_events is only position evidence."
        )
        safe_dataframe(
            [
                {"field": "automation_id", "value": format_value(live.get("automation_id"))},
                {"field": "status", "value": format_status(live.get("status"))},
                {"field": "raw_status", "value": format_value(live.get("raw_status"))},
                {"field": "stage", "value": format_stage(live.get("stage"))},
                {"field": "run_id", "value": format_value(live.get("run_id"))},
                {"field": "next_stage", "value": format_stage(live.get("next_stage"))},
                {
                    "field": "current_live_position",
                    "value": "available"
                    if live.get("current_position_available")
                    else "unavailable; showing last position",
                },
                {
                    "field": "MASTER_STATE heartbeat",
                    "value": format_datetime(live.get("last_heartbeat_at")),
                },
            ],
            height=300,
        )
        last_position = snapshot.crawler.get("last_crawl_position") or {}
        st.caption(
            "Last crawl position (not current work): "
            f"{format_value(last_position.get('city_name'))} / "
            f"{format_value(last_position.get('source_role'))} / "
            f"{format_value(last_position.get('start_date'))}-"
            f"{format_value(last_position.get('end_date'))}"
        )
        safe_dataframe(_translated_events(snapshot.frames["recent_events"]), height=470)
    with tabs[1]:
        versions = snapshot.documents.get("document_versions")
        quality = snapshot.quality
        rows = [
            {
                "处理阶段": "发现与抓取",
                "数量": snapshot.crawler.get("total_fetched_requests"),
                "说明": "累计成功 HTTP 抓取次数，可能包含重复候选",
            },
            {
                "处理阶段": "文档版本落盘",
                "数量": versions,
                "说明": "policy_document_versions 正式行数",
            },
            {
                "处理阶段": "正文可用",
                "数量": max(0, int(quality.get("total", 0)) - int(quality.get("missing_text", 0))),
                "说明": "正式记录中正文非空",
            },
            {
                "处理阶段": "AI 抽取",
                "数量": snapshot.ai.get("extractions"),
                "说明": "当前全量回溯禁用 AI，不会伪造进度",
            },
            {
                "处理阶段": "PDF 已解析",
                "数量": snapshot.archive.get("parsed_pdf_assets"),
                "说明": "text_char_count > 0 的有效 PDF",
            },
        ]
        safe_dataframe(rows, height=340)
        st.caption("各阶段分母不同，页面不把它们拼成一个不透明的总完成度。")
    with tabs[2]:
        matrix = _city_role_matrix(snapshot.frames["source_slots"])
        provinces = (
            sorted(matrix.get_column("省份").drop_nulls().unique().to_list())
            if not matrix.is_empty()
            else []
        )
        selected = st.selectbox("省份筛选", ["全部", *provinces], key="collection_province")
        if selected != "全部":
            matrix = matrix.filter(pl.col("省份") == selected)
        safe_dataframe(matrix, height=560)
        st.caption("分母固定为 105 城 × 5 类来源角色 = 525 个必需槽位。")
    with tabs[3]:
        st.warning("此处展示严格 city-year 完整性门控，不代表当前 crawler 的实时执行百分比。")
        years = _year_matrix(snapshot.frames["city_year_progress"])
        safe_dataframe(years, height=560)
    with tabs[4]:
        runs = snapshot.frames["crawl_runs"]
        if runs.is_empty():
            st.info("暂无运行历史。")
        else:
            selected = [
                name
                for name in (
                    "run_id",
                    "run_type",
                    "scope_id",
                    "period_start",
                    "period_end",
                    "status",
                    "item_count",
                    "fetched_count",
                    "failed_count",
                    "started_at",
                    "finished_at",
                )
                if name in runs.columns
            ]
            display = runs.sort("started_at", descending=True).head(50).select(selected)
            safe_dataframe(display, height=500)
    with tabs[5]:
        st.caption("Dashboard 只写入经过校验的 JSON job request，不执行任意 shell 命令。")
        if snapshot.crawler.get("running"):
            st.warning("当前已有全量历史回溯在运行。为避免第二个 writer，新的抓取任务按钮已禁用。")
        action = st.selectbox("预定义操作", ["刷新覆盖统计", "生成研究快照", "启动 105 城快速覆盖"])
        confirm = st.checkbox("我确认操作范围和影响", key="operation_confirm")
        action_map = {
            "刷新覆盖统计": "refresh_metrics",
            "生成研究快照": "research_snapshot",
            "启动 105 城快速覆盖": "fast_bulk_ingest",
        }
        write_action = action_map[action]
        disabled = (
            settings.read_only
            or snapshot.crawler.get("running")
            or (write_action == "fast_bulk_ingest" and not confirm)
        )
        if st.button("创建任务", type="primary", disabled=disabled):
            _enqueue_operation(
                settings,
                write_action,
                {"cities": []},
                confirm or write_action != "fast_bulk_ingest",
            )
        jobs = list_jobs(settings)
        if jobs:
            safe_dataframe(
                [
                    {
                        "任务 ID": row.get("job_id"),
                        "操作": row.get("action"),
                        "状态": format_status(row.get("status")),
                        "申请时间": format_datetime(row.get("requested_at")),
                    }
                    for row in jobs[:20]
                ],
                height=320,
            )


def render_quality_page(settings: Settings) -> None:
    snapshot = _snapshot(settings)
    quality = snapshot.quality
    coverage = snapshot.coverage
    columns = st.columns(6)
    for column, (label, value) in zip(
        columns,
        [
            ("缺标题", quality.get("missing_title")),
            ("缺日期", quality.get("missing_date")),
            ("缺正文", quality.get("missing_text")),
            ("正文过短", quality.get("short_text")),
            ("重复 URL", quality.get("duplicate_url")),
            ("重复内容 hash", quality.get("duplicate_hash")),
        ],
        strict=True,
    ):
        column.metric(label, format_count(value))
    tabs = st.tabs(["完整性分项", "城市覆盖", "年份覆盖", "来源覆盖", "缺口登记", "错误 Pareto"])
    with tabs[0]:
        total = quality.get("total") or 0
        rows = []
        definitions = {
            "missing_title": "正式记录中标题不为空",
            "missing_date": "正式记录中发布日期不为空",
            "missing_text": "正式记录中正文不为空",
            "missing_source": "正式记录中来源 URL 不为空",
        }
        for label, missing_key in (
            ("标题完整率", "missing_title"),
            ("日期完整率", "missing_date"),
            ("正文完整率", "missing_text"),
            ("来源 URL 完整率", "missing_source"),
        ):
            present = max(0, total - int(quality.get(missing_key) or 0))
            rows.append(
                {
                    "指标": label,
                    "分子": present,
                    "分母": total,
                    "结果": format_percentage(present, total),
                    "定义": definitions[missing_key],
                }
            )
        rows.extend(
            [
                {
                    "指标": "来源验证率",
                    "分子": coverage.get("verified_slots"),
                    "分母": coverage.get("total_slots"),
                    "结果": format_percentage(
                        coverage.get("verified_slots"), coverage.get("total_slots")
                    ),
                    "定义": "通过既定严格来源验证的必需槽位",
                },
                {
                    "指标": "来源启用率",
                    "分子": coverage.get("enabled_slots"),
                    "分母": coverage.get("total_slots"),
                    "结果": format_percentage(
                        coverage.get("enabled_slots"), coverage.get("total_slots")
                    ),
                    "定义": "已有启用来源的必需槽位",
                },
            ]
        )
        safe_dataframe(rows, height=380)
    with tabs[1]:
        city = snapshot.frames["city_progress"]
        safe_dataframe(
            city.rename(
                {
                    "city_id": "城市 ID",
                    "city_name": "城市",
                    "processed_shards": "已处理分片",
                    "planned_shards": "已规划分片",
                    "city_status": "运行状态",
                }
            )
            if not city.is_empty()
            else city,
            height=520,
        )
    with tabs[2]:
        records = snapshot.frames["records"]
        geographies = snapshot.frames["record_geographies"]
        if records.is_empty() or geographies.is_empty():
            st.info("暂无可计算的城市×年份文档覆盖。")
        else:
            years = (
                records.select("record_id", "record_date")
                .filter(pl.col("record_date").is_not_null())
                .with_columns(pl.col("record_date").dt.year().alias("年份"))
            )
            city_year = (
                geographies.select("record_id", "city_id", "city_name")
                .join(years, on="record_id", how="inner")
                .group_by("年份")
                .agg(
                    pl.col("city_id").n_unique().alias("有文档城市数"), pl.len().alias("政策记录数")
                )
                .sort("年份")
            )
            safe_dataframe(city_year, height=460)
    with tabs[3]:
        slots = snapshot.frames["source_slots"]
        if slots.is_empty():
            st.info("暂无来源槽位数据。")
        else:
            role = (
                slots.group_by("source_role")
                .agg(
                    pl.len().alias("必需槽位"),
                    (pl.col("verified_candidate_count") > 0).sum().alias("已核验"),
                    (pl.col("enabled_source_count") > 0).sum().alias("已启用"),
                    (pl.col("status") == "unresolved").sum().alias("未解决"),
                )
                .with_columns(
                    pl.col("source_role")
                    .map_elements(format_source_role, return_dtype=pl.String)
                    .alias("来源角色")
                )
                .select("来源角色", "必需槽位", "已核验", "已启用", "未解决")
            )
            safe_dataframe(role, height=360)
    with tabs[4]:
        gaps = snapshot.frames["coverage_gaps"]
        if gaps.is_empty():
            st.info("暂无缺口登记。")
        else:
            columns = [
                name
                for name in (
                    "gap_id",
                    "city_id",
                    "source_id",
                    "gap_type",
                    "start_date",
                    "end_date",
                    "severity",
                    "status",
                    "repair_attempts",
                    "next_retry_at",
                    "resolution",
                )
                if name in gaps.columns
            ]
            safe_dataframe(gaps.select(columns).head(500), height=550)
    with tabs[5]:
        gaps = snapshot.frames["coverage_gaps"]
        if gaps.is_empty() or "gap_type" not in gaps.columns:
            st.info("暂无可汇总的失败原因。")
        else:
            pareto = (
                gaps.group_by("gap_type")
                .agg(pl.len().alias("数量"))
                .sort("数量", descending=True)
                .head(20)
            )
            figure = px.bar(
                safe_pandas(pareto),
                x="数量",
                y="gap_type",
                orientation="h",
                title="缺口类型 Pareto",
                color_discrete_sequence=[NANJING_PURPLE],
            )
            st.plotly_chart(style_plotly_figure(figure, height=460), width="stretch")
    freshness_caption(snapshot.generated_at, "E 盘 Curated 统一快照")


def render_review_page(settings: Settings) -> None:
    snapshot = _snapshot(settings)
    st.caption("人工审核只处理机器无法可靠决定的事项；AI 排序、置信度和建议均不是事实标准。")
    columns = st.columns(4)
    columns[0].metric("来源候选待审", format_count(snapshot.review.get("source_candidates")))
    columns[1].metric(
        "低置信度候选", format_count(snapshot.review.get("low_confidence_candidates"))
    )
    columns[2].metric("文档字段问题", format_count(snapshot.review.get("document_issues")))
    columns[3].metric("高严重度缺口", format_count(snapshot.coverage.get("critical_gaps")))
    tabs = st.tabs(["来源审核", "文档审核", "AI 低置信度", "异常与冲突"])
    candidates = snapshot.frames["source_candidates"]
    with tabs[0]:
        if candidates.is_empty():
            st.info("暂无来源候选。")
        else:
            review = candidates.filter(
                pl.col("manual_review_status")
                .fill_null("")
                .str.to_lowercase()
                .is_in(["pending", "human_review", "requires_human_review", "needs_research"])
            )
            rows = [
                {
                    "候选 ID": row.get("candidate_id"),
                    "城市 ID": row.get("city_id"),
                    "来源角色": format_source_role(row.get("source_role")),
                    "候选 URL": row.get("candidate_url"),
                    "官方站点": row.get("is_official"),
                    "严格核验": row.get("is_verified"),
                    "已启用": row.get("is_enabled"),
                    "审核状态": format_status(row.get("manual_review_status")),
                    "置信度": row.get("overall_confidence"),
                    "更新时间": format_datetime(row.get("updated_at")),
                }
                for row in review.head(100).to_dicts()
            ]
            safe_dataframe(rows, height=520, limit=100)
            st.caption(
                f"当前显示前 {min(100, review.height)} 条，共 {review.height:,} 条；完整清单可从数据质量页导出。"
            )
    with tabs[1]:
        issues = [
            {"问题": key, "数量": value}
            for key, value in snapshot.quality.items()
            if key
            in {
                "missing_title",
                "missing_date",
                "missing_text",
                "short_text",
                "missing_source",
                "duplicate_url",
                "duplicate_hash",
            }
        ]
        safe_dataframe(issues, height=360)
    with tabs[2]:
        if candidates.is_empty() or "overall_confidence" not in candidates.columns:
            st.info("暂无 AI 置信度记录。")
        else:
            low = candidates.filter(
                pl.col("overall_confidence").is_not_null() & (pl.col("overall_confidence") < 0.7)
            )
            rows = [
                {
                    "候选 ID": row.get("candidate_id"),
                    "城市 ID": row.get("city_id"),
                    "来源角色": format_source_role(row.get("source_role")),
                    "候选 URL": row.get("candidate_url"),
                    "置信度": row.get("overall_confidence"),
                    "审核状态": format_status(row.get("manual_review_status")),
                }
                for row in low.head(100).to_dicts()
            ]
            safe_dataframe(rows, height=520, limit=100)
    with tabs[3]:
        gaps = snapshot.frames["coverage_gaps"]
        if gaps.is_empty():
            st.info("暂无异常与冲突记录。")
        else:
            severe = (
                gaps.filter(
                    pl.col("severity").fill_null("").str.to_lowercase().is_in(["critical", "high"])
                )
                if "severity" in gaps.columns
                else gaps
            )
            safe_dataframe(severe.head(300), height=520)
    if settings.read_only:
        st.info("当前为只读模式；审核结论需要在本地管理模式中通过正式审核服务写入审计历史。")


def _configured_secret(name: str) -> str:
    try:
        return "已配置" if default_secret_store().has_secret(name) else "未配置"
    except Exception:
        return "状态不可用"


def render_system_page(settings: Settings) -> None:
    snapshot = _snapshot(settings)
    database = snapshot.system.get("database") or {}
    processes = snapshot.system.get("processes") or {}
    master = (snapshot.system.get("automation") or {}).get("MASTER_STATE") or {}
    rows = [
        {
            "组件": "正式数据库",
            "状态": database.get("status") or "QUERY_UNAVAILABLE",
            "更新时间": format_datetime(database.get("updated_at")),
            "说明": "file / connect / representative query 只读检查",
        },
        {
            "组件": "全量抓取器",
            "状态": format_status(snapshot.crawler.get("status")),
            "更新时间": format_datetime(snapshot.crawler.get("last_heartbeat_at")),
            "说明": f"worker PID {format_value(snapshot.crawler.get('worker_pid'))}",
        },
        {
            "组件": "自动化控制器",
            "状态": format_status(master.get("status")),
            "更新时间": format_datetime(master.get("last_heartbeat_at")),
            "说明": format_stage(master.get("stage")),
        },
        {
            "组件": "AI 队列",
            "状态": format_status(snapshot.ai.get("status")),
            "更新时间": format_datetime(snapshot.ai.get("updated_at")),
            "说明": "当前全量回溯未启用 AI",
        },
        {
            "组件": "PDF 归档",
            "状态": format_status(snapshot.archive.get("pdf_status")),
            "更新时间": "暂无数据",
            "说明": f"{format_count(snapshot.archive.get('pdf_assets'))} 个 PDF 资产",
        },
        {
            "组件": "Dashboard",
            "状态": "运行中" if processes.get("dashboard_processes") else "状态未知",
            "更新时间": format_datetime(snapshot.generated_at),
            "说明": "仅监听本机地址",
        },
    ]
    safe_dataframe(rows, height=350)
    tabs = st.tabs(["运行环境", "Provider", "自动化状态", "归档与磁盘", "政策强度占位", "诊断信息"])
    with tabs[0]:
        st.write("数据根目录：CRPD E 盘数据目录")
        st.write(
            "数据库模式："
            + (
                "正式 DuckDB"
                if database.get("status") == "HEALTHY"
                else "Curated 只读降级"
                if database.get("fallback_available")
                else "不可用"
            )
        )
        st.write("Dashboard 刷新：实时页面每 20 秒；政策和审核查询按用户操作刷新")
        st.write("运行权限：" + ("只读" if settings.read_only else "本地管理"))
    with tabs[1]:
        provider_rows = [
            {
                "服务": "AI Provider",
                "Provider": settings.ai_provider,
                "模型": format_value(settings.siliconflow_chat_model or settings.glm_model),
                "配置状态": _configured_secret("siliconflow_api_key"),
            },
            {
                "服务": "Search Provider",
                "Provider": format_value(settings.search_provider),
                "模型": "不适用",
                "配置状态": _configured_secret("search_api_key"),
            },
        ]
        safe_dataframe(provider_rows, height=240)
        st.caption("页面只显示 provider、模型与 configured 状态，不读取或输出密钥内容。")
    with tabs[2]:
        automation = snapshot.system.get("automation") or {}
        safe_dataframe(
            [
                {
                    "状态域": name.replace("_", " "),
                    "状态": format_status(value.get("status")),
                    "更新时间": format_datetime(
                        value.get("updated_at") or value.get("last_heartbeat_at")
                    ),
                }
                for name, value in automation.items()
            ],
            height=320,
        )
    with tabs[3]:
        archive = snapshot.archive
        safe_dataframe(
            [
                {"指标": "PDF 资产", "值": archive.get("pdf_assets")},
                {"指标": "有效 PDF", "值": archive.get("valid_pdf_assets")},
                {"指标": "已解析 PDF", "值": archive.get("parsed_pdf_assets")},
                {"指标": "扫描件", "值": archive.get("scanned_pdf_assets")},
                {
                    "指标": "归档目录",
                    "值": "可用" if archive.get("archive_root_exists") else "不可用",
                },
            ],
            height=260,
        )
        disk = snapshot.system.get("disk") or {}
        if disk:
            safe_dataframe(
                [
                    {
                        "存储区域": "CRPD 数据盘",
                        "状态": format_status(disk.get("status")),
                        "剩余空间（GB）": (disk.get("data") or {}).get("free_gb"),
                    },
                    {
                        "存储区域": "项目盘",
                        "状态": format_status(disk.get("status")),
                        "剩余空间（GB）": (disk.get("project") or {}).get("free_gb"),
                    },
                ],
                height=180,
            )
    with tabs[4]:
        st.warning("政策强度测度：尚未启用")
        st.write("原因：政策强度指标体系仍在设计。")
        st.write(f"已有可测度文档：{format_count(snapshot.documents.get('records'))}")
        st.write("已测度文档：0")
        st.write("下一步：配置指标体系、提示词版本和测度模型后启用。")
        st.caption(
            "现有历史强度表不会在本页面被当作当前正式结果；Dashboard 不触发任何政策强度 API 调用。"
        )
    with tabs[5]:
        st.caption("以下完整路径仅用于本机诊断，默认不出现在业务页面。")
        st.code(
            f"数据根目录：{snapshot.data_root}\n数据库：{snapshot.database_path}\nCurated：{settings.curated}",
            language=None,
        )
        unavailable = [
            name
            for name, meta in snapshot.availability.items()
            if meta.get("status") != "available"
        ]
        st.write("暂不可用的数据接口：" + ("、".join(unavailable) if unavailable else "无"))


__all__ = [
    "render_collection_page",
    "render_overview_page",
    "render_policy_center_page",
    "render_quality_page",
    "render_review_page",
    "render_system_page",
]
