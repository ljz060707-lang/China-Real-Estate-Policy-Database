from __future__ import annotations

from pathlib import Path

import plotly.express as px
import polars as pl
import streamlit as st

from policydb.seed_source_candidates import audit_download_bytes
from policydb.settings import Settings
from policydb.source_discovery import REQUIRED_ROLES

STATUS_LABELS = {
    "not_started": "未开始",
    "source_incomplete": "来源不完整",
    "running": "运行中",
    "partial_cap": "达到上限",
    "partial_network": "网络异常",
    "partial_parser": "解析异常",
    "partial_archive": "归档不完整",
    "complete_unverified": "扫描完成待认证",
    "certified_complete": "已认证完整",
    "confirmed_zero": "确认零政策",
    "failed": "失败",
}


def _read(path: Path) -> pl.DataFrame:
    return pl.read_parquet(path) if path.exists() else pl.DataFrame()


def render_exhaustive_progress(root: Path) -> None:
    settings = Settings.discover(root)
    progress = _read(settings.curated / "city_year_progress.parquet")
    slots = _read(settings.curated / "source_requirement_slots.parquet")
    candidates = _read(settings.curated / "source_candidates.parquet")
    candidate_evidence = _read(
        settings.curated / "source_candidate_evidence.parquet"
    )
    shards = _read(settings.curated / "crawl_shards.parquet")

    if progress.is_empty() and slots.is_empty():
        st.info(
            "尚未生成全量搜索审计层。先运行："
            ".\\.venv\\Scripts\\policydb.exe sources audit-525"
        )
        return
    st.caption(
        "完成度采用门控逻辑：来源缺失、网络失败、分页未耗尽、命中上限或未知日期均不会认证为完整。"
    )
    overview_tab, matrix_tab, city_tab, candidate_tab = st.tabs(
        ["105城市总览", "城市—年份矩阵", "城市详情", "候选来源审核"]
    )

    with overview_tab:
        columns = st.columns(5)
        required = slots.height
        values = [
            ("城市", slots["city_id"].n_unique() if slots.height else 0),
            ("必需来源槽位", required),
            (
                "有候选",
                slots.filter(pl.col("candidate_count") > 0).height
                if slots.height
                else 0,
            ),
            (
                "已核验",
                slots.filter(pl.col("verified_candidate_count") > 0).height
                if slots.height
                else 0,
            ),
            (
                "已启用",
                slots.filter(pl.col("enabled_source_count") > 0).height
                if slots.height
                else 0,
            ),
        ]
        for column, (label, value) in zip(columns, values, strict=True):
            column.metric(label, value)
        if progress.height:
            statuses = (
                progress.group_by("status")
                .agg(pl.len().alias("count"))
                .with_columns(
                    pl.col("status")
                    .replace_strict(STATUS_LABELS, default=pl.col("status"))
                    .alias("状态")
                )
            )
            chart = px.bar(
                statuses.to_pandas(),
                x="状态",
                y="count",
                color_discrete_sequence=["#5B1AA8"],
                title="城市—年度状态分布",
            )
            chart.update_layout(showlegend=False)
            st.plotly_chart(chart, width="stretch")

    with matrix_tab:
        if progress.is_empty():
            st.info("尚无城市—年度进度。创建逐城任务后会自动生成。")
        else:
            provinces = sorted(
                progress["province_name"].drop_nulls().unique().to_list()
            )
            selected_province = st.selectbox(
                "省份", ["全部", *provinces], key="exhaustive_matrix_province"
            )
            states = sorted(progress["status"].drop_nulls().unique().to_list())
            selected_states = st.multiselect(
                "状态",
                states,
                format_func=lambda value: STATUS_LABELS.get(value, value),
            )
            filtered = progress
            if selected_province != "全部":
                filtered = filtered.filter(
                    pl.col("province_name") == selected_province
                )
            if selected_states:
                filtered = filtered.filter(pl.col("status").is_in(selected_states))
            heat = filtered.select(
                "city_name", "year", "overall_completion_pct", "status"
            ).sort(["city_name", "year"])
            if heat.height:
                pivoted = heat.pivot(
                    index="city_name",
                    on="year",
                    values="overall_completion_pct",
                    aggregate_function="max",
                ).to_pandas().set_index("city_name")
                figure = px.imshow(
                    pivoted,
                    color_continuous_scale=[
                        (0.0, "#F4F1F7"),
                        (0.5, "#B79AD6"),
                        (1.0, "#4A148C"),
                    ],
                    zmin=0,
                    zmax=100,
                    aspect="auto",
                    labels={"color": "门控完成度(%)"},
                )
                figure.update_layout(
                    height=max(480, min(1600, heat["city_name"].n_unique() * 20))
                )
                st.plotly_chart(figure, width="stretch")
                st.dataframe(
                    heat.with_columns(
                        pl.col("status")
                        .replace_strict(STATUS_LABELS, default=pl.col("status"))
                        .alias("状态")
                    ).rename(
                        {
                            "city_name": "城市",
                            "year": "年份",
                            "overall_completion_pct": "门控完成度(%)",
                        }
                    ),
                    width="stretch",
                    hide_index=True,
                )
            else:
                st.info("当前筛选条件下没有数据。")

    with city_tab:
        city_names = (
            sorted(slots["city_name"].unique().to_list()) if slots.height else []
        )
        if not city_names:
            st.info("来源槽位尚未建立。")
        else:
            city = st.selectbox("选择城市", city_names, key="exhaustive_detail_city")
            city_slots = slots.filter(pl.col("city_name") == city)
            city_id = city_slots[0, "city_id"]
            st.subheader(f"{city} · 5类必需来源")
            st.dataframe(
                city_slots.select(
                    "source_role",
                    "status",
                    "coverage_status",
                    "candidate_count",
                    "verified_candidate_count",
                    "enabled_source_count",
                    "resolution_note",
                ).rename(
                    {
                        "source_role": "来源角色",
                        "status": "技术状态",
                        "coverage_status": "审计覆盖状态",
                        "candidate_count": "候选",
                        "verified_candidate_count": "已核验",
                        "enabled_source_count": "已启用",
                        "resolution_note": "说明",
                    }
                ),
                width="stretch",
                hide_index=True,
            )
            city_progress = (
                progress.filter(pl.col("city_id") == city_id)
                if progress.height
                else pl.DataFrame()
            )
            if city_progress.height:
                st.subheader("逐年完成度")
                st.dataframe(city_progress, width="stretch", hide_index=True)
            city_shards = (
                shards.filter(pl.col("city_id") == city_id)
                if shards.height
                else pl.DataFrame()
            )
            if city_shards.height:
                st.subheader("月度分片与分页证据")
                st.dataframe(
                    city_shards.select(
                        "start_date",
                        "end_date",
                        "source_role",
                        "source_id",
                        "status",
                        "pages_scanned",
                        "pagination_exhausted",
                        "candidate_count",
                        "fetched",
                        "failed",
                        "date_unknown_count",
                        "cross_period_rejected_count",
                    ),
                    width="stretch",
                    hide_index=True,
                )
            st.code(
                f'.\\.venv\\Scripts\\policydb.exe crawl exhaustive-resume '
                f'--city "{city}" --from "2018-01-01" --to "today"',
                language="powershell",
            )

    with candidate_tab:
        st.warning(
            "此页默认只读。候选URL不会因出现在列表中而自动成为官方或启用来源。"
        )
        if candidates.is_empty():
            st.info("尚无候选。可先从既有注册表提取，或运行来源发现。")
        else:
            city_filter = st.selectbox(
                "城市筛选",
                ["全部", *sorted(slots["city_name"].unique().to_list())],
                key="candidate_city_filter",
            )
            shown = candidates
            if city_filter != "全部":
                city_id = slots.filter(pl.col("city_name") == city_filter)[0, "city_id"]
                shown = shown.filter(pl.col("city_id") == city_id)
            roles = st.multiselect(
                "来源角色", list(REQUIRED_ROLES), key="candidate_role_filter"
            )
            if roles:
                shown = shown.filter(pl.col("source_role").is_in(roles))
            st.dataframe(
                shown.select(
                    "city_id",
                    "source_role",
                    "candidate_url",
                    "candidate_kind",
                    "page_type",
                    "entry_eligible",
                    "site_name",
                    "network_route",
                    "health_status",
                    "overall_confidence",
                    "is_verified",
                    "is_enabled",
                    "manual_review_status",
                    "source_record_count",
                    "conflict_count",
                ),
                width="stretch",
                hide_index=True,
            )
            download_columns = st.columns(3)
            download_columns[0].download_button(
                "下载候选 CSV",
                audit_download_bytes(shown, ".csv"),
                file_name="source_candidates.csv",
                mime="text/csv",
            )
            download_columns[1].download_button(
                "下载候选 Parquet",
                audit_download_bytes(shown, ".parquet"),
                file_name="source_candidates.parquet",
                mime="application/octet-stream",
            )
            download_columns[2].download_button(
                "下载候选 Excel",
                audit_download_bytes(shown, ".xlsx"),
                file_name="source_candidates.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
            if candidate_evidence.height:
                shown_ids = shown["candidate_id"].drop_nulls().to_list()
                evidence = candidate_evidence.filter(
                    pl.col("candidate_id").is_in(shown_ids)
                )
                with st.expander("逐记录证据与冲突审计", expanded=False):
                    st.dataframe(
                        evidence.select(
                            "candidate_id",
                            "record_id",
                            "record_title",
                            "record_date",
                            "original_url",
                            "jurisdiction_name",
                            "relation_type",
                            "role_assignment_method",
                            "role_assignment_evidence",
                            "needs_manual_review",
                            "review_reason",
                        ),
                        width="stretch",
                        hide_index=True,
                    )
