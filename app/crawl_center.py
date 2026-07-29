from __future__ import annotations

import csv
import json
import time
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import plotly.express as px
import streamlit as st

from app.theme import style_plotly_figure
from app.ui import safe_dataframe
from policydb.crawl.registry import load_registry, set_sources_enabled
from policydb.jobs import CrawlJobRequest, JobManager
from policydb.settings import Settings

MODE_LABELS = {
    "智能组合抓取（推荐）": "smart",
    "官方来源增量更新": "official_update",
    "全网政策发现": "web_discovery",
    "中金原数据库链接回溯": "seed_backtrack",
    "105城市历史回溯": "historical_105",
    "缺失来源自动恢复": "recover_missing",
    "来源体检与智能启用": "source_health",
}

TOPICS = [
    "限购", "限售", "商业贷款与首付", "住房公积金", "购房补贴", "人才与落户",
    "保障性住房", "房地产融资协调机制", "项目白名单", "保交房", "土地供应与出让",
    "城市更新", "城中村改造", "老旧小区改造",
]


@st.cache_resource(show_spinner=False, ttl=10)
def _cached_sources(root: str, registry_stamp: int):
    del registry_stamp
    return load_registry(Settings.discover(root))


@st.cache_data(show_spinner=False, ttl=30)
def _cached_configuration(root: str) -> dict[str, bool]:
    settings = Settings.discover(root)
    return {
        "ai": bool(settings.siliconflow_api_key),
        "tianditu": bool(settings.tianditu_token),
        "search": bool(
            settings.search_api_key and settings.search_provider != "None"
        ),
    }


def _city_options(settings: Settings) -> list[str]:
    """Read the small UI selector without loading Polars in Streamlit."""
    path = settings.root / "data" / "reference" / "cities_105.csv"
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        rows = csv.DictReader(handle)
        values = [str(row.get("city_name") or "").strip() for row in rows]
    return [value for value in values if value]


def _start(manager: JobManager, request: CrawlJobRequest) -> bool:
    started = time.perf_counter()
    try:
        state = manager.create(request)
        manager.start(state.job_id)
    except PermissionError as exc:
        st.error(f"任务创建失败：{exc}")
        return False
    except Exception as exc:
        st.error(f"后台进程启动失败：{type(exc).__name__}：{exc}")
        return False
    st.session_state["active_crawl_job"] = state.job_id
    elapsed_ms = round((time.perf_counter() - started) * 1000, 1)
    manager.record_timing(state.job_id, "streamlit_start_seconds", elapsed_ms / 1000)
    st.session_state["crawl_start_elapsed_ms"] = elapsed_ms
    st.toast(f"后台任务已启动：{state.job_id}（{elapsed_ms} ms）")
    return True


def _configuration_cards(configuration: dict[str, bool], sources) -> None:
    cards = [
        ("AI服务", "已配置" if configuration["ai"] else "未配置"),
        ("天地图", "已配置" if configuration["tianditu"] else "未配置"),
        ("搜索服务", "已配置" if configuration["search"] else "未配置"),
        ("官方来源", sum(source.crawl_enabled and source.official_status in {"official", "official_reprint"} for source in sources)),
        ("来源总量", len(sources)),
    ]
    for column, (label, value) in zip(st.columns(5), cards, strict=True):
        column.metric(label, value)


@st.fragment
def _render_start_actions(
    manager: JobManager,
    request: CrawlJobRequest,
    *,
    read_only: bool,
    start_disabled: bool,
) -> None:
    action_columns = st.columns(2)
    if action_columns[0].button(
        "开始抓取",
        type="primary",
        width="stretch",
        disabled=read_only or start_disabled,
    ):
        if _start(manager, request):
            st.stop()
    if action_columns[1].button(
        "运行本地演示",
        width="stretch",
        disabled=read_only,
        help="固定使用 5 个本地样例，不访问网络、不写入政策主数据。",
    ):
        if _start(
            manager,
            request.model_copy(
                update={
                    "demo_mode": True,
                    "max_fetches": 5,
                    "run_glm": False,
                    "rebuild_database": False,
                    "run_validation": False,
                    "processing_mode": "staged_only",
                }
            ),
        ):
            st.stop()


def _render_new_job(
    settings: Settings,
    manager: JobManager,
    sources,
    configuration: dict[str, bool],
) -> None:
    read_only = settings.read_only
    if read_only:
        st.warning("当前为只读公开部署。API配置和抓取任务仅允许在本地管理环境执行。")
    label = st.selectbox("抓取模式", list(MODE_LABELS))
    mode = MODE_LABELS[label]
    default_start = date.today() - timedelta(days=3)
    if mode == "historical_105":
        default_start = date(2018, 1, 1)
    date_columns = st.columns(2)
    start_date = date_columns[0].date_input("起始日期", value=default_start)
    end_date = date_columns[1].date_input("结束日期", value=date.today())
    city_options = _city_options(settings)
    cities = st.multiselect("城市范围（留空按模式默认）", city_options)
    topics = st.multiselect("政策主题（留空表示全部主题）", TOPICS)
    limits = st.columns(2)
    max_candidates = int(limits[0].number_input("最大候选数", min_value=1, max_value=100000, value=200))
    max_fetches = int(limits[1].number_input("最大抓取数", min_value=1, max_value=10000, value=5 if mode == "seed_backtrack" else 100))
    options = st.columns(3)
    run_glm = options[0].checkbox("抓取后运行AI", value=configuration["ai"])
    verify = options[1].checkbox("执行第二轮自动复核", value=True)
    rebuild = options[2].checkbox("完成后重建数据库", value=True)
    validate = st.checkbox("完成后执行 validate", value=True)
    processing_label = st.selectbox(
        "处理深度",
        [
            "仅抓取并暂存",
            "抓取＋AI解析",
            "抓取＋AI＋独立复核",
            "完整处理并重建数据库",
        ],
        index=3,
    )
    processing_mode = {
        "仅抓取并暂存": "staged_only",
        "抓取＋AI解析": "glm",
        "抓取＋AI＋独立复核": "glm_verify",
        "完整处理并重建数据库": "full",
    }[processing_label]
    if processing_mode == "staged_only":
        run_glm = verify = rebuild = validate = False
    elif processing_mode == "glm":
        run_glm, verify, rebuild, validate = True, False, False, False
    elif processing_mode == "glm_verify":
        run_glm, verify, rebuild, validate = True, True, False, False
    demo_mode = st.checkbox(
        "本地演示（不访问网络，仅验证后台任务、进度与报告）",
        value=False,
        help="用于首次安装验收；结果会明确标记为模拟数据，不写入政策主数据。",
    )
    include_recommended = False
    confirmed_ids: list[str] = []
    if mode == "smart":
        st.checkbox("仅使用已启用来源", value=True, disabled=True)
        include_recommended = st.checkbox("同时使用高置信推荐来源", value=False)
        if include_recommended:
            recommended = [source for source in sources if source.recommended_enabled and not source.crawl_enabled]
            st.info(f"检测到 {len(recommended)} 个推荐来源。只有下方勾选并确认的来源会启用。")
            confirmed_ids = st.multiselect("确认本次启用来源", [source.source_id for source in recommended], format_func=lambda value: next((source.source_name for source in recommended if source.source_id == value), value))
    request = CrawlJobRequest(
        mode=mode,
        start_date=start_date,
        end_date=end_date,
        cities=cities,
        topics=topics,
        max_candidates=max_candidates,
        max_fetches=max_fetches,
        include_recommended=include_recommended,
        confirmed_recommended_source_ids=confirmed_ids,
        run_glm=run_glm,
        run_verification=verify,
        rebuild_database=rebuild,
        run_validation=validate,
        demo_mode=demo_mode,
        processing_mode=processing_mode,
    )
    enabled_source_count = sum(source.crawl_enabled for source in sources)
    estimate = request.estimate(enabled_source_count)
    st.subheader("任务预览")
    preview = [
        ("模式", label), ("日期范围", f"{start_date} 至 {end_date}"),
        ("城市数", estimate["city_count"]), ("主题数", estimate["topic_count"]),
        ("来源数", estimate["source_count"]), ("预计查询数", estimate["query_count"]),
        ("最大抓取量", max_fetches), ("API费用", "可能产生" if estimate["possible_api_calls"] or run_glm else "不涉及"),
    ]
    safe_dataframe([{"项目": key, "本次任务": value} for key, value in preview])
    enabled_official = sum(source.crawl_enabled and source.official_status in {"official", "official_reprint"} for source in sources)
    if mode == "official_update" and enabled_official == 0:
        st.error("当前没有已启用来源。官方增量任务不会被显示为“成功抓取0条”。")
        if st.button("运行来源体检", width="stretch", disabled=read_only):
            _start(manager, CrawlJobRequest(mode="source_health", max_fetches=20, rebuild_database=False, run_validation=False))
    if mode == "web_discovery" and not configuration["search"]:
        st.warning("全网发现需要配置搜索服务API；官方来源增量抓取和中金链接回溯仍可运行。")
    if mode == "historical_105" and estimate["possible_api_calls"] > 10000:
        st.error("规模估算超过一万次查询，请缩小城市、主题或时间范围。")
    _render_start_actions(
        manager,
        request,
        read_only=read_only,
        start_disabled=mode == "official_update" and enabled_official == 0,
    )
    _render_active_job(manager, key_prefix="new_job")


def _render_job_state(manager: JobManager, job_id: str, *, key_prefix: str) -> None:
    try:
        state = manager.inspect_state(job_id)
    except FileNotFoundError:
        return
    st.divider()
    st.subheader("当前后台任务")
    st.write(f"{state.job_id} · {state.stage} · {state.message}")
    terminal = {"completed", "completed_with_warnings", "failed", "cancelled"}
    now = datetime.now(UTC)
    if state.status not in terminal and state.heartbeat_at:
        heartbeat_age = (now - state.heartbeat_at).total_seconds()
        if heartbeat_age > 60:
            st.warning("任务长时间无进展；可请求安全停止，必要时再强制终止。")
        elif heartbeat_age > 15:
            st.info("后台任务正在等待网络或外部服务响应。")
        else:
            st.caption("后台进程运行正常；状态区每 2 秒局部刷新。")
    st.progress(min(state.progress_current / max(state.progress_total, 1), 1.0))
    counters = state.counters
    labels = [("已发现", "discovered"), ("已抓取", "fetched"), ("失败", "failed"), ("新增版本", "document_versions"), ("AI完成", "glm_completed"), ("待人工", "manual_review")]
    for column, (label, key) in zip(st.columns(6), labels, strict=True):
        column.metric(label, counters.get(key, counters.get("candidate_count", 0) if key == "discovered" else 0))
    columns = st.columns(4)
    columns[0].caption(f"PID：{state.pid or '未启动'}")
    if columns[1].button(
        "停止任务",
        width="stretch",
        disabled=state.status
        in {"completed", "completed_with_warnings", "failed", "cancelled"},
        key=f"{key_prefix}_cancel_{job_id}",
    ):
        manager.cancel(job_id)
        st.toast("已请求安全停止")
    confirm_force = columns[2].checkbox(
        "确认强制终止",
        key=f"{key_prefix}_force_confirm_{job_id}",
        disabled=state.status in terminal,
    )
    if columns[2].button(
        "强制终止进程",
        key=f"{key_prefix}_force_{job_id}",
        disabled=state.status in terminal or not confirm_force,
    ):
        manager.terminate(job_id)
        st.toast("后台进程已终止；暂存文件保留")
    report_path = manager.job_dir(job_id) / "report.md"
    if report_path.exists():
        columns[3].download_button("下载报告", report_path.read_bytes(), file_name=f"{job_id}_report.md", width="stretch")
    st.caption(f"任务日志：{manager.job_dir(job_id)}")
    if state.status in {"completed", "completed_with_warnings"}:
        request = manager.load_request(job_id)
        if request.processing_mode != "full":
            st.info("抓取结果已暂存，尚未合并到正式数据库。")


@st.fragment(run_every=2.0)
def _render_active_job(manager: JobManager, *, key_prefix: str) -> None:
    active = st.session_state.get("active_crawl_job")
    if active:
        _render_job_state(manager, active, key_prefix=key_prefix)


def _render_sources(settings: Settings, manager: JobManager, sources) -> None:
    official = st.selectbox("官方状态", ["全部", "official", "official_reprint", "unknown"])
    enabled = st.selectbox("启用状态", ["全部", "已启用", "未启用"])
    filtered = [source for source in sources if (official == "全部" or source.official_status == official) and (enabled == "全部" or source.crawl_enabled == (enabled == "已启用"))]
    page_size = 50
    page = int(
        st.number_input(
            "来源页码",
            min_value=1,
            max_value=max(1, (len(filtered) + page_size - 1) // page_size),
            value=1,
        )
    )
    page_sources = filtered[(page - 1) * page_size : page * page_size]
    rows = [{"source_id": source.source_id, "来源": source.source_name, "官方状态": source.official_status, "健康评分": source.source_health_score, "已启用": source.crawl_enabled, "推荐": source.recommended_enabled, "最近错误": source.last_error} for source in page_sources]
    safe_dataframe(rows, height=420)
    st.caption(f"共 {len(filtered)} 个来源；当前第 {page} 页，每页 {page_size} 个。")
    recommended_ids = [source.source_id for source in filtered if source.recommended_enabled and not source.crawl_enabled]
    selected = st.multiselect("批量选择当前页来源", [source.source_id for source in page_sources], format_func=lambda value: next((source.source_name for source in page_sources if source.source_id == value), value))
    columns = st.columns(4)
    if columns[0].button("运行来源体检", width="stretch", disabled=settings.read_only):
        _start(manager, CrawlJobRequest(mode="source_health", max_fetches=20, rebuild_database=False, run_validation=False))
    if columns[1].button("启用所选", width="stretch", disabled=settings.read_only or not selected):
        set_sources_enabled(selected, True, settings)
        st.success(f"已启用 {len(selected)} 个来源。")
    if columns[2].button("关闭所选", width="stretch", disabled=settings.read_only or not selected):
        set_sources_enabled(selected, False, settings)
        st.success(f"已关闭 {len(selected)} 个来源。")
    if columns[3].button("选择推荐前20项", width="stretch", disabled=not recommended_ids):
        st.info("请在“批量选择来源”中逐项确认；系统不会静默启用816个来源。")


def _render_history(manager: JobManager) -> None:
    states = manager.list_states()
    safe_dataframe([state.model_dump(mode="json") for state in states], height=420)
    if states:
        selected = st.selectbox("查看任务", [state.job_id for state in states])
        _render_job_state(manager, selected, key_prefix="history")


def _render_reports(settings: Settings, manager: JobManager) -> None:
    states = [state for state in manager.list_states() if (manager.job_dir(state.job_id) / "report.json").exists()]
    if not states:
        st.info("尚无抓取报告。完成一个后台任务后会自动生成 Markdown、JSON 和 CSV。")
        return
    selected = st.selectbox("报告任务", [state.job_id for state in states])
    summary = json.loads((manager.job_dir(selected) / "report.json").read_text(encoding="utf-8"))
    metrics = [(key, value) for key, value in summary.items() if isinstance(value, (int, float))]
    safe_dataframe([{"指标": key, "数值": value} for key, value in metrics])
    funnel_keys = [key for key in ("candidate_count", "fetched", "document_versions", "auto_verified") if key in summary]
    if funnel_keys:
        figure = px.funnel(x=[summary[key] for key in funnel_keys], y=funnel_keys, title="抓取阶段漏斗")
        st.plotly_chart(style_plotly_figure(figure), width="stretch")
    report = manager.job_dir(selected) / "report.md"
    if report.exists():
        st.markdown(report.read_text(encoding="utf-8"))
    output = settings.root / "outputs" / "crawl_reports" / selected
    st.caption(f"完整 CSV 报告目录：{output}")


def render_crawl_section(root: str | Path | None, section: str) -> None:
    settings = Settings.discover(root)
    manager = JobManager(settings)
    if "active_crawl_job" not in st.session_state:
        active = next(
            (
                state.job_id
                for state in manager.list_states(limit=10)
                if state.status
                not in {"completed", "completed_with_warnings", "failed", "cancelled"}
            ),
            None,
        )
        if active:
            st.session_state["active_crawl_job"] = active
    registry = settings.root / "data" / "reference" / "source_registry.yaml"
    sources = _cached_sources(str(settings.root), registry.stat().st_mtime_ns)
    configuration = _cached_configuration(str(settings.root))
    if section == "运行状态":
        _configuration_cards(configuration, sources)
        _render_new_job(settings, manager, sources, configuration)
    elif section == "来源管理":
        _render_sources(settings, manager, sources)
    elif section == "运行历史":
        _render_history(manager)
    elif section == "抓取报告":
        _render_reports(settings, manager)


def render_crawl_center(root: str | Path | None = None) -> None:
    st.caption("选择模式、设置范围并启动后台任务；页面刷新后仍可恢复进度和报告。")
    view = st.segmented_control(
        "抓取中心视图",
        ["运行状态", "来源管理", "运行历史", "抓取报告"],
        default="运行状态",
        label_visibility="collapsed",
    )
    render_crawl_section(root, view or "运行状态")
