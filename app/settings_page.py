from __future__ import annotations

from pathlib import Path

import httpx
import streamlit as st

from policydb.ai import SiliconFlowProvider
from policydb.config.preferences import PreferencesStore
from policydb.config.providers import build_search_provider
from policydb.config.secret_store import default_secret_store
from policydb.settings import Settings


def _result_label(exc: Exception | None, status_code: int | None = None) -> str:
    if exc is None and status_code and 200 <= status_code < 300:
        return "连接成功"
    if status_code in {401, 403}:
        return "认证失败"
    if status_code == 429:
        return "余额或配额不足"
    if isinstance(exc, (httpx.ConnectTimeout, httpx.ReadTimeout)):
        return "请求超时"
    if isinstance(exc, httpx.HTTPError):
        return "网络失败"
    return "响应格式异常"


def _configured(store, name: str) -> str:
    return "已配置：••••••••" if store.has_secret(name) else "未配置"


def _save_secret(store, name: str, value: str, read_only: bool) -> None:
    if read_only:
        return
    if value.strip():
        store.set_secret(name, value.strip())


@st.cache_data(ttl=60, show_spinner=False)
def _archive_stats(path: str) -> dict:
    root = Path(path)
    if not root.exists():
        return {"exists": False, "pdf": 0, "html": 0, "attachments": 0, "free": None}
    files = [item for item in root.rglob("*") if item.is_file()]
    usage = root.stat().st_dev
    import shutil

    return {
        "exists": True,
        "pdf": sum(item.suffix.lower() == ".pdf" for item in files),
        "html": sum(item.suffix.lower() in {".html", ".htm"} for item in files),
        "attachments": len(files),
        "free": shutil.disk_usage(root).free,
        "device": usage,
    }


def render_settings_page(root: str | Path | None = None) -> None:
    settings = Settings.discover(root)
    store = default_secret_store()
    preferences = PreferencesStore(settings.preferences_path)
    values = preferences.load()
    st.title("个人设置")
    st.caption("密钥只进入操作系统凭据库；普通设置文件、任务请求和日志均不保存密钥。")
    if settings.read_only:
        st.warning("当前为只读公开部署。API配置和抓取任务仅允许在本地管理环境执行。")
    ai_tab, search_tab, archive_tab, map_tab, system_tab = st.tabs(
        ["AI服务", "搜索服务", "档案存储", "地图服务", "系统运行"]
    )
    disabled = settings.read_only
    with ai_tab:
        st.write("AI服务：SiliconFlow")
        st.write(_configured(store, "siliconflow_api_key"))
        ai_key = st.text_input("SiliconFlow API Key", type="password", value="", placeholder="留空表示不修改", disabled=disabled)
        chat_model = st.text_input("分类/抽取模型", value=settings.siliconflow_chat_model, disabled=disabled)
        verify_model = st.text_input("独立复核模型", value=settings.siliconflow_verify_model, disabled=disabled)
        embedding_model = st.text_input("Embedding模型", value=settings.siliconflow_embedding_model, disabled=disabled)
        rerank_model = st.text_input("Rerank模型", value=settings.siliconflow_rerank_model, disabled=disabled)
        ai_base = st.text_input("SiliconFlow Base URL", value=settings.siliconflow_base_url, disabled=disabled)
        timeout = st.number_input("请求超时（秒）", min_value=5, max_value=300, value=int(settings.request_timeout), disabled=disabled)
        retries = st.number_input("最大重试", min_value=0, max_value=10, value=settings.max_retries, disabled=disabled)
        columns = st.columns(3)
        if columns[0].button("保存AI设置", width="stretch", disabled=disabled):
            _save_secret(store, "siliconflow_api_key", ai_key, disabled)
            preferences.save({
                "ai_provider": "siliconflow",
                "siliconflow_base_url": ai_base,
                "siliconflow_chat_model": chat_model,
                "siliconflow_verify_model": verify_model,
                "siliconflow_embedding_model": embedding_model,
                "siliconflow_rerank_model": rerank_model,
                "request_timeout": timeout,
                "max_retries": retries,
            })
            st.cache_resource.clear()
            st.success("设置已保存；密钥输入框不会回显。")
        if columns[1].button("测试连接", width="stretch", disabled=disabled or not store.has_secret("siliconflow_api_key")):
            try:
                result = SiliconFlowProvider(Settings.discover(root)).test()
                if not result["connected"]:
                    st.error(
                        "连接失败："
                        + {
                            "authentication_failed": "认证失败",
                            "quota_or_rate_limit": "余额或配额不足",
                        }.get(result.get("error_type"), "网络失败")
                    )
                elif result["unavailable_models"]:
                    st.warning("连接成功，但以下配置模型当前不可用：" + "、".join(result["unavailable_models"]))
                else:
                    st.success(f"连接成功，可用模型 {result['model_count']} 个。")
            except Exception as exc:
                st.info(_result_label(exc))
        confirm = st.checkbox("确认清除 SiliconFlow Key", key="clear_glm_confirm", disabled=disabled)
        if columns[2].button("清除密钥", width="stretch", disabled=disabled or not confirm):
            store.delete_secret("siliconflow_api_key")
            st.cache_resource.clear()
            st.success("SiliconFlow Key 已清除。")
    with archive_tab:
        st.caption("内容寻址档案与 Raw 层分离；归档检查不会修改原始资料。")
        st.code(str(settings.policy_archive_root), language=None)
        archive = _archive_stats(str(settings.policy_archive_root))
        for column, (label, value) in zip(
            st.columns(5),
            [
                ("目录状态", "可用" if archive["exists"] else "不可用"),
                ("PDF数量", archive["pdf"]),
                ("HTML数量", archive["html"]),
                ("附件数量", archive["attachments"]),
                ("剩余空间", f"{archive['free'] / 1024**3:.1f} GB" if archive["free"] else "—"),
            ],
            strict=True,
        ):
            column.metric(label, value)
        st.caption("完整性检查：uv run policydb archive audit")
    with map_tab:
        st.write(_configured(store, "tianditu_token"))
        token = st.text_input("天地图 Token", type="password", value="", placeholder="留空表示不修改", disabled=disabled)
        approval = st.text_input("审图号", value=settings.tianditu_map_approval, disabled=disabled)
        qualification = st.text_input("测绘资质号", value=settings.tianditu_qualification, disabled=disabled)
        columns = st.columns(3)
        if columns[0].button("保存地图设置", width="stretch", disabled=disabled):
            _save_secret(store, "tianditu_token", token, disabled)
            preferences.save({"tianditu_map_approval": approval, "tianditu_qualification": qualification})
            st.cache_resource.clear()
            st.success("地图设置已保存。")
        if columns[1].button("测试地图服务", width="stretch", disabled=disabled or not store.has_secret("tianditu_token")):
            try:
                response = httpx.get("https://api.tianditu.gov.cn/api", params={"v": "4.0", "tk": store.get_secret("tianditu_token")}, timeout=10)
                st.info(_result_label(None, response.status_code))
            except Exception as exc:
                st.info(_result_label(exc))
        confirm = st.checkbox("确认清除天地图 Token", key="clear_map_confirm", disabled=disabled)
        if columns[2].button("清除Token", width="stretch", disabled=disabled or not confirm):
            store.delete_secret("tianditu_token")
            st.cache_resource.clear()
            st.success("天地图 Token 已清除。")
    with search_tab:
        provider_name = st.selectbox("Provider", ["None", "Bing", "Serper", "Tavily"], index=["None", "Bing", "Serper", "Tavily"].index(settings.search_provider if settings.search_provider in {"None", "Bing", "Serper", "Tavily"} else "None"), disabled=disabled)
        st.write(_configured(store, "search_api_key"))
        search_key = st.text_input("搜索 API Key", type="password", value="", placeholder="留空表示不修改", disabled=disabled)
        search_base = st.text_input("API Base URL（可选）", value=settings.search_base_url or "", disabled=disabled)
        max_results = st.number_input("单次最大结果数", min_value=1, max_value=50, value=int(values.get("search_max_results", 10)), disabled=disabled)
        columns = st.columns(3)
        if columns[0].button("保存搜索设置", width="stretch", disabled=disabled):
            _save_secret(store, "search_api_key", search_key, disabled)
            preferences.save({"search_provider": provider_name, "search_base_url": search_base, "search_max_results": max_results})
            st.cache_resource.clear()
            st.success("搜索设置已保存。")
        if columns[1].button("测试搜索", width="stretch", disabled=disabled or provider_name == "None" or not store.has_secret("search_api_key")):
            try:
                provider = build_search_provider(provider_name, store.get_secret("search_api_key"), base_url=search_base or None)
                provider.search("房地产政策 site:gov.cn", max_results=1)
                st.info("连接成功")
            except httpx.HTTPStatusError as exc:
                st.info(_result_label(exc, exc.response.status_code))
            except Exception as exc:
                st.info(_result_label(exc))
        confirm = st.checkbox("确认清除搜索 API Key", key="clear_search_confirm", disabled=disabled)
        if columns[2].button("清除搜索密钥", width="stretch", disabled=disabled or not confirm):
            store.delete_secret("search_api_key")
            st.cache_resource.clear()
            st.success("搜索 API Key 已清除。")
    with system_tab:
        user_agent = st.text_input("User-Agent", value=settings.user_agent, disabled=disabled)
        connect_timeout = st.number_input("连接超时（秒）", min_value=1, max_value=120, value=int(settings.connect_timeout), disabled=disabled)
        rate_limit = st.number_input("默认域名限速（秒）", min_value=0.0, max_value=60.0, value=float(settings.default_rate_limit), disabled=disabled)
        concurrency = st.number_input("最大并发数", min_value=1, max_value=20, value=int(values.get("max_concurrency", 4)), disabled=disabled)
        robots = st.checkbox("遵守 robots.txt", value=True, disabled=True, help="公开部署与默认本地模式均强制开启")
        proxy = st.text_input("HTTP代理（作为密钥保存）", type="password", value="", placeholder="留空表示不修改", disabled=disabled)
        overlap = st.number_input("默认日期重叠窗口（天）", min_value=1, max_value=30, value=int(values.get("default_overlap_days", 3)), disabled=disabled)
        max_fetches = st.number_input("默认最大抓取数", min_value=1, max_value=10000, value=int(values.get("default_max_fetches", 100)), disabled=disabled)
        if st.button("保存网络设置", width="stretch", disabled=disabled):
            _save_secret(store, "http_proxy", proxy, disabled)
            preferences.save({"user_agent": user_agent, "connect_timeout": connect_timeout, "default_rate_limit": rate_limit, "max_concurrency": concurrency, "respect_robots": robots, "default_overlap_days": overlap, "default_max_fetches": max_fetches})
            st.cache_resource.clear()
            st.success("网络设置已保存。")
        st.metric("运行模式", "只读公开部署" if settings.read_only else "本地管理")
        st.write("密钥读取顺序：操作系统 Keyring → Streamlit Secrets → 环境变量 → 本地 .env 兼容层。")
        st.write("任务请求、DuckDB、Parquet、报告和日志均不保存密钥；日志会对 Bearer、常见 Key 名称和已知密钥值脱敏。")
