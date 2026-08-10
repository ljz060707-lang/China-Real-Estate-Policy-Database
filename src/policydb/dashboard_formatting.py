"""Chinese presentation helpers for the CRPD Dashboard.

The Dashboard never treats a missing value as zero and never exposes Python
container representations as user-facing text.
"""

from __future__ import annotations

import math
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

CHINA_TZ = ZoneInfo("Asia/Shanghai")

STATUS_LABELS = {
    "RUNNING": "运行中",
    "running": "运行中",
    "WAIT_CURRENT_RUN": "等待当前任务完成",
    "NOT_STARTED": "尚未启动",
    "UNKNOWN": "状态未知",
    "SUCCESS": "成功",
    "COMPLETED": "已完成",
    "completed": "已完成",
    "complete_policy_found": "已完成并发现政策",
    "complete_unverified": "采集完成，完整性待核验",
    "certified_complete": "完整性已认证",
    "confirmed_zero": "已确认无政策",
    "COMPLETE_WITH_GAPS": "完成但仍有缺口",
    "PARTIAL_BUT_USABLE": "部分完成，可使用",
    "PARTIAL_EMPTY": "部分完成，暂无内容",
    "PAUSED_BUDGET": "因预算暂停",
    "RETRY_WAIT": "等待重试",
    "HUMAN_REVIEW": "需要人工审核",
    "FAILED_TERMINAL": "终止失败",
    "SKIPPED_DEPENDENCY": "因依赖跳过",
    "source_incomplete": "来源不完整",
    "partial_cap": "达到安全上限",
    "partial_network": "网络异常",
    "partial_parser": "解析异常",
    "partial_archive": "归档不完整",
    "partial_temporal": "历史时段未完成",
    "failed": "失败",
    "pending": "等待处理",
    "candidate": "已有候选",
    "enabled": "已启用",
    "verified": "已核验",
    "unresolved": "未解决",
    "discovering": "正在发现政策链接",
    "fetching": "正在抓取政策页面",
    "parsing": "正在解析正文",
    "archiving": "正在归档",
    "postprocessing": "正在清洗与去重",
    "split_parent": "已拆分为子分片",
    "QUEUED": "排队中",
    "FAILED": "失败",
    "BLOCKED": "已阻断",
    "operational": "运行正常",
    "configured": "已配置",
    "unavailable": "暂不可用",
    "available": "可用",
    "stale": "数据已过期",
    "fresh": "数据新鲜",
    "healthy": "健康",
    "warning": "需要关注",
}

SOURCE_ROLE_LABELS = {
    "municipal_government": "市政府门户",
    "government_gazette": "政府公报",
    "housing_department": "住房城乡建设部门",
    "natural_resources_department": "自然资源部门",
    "provident_fund_center": "住房公积金中心",
}

STAGE_LABELS = {
    **STATUS_LABELS,
    "WAIT_CURRENT_RUN": "等待既有全量回溯结束",
    "ROUND_1_FAST_COVERAGE": "第一轮：快速覆盖",
    "ROUND_2_ROLE_COMPLETION": "第二轮：来源角色补齐",
    "ROUND_3_YEAR_COMPLETION": "第三轮：年份补齐",
    "ROUND_4_DEEP_BACKFILL": "第四轮：深度历史回溯",
    "ROUND_5_ATTACHMENTS": "第五轮：附件补齐",
    "ROUND_6_MANUAL_REVIEW": "第六轮：人工审核",
}


def is_missing(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return not value.strip() or value.strip().lower() in {"none", "null", "nan"}
    return isinstance(value, float) and math.isnan(value)


def format_value(value: Any, *, missing: str = "暂无数据") -> str:
    if is_missing(value):
        return missing
    if isinstance(value, bool):
        return "是" if value else "否"
    if isinstance(value, (datetime, date)):
        return format_datetime(value)
    if isinstance(value, dict):
        return f"结构化信息（{len(value)} 项）" if value else missing
    if isinstance(value, (list, tuple, set)):
        shown = [format_value(item, missing="") for item in value]
        shown = [item for item in shown if item]
        return "、".join(shown) if shown else missing
    if isinstance(value, int):
        return f"{value:,}"
    if isinstance(value, float):
        return f"{value:,.2f}".rstrip("0").rstrip(".")
    return str(value)


def format_count(value: Any) -> str:
    if is_missing(value):
        return "暂无数据"
    try:
        return f"{int(value):,}"
    except (TypeError, ValueError, OverflowError):
        return "暂无数据"


def parse_datetime(value: Any, *, assume_timezone=UTC) -> datetime | None:
    if is_missing(value):
        return None
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, date):
        parsed = datetime.combine(value, datetime.min.time())
    else:
        text = str(value).strip().replace("Z", "+00:00")
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError:
            return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=assume_timezone)
    return parsed


def format_datetime(value: Any, *, include_seconds: bool = True) -> str:
    parsed = parse_datetime(value)
    if parsed is None:
        return "暂无数据"
    local = parsed.astimezone(CHINA_TZ)
    pattern = "%Y-%m-%d %H:%M:%S" if include_seconds else "%Y-%m-%d %H:%M"
    return local.strftime(pattern)


def format_status(value: Any) -> str:
    if is_missing(value):
        return "暂无数据"
    text = str(value)
    return STATUS_LABELS.get(text, STAGE_LABELS.get(text, "未识别状态"))


def format_stage(value: Any) -> str:
    if is_missing(value):
        return "暂无数据"
    return STAGE_LABELS.get(str(value), format_status(value))


def format_source_role(value: Any) -> str:
    if is_missing(value):
        return "暂无数据"
    return SOURCE_ROLE_LABELS.get(str(value), "其他来源角色")


def percentage_value(numerator: Any, denominator: Any) -> float | None:
    if is_missing(numerator) or is_missing(denominator):
        return None
    try:
        denominator_value = float(denominator)
        if denominator_value <= 0:
            return None
        return float(numerator) / denominator_value
    except (TypeError, ValueError, OverflowError):
        return None


def format_percentage(
    numerator: Any,
    denominator: Any,
    *,
    decimals: int = 1,
    include_fraction: bool = True,
) -> str:
    value = percentage_value(numerator, denominator)
    if value is None:
        return "暂无数据"
    percent = f"{value * 100:.{decimals}f}%"
    if not include_fraction:
        return percent
    return f"{format_count(numerator)} / {format_count(denominator)}（{percent}）"


def format_path(value: Any, *, data_root: Path | None = None) -> str:
    if is_missing(value):
        return "暂无数据"
    path = Path(str(value))
    if data_root:
        try:
            relative = path.resolve().relative_to(data_root.resolve())
            return f"CRPD 数据目录 / {relative.as_posix()}"
        except (OSError, ValueError):
            pass
    return path.name or str(path)


__all__ = [
    "CHINA_TZ",
    "SOURCE_ROLE_LABELS",
    "STAGE_LABELS",
    "STATUS_LABELS",
    "format_count",
    "format_datetime",
    "format_path",
    "format_percentage",
    "format_source_role",
    "format_stage",
    "format_status",
    "format_value",
    "is_missing",
    "parse_datetime",
    "percentage_value",
]
