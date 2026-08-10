from __future__ import annotations

import html
from datetime import date, datetime
from typing import Any

import pandas as pd
import polars as pl
import streamlit as st

from policydb.dashboard_formatting import format_value
from policydb.dashboard_live_state import ProgressMetric


def _pandas_value(value: Any) -> Any:
    if isinstance(value, (date, datetime, list, tuple, set, dict)) or value is None:
        return format_value(value)
    return value


def _table_value(value: Any) -> str:
    return format_value(value, missing="")


def safe_dataframe(frame: Any, *, height: int | None = None, limit: int = 500) -> None:
    """Render a bounded table without Streamlit's unstable Arrow string bridge."""
    if isinstance(frame, pl.DataFrame):
        rows = frame.to_dicts()
    elif isinstance(frame, pd.DataFrame):
        rows = frame.to_dict(orient="records")
    else:
        rows = list(frame or [])
    rows = [{key: _table_value(value) for key, value in row.items()} for row in rows[:limit]]
    if not rows:
        st.caption("暂无数据")
        return
    columns = list(rows[0])
    header = "".join(f"<th>{html.escape(str(column))}</th>" for column in columns)
    body = "".join(
        "<tr>"
        + "".join(f"<td>{html.escape(str(row.get(column, '')))}</td>" for column in columns)
        + "</tr>"
        for row in rows
    )
    max_height = height or 430
    st.markdown(
        f"""
        <div class="crpd-table-wrap" style="max-height:{max_height}px;overflow:auto">
          <table class="crpd-table" style="width:100%;border-collapse:collapse;font-size:.76rem">
            <thead style="position:sticky;top:0;background:#F3F2F4;z-index:1"><tr>{header}</tr></thead>
            <tbody>{body}</tbody>
          </table>
        </div>
        <style>
          .crpd-table th,.crpd-table td {{padding:.43rem .52rem;text-align:left;border-bottom:1px solid #EAE8EC;vertical-align:top}}
          .crpd-table th {{color:#514C55;font-weight:650;white-space:nowrap}}
          .crpd-table td {{color:#2E2B31;max-width:420px}}
        </style>
        """,
        unsafe_allow_html=True,
    )


def safe_pandas(frame: pl.DataFrame) -> pd.DataFrame:
    """Build pandas objects from Python rows, avoiding the Windows Arrow crash."""
    rows = [{key: _pandas_value(value) for key, value in row.items()} for row in frame.to_dicts()]
    if not rows:
        return pd.DataFrame()
    columns: dict[str, pd.Series] = {}
    for name in rows[0]:
        values = [row.get(name) for row in rows]
        non_null = [value for value in values if value is not None]
        if non_null and all(
            isinstance(value, (int, float)) and not isinstance(value, bool) for value in non_null
        ):
            dtype = (
                "float64"
                if any(isinstance(value, float) for value in non_null)
                or len(non_null) != len(values)
                else "int64"
            )
            columns[name] = pd.Series(values, dtype=dtype)
        else:
            columns[name] = pd.Series(values, dtype="object")
    with pd.option_context("future.infer_string", False):
        return pd.DataFrame(columns)


def render_progress_metric(metric: ProgressMetric) -> None:
    if metric.numerator is None or metric.denominator in (None, 0):
        shown = "暂无数据"
    else:
        shown = f"{int(metric.numerator):,} / {int(metric.denominator):,}"
    delta = f"{float(metric.value) * 100:.1f}%" if metric.value is not None else None
    st.metric(metric.label, shown, delta=delta, delta_color="off")
    st.markdown(
        f'<div class="crpd-definition">{html.escape(metric.definition)}<br>来源：{html.escape(metric.source)}</div>',
        unsafe_allow_html=True,
    )


def render_status_strip(items: list[tuple[str, str, str]]) -> None:
    cells = "".join(
        '<div><span class="crpd-topline-label">{}</span><span class="crpd-topline-value {}">{}</span></div>'.format(
            html.escape(label),
            f"crpd-status {tone}" if tone else "",
            html.escape(value),
        )
        for label, value, tone in items
    )
    st.markdown(
        f'<div class="crpd-topline"><div class="crpd-topline-grid">{cells}</div></div>',
        unsafe_allow_html=True,
    )


def freshness_caption(updated_at: str | None, source: str) -> None:
    from policydb.dashboard_formatting import format_datetime

    st.caption(f"数据更新时间：{format_datetime(updated_at)} · 数据源：{source}")


__all__ = [
    "freshness_caption",
    "render_progress_metric",
    "render_status_strip",
    "safe_dataframe",
    "safe_pandas",
]
