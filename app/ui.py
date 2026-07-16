from __future__ import annotations

import html
from datetime import date, datetime

import pandas as pd
import polars as pl
import streamlit as st


def _pandas_value(value):
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, (list, dict)):
        return str(value)
    return value


def _table_value(value) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "是" if value else "否"
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, (list, dict)):
        return str(value)
    return str(value)


def safe_dataframe(frame, *, height: int | None = None) -> None:
    """Render a bounded table without Streamlit's unstable Arrow bridge on Windows."""
    if isinstance(frame, pl.DataFrame):
        rows = frame.to_dicts()
    else:
        rows = frame
    if isinstance(rows, list):
        rows = [
            {key: _table_value(value) for key, value in row.items()}
            for row in rows
        ]
    if not rows:
        st.caption("暂无数据")
        return
    columns = list(rows[0])
    header = "".join(f"<th>{html.escape(str(column))}</th>" for column in columns)
    body = "".join(
        "<tr>"
        + "".join(f"<td>{html.escape(str(row.get(column, '')))}</td>" for column in columns)
        + "</tr>"
        for row in rows[:500]
    )
    max_height = height or 430
    st.markdown(
        f"""
        <div style="max-height:{max_height}px;overflow:auto;border-top:2px solid #82318E;border-bottom:1px solid #E8E1EA">
          <table style="width:100%;border-collapse:collapse;font-size:0.82rem">
            <thead style="position:sticky;top:0;background:#F8F6F9;z-index:1"><tr>{header}</tr></thead>
            <tbody>{body}</tbody>
          </table>
        </div>
        <style>
          table th, table td {{padding:0.42rem 0.55rem;text-align:left;border-bottom:1px solid #EEE9F0;vertical-align:top}}
          table th {{color:#5B2C83;font-weight:650;white-space:nowrap}}
        </style>
        """,
        unsafe_allow_html=True,
    )


def safe_pandas(frame: pl.DataFrame) -> pd.DataFrame:
    """Build pandas objects from Python rows, bypassing Polars' Arrow string bridge."""
    return pd.DataFrame(
        [{key: _pandas_value(value) for key, value in row.items()} for row in frame.to_dicts()]
    )
