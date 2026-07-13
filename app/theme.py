from __future__ import annotations

import plotly.graph_objects as go
import streamlit as st

TSINGHUA_PURPLE = "#82318E"
NANJING_PURPLE = "#5B2C83"


def apply_academic_theme() -> None:
    st.markdown(
        """
        <style>
        :root {
            --academic-purple: #82318E;
            --academic-purple-deep: #5B2C83;
            --academic-ink: #201B24;
            --academic-muted: #746C79;
            --academic-line: #E8E1EA;
            --academic-soft: #F8F6F9;
        }
        html, body, [class*="css"] {
            font-family: "Noto Sans SC", "Microsoft YaHei", "PingFang SC", sans-serif;
            color: var(--academic-ink);
        }
        [data-testid="stAppViewContainer"],
        [data-testid="stMain"],
        .stApp {
            background: #FFFFFF;
        }
        [data-testid="stHeader"] {
            background: rgba(255, 255, 255, 0.96);
            border-bottom: 1px solid var(--academic-line);
        }
        [data-testid="stSidebar"] {
            background: #FBFAFC;
            border-right: 1px solid var(--academic-line);
        }
        [data-testid="stSidebar"] > div:first-child {
            padding-top: 1.25rem;
        }
        section[data-testid="stSidebar"] div[data-testid="stSidebarNav"],
        section[data-testid="stSidebar"] ul[data-testid="stSidebarNavItems"] {
            display: none !important;
        }
        .academic-brand {
            padding: 0.2rem 0 1.15rem 0;
            margin-bottom: 1rem;
            border-bottom: 1px solid var(--academic-purple);
            box-shadow: 0 3px 0 -2px var(--academic-purple-deep);
        }
        .academic-brand strong {
            display: block;
            color: var(--academic-ink);
            font-size: 1.06rem;
            font-weight: 700;
            letter-spacing: 0.04em;
        }
        .academic-brand span {
            display: block;
            margin-top: 0.3rem;
            color: var(--academic-muted);
            font-size: 0.72rem;
            letter-spacing: 0.08em;
            text-transform: uppercase;
        }
        [data-testid="stSidebar"] [role="radiogroup"] label {
            min-height: 2.35rem;
            padding: 0.34rem 0.5rem;
            border-left: 2px solid transparent;
            border-radius: 0 4px 4px 0;
        }
        [data-testid="stSidebar"] [role="radiogroup"] label:has(input:checked) {
            color: var(--academic-purple-deep);
            background: #F3EDF5;
            border-left-color: var(--academic-purple);
            font-weight: 650;
        }
        .block-container {
            max-width: 1480px;
            padding-top: 2.2rem;
            padding-bottom: 3rem;
        }
        h1 {
            color: var(--academic-ink) !important;
            font-size: clamp(1.8rem, 2.2vw, 2.45rem) !important;
            font-weight: 720 !important;
            letter-spacing: -0.025em !important;
        }
        h1::after {
            content: "";
            display: block;
            width: 92px;
            margin-top: 0.68rem;
            border-bottom: 2px solid var(--academic-purple);
            box-shadow: 0 4px 0 -2px var(--academic-purple-deep);
        }
        h2, h3 {
            color: var(--academic-ink) !important;
            letter-spacing: -0.012em;
        }
        .academic-page-subtitle {
            max-width: 760px;
            margin: -0.25rem 0 1.6rem 0;
            color: var(--academic-muted);
            font-size: 0.92rem;
            line-height: 1.65;
        }
        [data-testid="stMetric"] {
            min-height: 92px;
            padding: 0.65rem 0.9rem 0.65rem 1rem;
            background: #FFFFFF;
            border: 0;
            border-left: 2px solid var(--academic-purple);
            border-radius: 0;
        }
        [data-testid="stMetricLabel"] {
            color: var(--academic-muted);
            font-size: 0.78rem;
            letter-spacing: 0.035em;
        }
        [data-testid="stMetricValue"] {
            color: var(--academic-ink);
            font-size: clamp(1.55rem, 2.05vw, 2rem);
            font-variant-numeric: tabular-nums;
        }
        [data-testid="stDataFrame"], [data-testid="stTable"] {
            border-top: 2px solid var(--academic-purple);
            border-bottom: 1px solid var(--academic-line);
        }
        [data-testid="stPlotlyChart"] {
            border-top: 1px solid var(--academic-line);
            padding-top: 0.4rem;
        }
        .stButton > button, .stDownloadButton > button, [data-testid="stLinkButton"] a {
            border: 1px solid var(--academic-purple) !important;
            border-radius: 4px !important;
            color: var(--academic-purple-deep) !important;
            background: #FFFFFF !important;
            font-weight: 620 !important;
        }
        .stButton > button:hover, .stDownloadButton > button:hover,
        [data-testid="stLinkButton"] a:hover {
            color: #FFFFFF !important;
            background: var(--academic-purple-deep) !important;
            border-color: var(--academic-purple-deep) !important;
        }
        [data-baseweb="input"] > div, [data-baseweb="select"] > div,
        [data-baseweb="textarea"] > div {
            border-radius: 4px !important;
        }
        [data-baseweb="input"] > div:focus-within,
        [data-baseweb="select"] > div:focus-within,
        [data-baseweb="textarea"] > div:focus-within {
            border-color: var(--academic-purple) !important;
            box-shadow: 0 0 0 1px var(--academic-purple) !important;
        }
        [data-testid="stAlert"] {
            border-radius: 4px;
            border: 1px solid var(--academic-line);
            border-left: 3px solid var(--academic-purple);
            background: #FBFAFC;
        }
        hr {
            border-color: var(--academic-line) !important;
        }
        @media (max-width: 900px) {
            .block-container { padding-top: 1.35rem; }
            [data-testid="stMetric"] { min-height: 78px; }
            h1 { font-size: 1.7rem !important; }
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def render_sidebar_brand() -> None:
    st.sidebar.markdown(
        """
        <div class="academic-brand">
          <strong>中国房地产政策数据库</strong>
          <span>Policy Research System</span>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_page_header(title: str, subtitle: str) -> None:
    st.title(title)
    st.markdown(
        f'<div class="academic-page-subtitle">{subtitle}</div>',
        unsafe_allow_html=True,
    )


def style_plotly_figure(figure: go.Figure) -> go.Figure:
    figure.update_layout(
        paper_bgcolor="#FFFFFF",
        plot_bgcolor="#FFFFFF",
        font={"family": "Microsoft YaHei, Arial, sans-serif", "color": "#3B3440"},
        colorway=[TSINGHUA_PURPLE, NANJING_PURPLE, "#A97CB0", "#6F5A75"],
        margin={"l": 30, "r": 20, "t": 64, "b": 30},
        title={"font": {"size": 18, "color": "#201B24"}, "x": 0.01},
        hoverlabel={"bgcolor": "#FFFFFF", "font_color": "#201B24"},
    )
    figure.update_xaxes(
        showgrid=False,
        linecolor="#D9D1DC",
        tickfont={"color": "#746C79"},
        title_font={"color": "#746C79"},
    )
    figure.update_yaxes(
        gridcolor="#EEE9F0",
        zeroline=False,
        linecolor="#D9D1DC",
        tickfont={"color": "#746C79"},
        title_font={"color": "#746C79"},
    )
    return figure
