from __future__ import annotations

import plotly.graph_objects as go
import streamlit as st

NANJING_PURPLE = "#5F0080"


def apply_academic_theme() -> None:
    st.markdown(
        """
        <style>
        :root {
          --crpd-purple:#5F0080; --crpd-purple-soft:#F3EDF6;
          --crpd-ink:#1E1D22; --crpd-muted:#68666E; --crpd-line:#E3E1E6;
          --crpd-surface:#FFFFFF; --crpd-bg:#F5F5F7; --crpd-good:#176B4D;
          --crpd-warn:#9A5B00; --crpd-bad:#A32D2D;
        }
        html,body,[class*="css"] { font-family:"Microsoft YaHei","Noto Sans SC",system-ui,sans-serif; color:var(--crpd-ink); }
        .stApp,[data-testid="stAppViewContainer"],[data-testid="stMain"] { background:var(--crpd-bg); }
        .block-container { max-width:1680px; padding:1.15rem 1.45rem 2.4rem; }
        [data-testid="stHeader"] { background:rgba(245,245,247,.96); border-bottom:1px solid var(--crpd-line); }
        [data-testid="stToolbar"] { visibility:hidden; }
        [data-testid="stSidebar"] { background:#F0EFF2; border-right:1px solid #D8D6DC; }
        [data-testid="stSidebarNav"] { display:none !important; }
        [data-testid="stSidebar"] > div:first-child { padding:1rem .78rem; }
        .crpd-brand { padding:.2rem .42rem .95rem; margin-bottom:.65rem; border-bottom:1px solid #D4D1D8; }
        .crpd-brand strong { display:block; font-size:1rem; letter-spacing:.01em; color:#28262C; }
        .crpd-brand span { color:var(--crpd-muted); font-size:.7rem; letter-spacing:.1em; }
        [data-testid="stSidebar"] [role="radiogroup"] label { min-height:2.42rem; padding:.42rem .58rem; border-radius:4px; }
        [data-testid="stSidebar"] [role="radiogroup"] label:has(input:checked) { background:#FFF; box-shadow:inset 3px 0 0 var(--crpd-purple); color:#2A142E; font-weight:650; }
        h1 { font-size:1.46rem !important; line-height:1.25 !important; font-weight:720 !important; letter-spacing:-.02em; margin-bottom:.12rem !important; }
        h2 { font-size:1.06rem !important; font-weight:680 !important; margin-top:1.2rem !important; }
        h3 { font-size:.94rem !important; font-weight:660 !important; }
        .crpd-page-subtitle { color:var(--crpd-muted); font-size:.78rem; margin-bottom:.75rem; }
        .crpd-topline { border-top:3px solid var(--crpd-purple); border-bottom:1px solid var(--crpd-line); background:#FFF; padding:.58rem .72rem; margin:.2rem 0 .85rem; }
        .crpd-topline-grid { display:grid; grid-template-columns:repeat(6,minmax(110px,1fr)); gap:.55rem 1rem; }
        .crpd-topline-label { display:block; color:var(--crpd-muted); font-size:.68rem; margin-bottom:.14rem; }
        .crpd-topline-value { display:block; font-size:.83rem; font-weight:650; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }
        .crpd-status { display:inline-flex; align-items:center; gap:.35rem; font-weight:650; }
        .crpd-status::before { content:""; width:.48rem; height:.48rem; border-radius:50%; background:#737078; }
        .crpd-status.good::before { background:var(--crpd-good); }
        .crpd-status.warn::before { background:var(--crpd-warn); }
        .crpd-status.bad::before { background:var(--crpd-bad); }
        [data-testid="stMetric"] { background:transparent; border:0; border-left:1px solid var(--crpd-line); border-radius:0; padding:.28rem .72rem; min-height:72px; }
        [data-testid="stMetricLabel"] { color:var(--crpd-muted); font-size:.72rem; }
        [data-testid="stMetricValue"] { color:var(--crpd-ink); font-size:1.42rem; font-weight:670; font-variant-numeric:tabular-nums; }
        [data-testid="stMetricDelta"] { font-size:.69rem; }
        [data-testid="stDataFrame"],.crpd-table-wrap { border-top:2px solid #B4A4BA; border-bottom:1px solid var(--crpd-line); background:#FFF; }
        [data-testid="stPlotlyChart"] { background:#FFF; border-top:1px solid var(--crpd-line); }
        [data-testid="stExpander"] { border:1px solid var(--crpd-line); border-radius:4px; background:#FFF; }
        [data-testid="stTabs"] [data-baseweb="tab-list"] { gap:1.1rem; border-bottom:1px solid var(--crpd-line); }
        [data-testid="stTabs"] [data-baseweb="tab"] { color:var(--crpd-muted); font-size:.78rem; padding-left:0; padding-right:0; }
        [data-testid="stTabs"] button[aria-selected="true"] { color:var(--crpd-purple)!important; border-bottom-color:var(--crpd-purple)!important; }
        .stButton > button,.stDownloadButton > button,[data-testid="stLinkButton"] a { border:1px solid #A49DA9!important; border-radius:4px!important; color:#302D33!important; background:#FFF!important; font-weight:600!important; }
        .stButton > button[kind="primary"] { color:#FFF!important; background:var(--crpd-purple)!important; border-color:var(--crpd-purple)!important; }
        .stButton > button:hover,.stDownloadButton > button:hover { border-color:var(--crpd-purple)!important; color:var(--crpd-purple)!important; }
        [data-baseweb="input"] > div,[data-baseweb="select"] > div,[data-baseweb="textarea"] > div { border-radius:4px!important; border-color:#CFCBD2!important; background:#FFF!important; }
        .crpd-section-note { color:var(--crpd-muted); font-size:.72rem; padding:.18rem 0 .55rem; }
        .crpd-definition { color:var(--crpd-muted); font-size:.68rem; line-height:1.45; }
        .crpd-alert { border-left:3px solid var(--crpd-warn); background:#FFF9ED; padding:.65rem .78rem; font-size:.78rem; margin:.5rem 0; }
        @media(max-width:1100px) { .crpd-topline-grid { grid-template-columns:repeat(3,1fr); } .block-container { padding:1rem; } }
        @media(max-width:900px) { .crpd-topline-grid { grid-template-columns:repeat(2,1fr); } .block-container { padding:.85rem; } }
        @media(max-width:700px) { .crpd-topline-grid { grid-template-columns:repeat(2,1fr); } [data-testid="stMetric"] { border-left:0; border-bottom:1px solid var(--crpd-line); } }
        </style>
        """,
        unsafe_allow_html=True,
    )


def render_sidebar_brand() -> None:
    st.sidebar.markdown(
        """<div class="crpd-brand"><strong>中国房地产政策数据库</strong>
        <span>CRPD · POLICY RESEARCH WORKSTATION</span></div>""",
        unsafe_allow_html=True,
    )


def render_page_header(title: str, subtitle: str) -> None:
    st.title(title)
    st.markdown(
        f'<div class="crpd-page-subtitle">{subtitle}</div>',
        unsafe_allow_html=True,
    )


def style_plotly_figure(figure: go.Figure, *, height: int | None = None) -> go.Figure:
    figure.update_layout(
        paper_bgcolor="#FFFFFF",
        plot_bgcolor="#FFFFFF",
        font={"family": "Microsoft YaHei, Arial, sans-serif", "color": "#2A282D", "size": 12},
        colorway=["#5F0080", "#84718B", "#176B4D", "#B17B2A", "#56657A"],
        margin={"l": 42, "r": 20, "t": 42, "b": 42},
        title={"font": {"size": 14, "color": "#2A282D"}, "x": 0.01},
        hoverlabel={"bgcolor": "#FFFFFF", "font_color": "#242227"},
        height=height,
    )
    figure.update_xaxes(showgrid=False, linecolor="#D9D6DC", tickfont={"color": "#67636B"})
    figure.update_yaxes(gridcolor="#ECEAEC", zeroline=False, tickfont={"color": "#67636B"})
    return figure


__all__ = [
    "NANJING_PURPLE",
    "apply_academic_theme",
    "render_page_header",
    "render_sidebar_brand",
    "style_plotly_figure",
]
