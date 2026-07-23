from __future__ import annotations

import plotly.graph_objects as go
import streamlit as st

TSINGHUA_PURPLE = "#4A148C"


def apply_academic_theme() -> None:
    st.markdown(
        """
        <style>
        :root { --primary-900:#4A148C; --primary-800:#5B1AA8; --primary-700:#6B21C8;
          --primary-600:#7C3AED; --primary-100:#F1E8FA; --primary-050:#FAF7FD;
          --ink:#21182B; --muted:#6E6477; --line:#E7E2EC; }
        html,body,[class*="css"] { font-family:"Microsoft YaHei","Noto Sans SC",sans-serif; color:var(--ink); }
        .stApp,[data-testid="stAppViewContainer"],[data-testid="stMain"] { background:#F7F7FA; }
        .block-container { max-width:1600px; padding:1.4rem 1.5rem 2.4rem; }
        [data-testid="stHeader"] { background:#FFFFFF; border-bottom:1px solid var(--line); }
        [data-testid="stSidebar"] { background:#FFFFFF; border-right:1px solid var(--line); }
        [data-testid="stSidebar"] > div:first-child { padding:1rem .75rem; }
        .academic-brand { padding:.3rem .45rem 1rem; margin-bottom:.7rem; border-bottom:2px solid var(--primary-900); }
        .academic-brand strong { display:block; color:var(--primary-900); font-size:1.05rem; font-weight:700; }
        .academic-brand span { display:block; color:var(--muted); font-size:.72rem; letter-spacing:.08em; margin-top:.2rem; }
        [data-testid="stSidebar"] [role="radiogroup"] label { min-height:2.45rem; padding:.42rem .55rem; border-radius:8px; }
        [data-testid="stSidebar"] [role="radiogroup"] label:has(input:checked) { background:var(--primary-100); color:var(--primary-900); font-weight:650; }
        h1 { font-size:24px !important; font-weight:700 !important; color:var(--ink) !important; margin-bottom:.2rem !important; }
        h2,h3 { color:var(--ink) !important; font-size:17px !important; }
        .academic-page-subtitle { color:var(--muted); font-size:13px; margin-bottom:1.1rem; }
        [data-testid="stMetric"] { background:#FFFFFF; border:1px solid var(--line); border-radius:9px; padding:.65rem .8rem; min-height:82px; box-shadow:0 1px 2px rgba(33,24,43,.03); }
        [data-testid="stMetricLabel"] { color:var(--muted); font-size:12px; }
        [data-testid="stMetricValue"] { color:var(--primary-900); font-size:1.55rem; font-variant-numeric:tabular-nums; }
        [data-testid="stDataFrame"] { border:1px solid var(--line); border-radius:9px; overflow:hidden; background:#FFF; }
        [data-testid="stPlotlyChart"] { background:#FFFFFF; border:1px solid var(--line); border-radius:9px; padding:.25rem; }
        .stButton > button,.stDownloadButton > button,[data-testid="stLinkButton"] a { border:1px solid var(--primary-700)!important; border-radius:8px!important; color:var(--primary-800)!important; background:#FFF!important; font-weight:600!important; }
        .stButton > button[kind="primary"] { color:#FFF!important; background:var(--primary-800)!important; }
        .stButton > button:hover,.stDownloadButton > button:hover,[data-testid="stLinkButton"] a:hover { background:var(--primary-050)!important; border-color:var(--primary-600)!important; }
        [data-baseweb="input"] > div,[data-baseweb="select"] > div,[data-baseweb="textarea"] > div { border-radius:8px!important; border-color:var(--line)!important; }
        [data-baseweb="input"] > div:focus-within,[data-baseweb="select"] > div:focus-within { border-color:var(--primary-600)!important; box-shadow:0 0 0 1px var(--primary-600)!important; }
        [data-testid="stExpander"] { border:1px solid var(--line); border-radius:8px; background:#FFF; }
        [data-testid="stTabs"] [data-baseweb="tab"] { color:var(--muted); font-size:13px; }
        [data-testid="stTabs"] button[aria-selected="true"] { color:var(--primary-900)!important; border-bottom-color:var(--primary-700)!important; }
        @media(max-width:900px) { .block-container { padding:1rem; } [data-testid="stMetric"] { min-height:72px; } }
        </style>
        """,
        unsafe_allow_html=True,
    )


def render_sidebar_brand() -> None:
    st.sidebar.markdown(
        """<div class="academic-brand"><strong>中国房地产政策数据库</strong>
        <span>CRPD · Policy Research System</span></div>""",
        unsafe_allow_html=True,
    )


def render_page_header(title: str, subtitle: str) -> None:
    st.title(title)
    st.markdown(f'<div class="academic-page-subtitle">{subtitle}</div>', unsafe_allow_html=True)


def style_plotly_figure(figure: go.Figure) -> go.Figure:
    figure.update_layout(
        paper_bgcolor="#FFFFFF", plot_bgcolor="#FFFFFF", showlegend=False,
        font={"family": "Microsoft YaHei, Arial, sans-serif", "color": "#21182B"},
        colorway=["#4A148C", "#5B1AA8", "#6B21C8", "#7C3AED", "#B99AE7"],
        margin={"l": 36, "r": 18, "t": 48, "b": 36},
        title={"font": {"size": 15, "color": "#21182B"}, "x": 0.02},
        hoverlabel={"bgcolor": "#FFFFFF", "font_color": "#21182B"},
    )
    figure.update_xaxes(showgrid=False, linecolor="#E7E2EC", tickfont={"color": "#6E6477"})
    figure.update_yaxes(gridcolor="#F0ECF4", zeroline=False, tickfont={"color": "#6E6477"})
    return figure
