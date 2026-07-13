from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))

import streamlit as st  # noqa: E402

from app.review_center import render_review_center  # noqa: E402
from app.theme import apply_academic_theme  # noqa: E402

st.set_page_config(page_title="人工审核中心", page_icon="✅", layout="wide")
apply_academic_theme()
render_review_center(ROOT)
