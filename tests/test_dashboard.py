from __future__ import annotations

from streamlit.testing.v1 import AppTest


def test_dashboard_pages_render_without_exceptions(root):
    app = AppTest.from_file(root / "app" / "dashboard.py", default_timeout=60).run()
    assert not app.exception
    labels = [option.value for option in app.radio]
    assert "数据总览" in labels

    for page in ("政策体系", "105城市", "时间趋势", "地区比较", "数据质量"):
        app.radio[0].set_value(page).run()
        assert not app.exception
