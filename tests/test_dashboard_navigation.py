from streamlit.testing.v1 import AppTest


def test_dashboard_has_only_six_primary_entries(root):
    app = AppTest.from_file(root / "app" / "dashboard.py", default_timeout=60).run()
    assert not app.exception
    assert app.radio[0].options == [
        "总览",
        "政策中心",
        "采集与处理",
        "数据质量与覆盖率",
        "人工审核",
        "系统与设置",
    ]


def test_dashboard_primary_pages_render(root, monkeypatch):
    monkeypatch.setenv("POLICYDB_READ_ONLY", "1")
    app = AppTest.from_file(root / "app" / "dashboard.py", default_timeout=60).run()
    for page in ["政策中心", "采集与处理", "数据质量与覆盖率", "人工审核", "系统与设置"]:
        app.radio[0].set_value(page).run()
        assert not app.exception, page
