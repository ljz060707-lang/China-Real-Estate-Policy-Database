from streamlit.testing.v1 import AppTest


def test_update_completeness_page_renders(root):
    app = AppTest.from_file(root / "app" / "dashboard.py", default_timeout=60).run()
    app.radio[0].set_value("自动更新与完整性").run()
    assert not app.exception
    assert any(tab.label == "覆盖完整性" for tab in app.tabs)
