from __future__ import annotations

from streamlit.testing.v1 import AppTest


def test_dashboard_pages_render_without_exceptions(root):
    app = AppTest.from_file(root / "app" / "dashboard.py", default_timeout=60).run()
    assert not app.exception
    labels = [option.value for option in app.radio]
    assert "数据总览" in labels

    for page in (
        "政策中心",
        "政策体系",
        "105城市",
        "政策检索",
        "时间趋势",
        "地区比较",
        "数据质量",
        "人工审核中心",
        "智能抓取",
        "自动更新与完整性",
        "个人设置",
    ):
        app.radio[0].set_value(page).run()
        assert not app.exception


def test_dashboard_city_filter_is_fast_and_stable(root):
    app = AppTest.from_file(root / "app" / "dashboard.py", default_timeout=60).run()
    app.radio[0].set_value("105城市").run()
    province = next(item for item in app.selectbox if item.label == "省份")
    province.set_value("上海市").run()
    assert not app.exception
    metrics = {item.label: item.value for item in app.metric}
    assert metrics["筛选政策数"] != "0"
