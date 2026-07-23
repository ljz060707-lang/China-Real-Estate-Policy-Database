def test_legacy_topic_fallback_is_not_in_primary_navigation(root):
    dashboard = (root / "app" / "dashboard.py").read_text(encoding="utf-8")
    assert "views.get(topic" not in dashboard
    assert "专题页面" not in dashboard
    assert "105城市" not in dashboard
