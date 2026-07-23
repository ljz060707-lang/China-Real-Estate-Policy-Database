def test_theme_has_narrow_screen_reflow(root):
    theme = (root / "app" / "theme.py").read_text(encoding="utf-8")
    center = (root / "app" / "policy_center.py").read_text(encoding="utf-8")
    assert "@media(max-width:900px)" in theme
    assert "st.columns([1.2, 4.2, 1.7]" in center
