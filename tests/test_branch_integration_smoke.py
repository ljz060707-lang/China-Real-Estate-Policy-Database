from pathlib import Path


def test_branch_audit_records_real_linear_heads():
    text = Path("docs/branch_integration_audit.md").read_text(encoding="utf-8")
    assert "8ba98a62a151eca39187d1a6ae012c3c32557421" in text
    assert "9ece7ea9d78d80041ddd6abad29369d36fbe2ca6" in text
    assert "2904b22e8ef6feafa3d4979242cdce8e334f07e9" in text
    assert "main → V2 → V3" in text
