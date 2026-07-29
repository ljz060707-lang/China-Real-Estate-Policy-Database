from pathlib import Path


def test_readme_commands_use_current_cli_names():
    text = Path("README.md").read_text(encoding="utf-8")
    for command in (
        "policydb storage plan-migration",
        "policydb sources complete-matrix",
        "policydb ai route-pools",
        "policydb schedule install --confirm",
        "policydb crawl historical",
    ):
        assert command in text
    assert "V1/V2/V3分别运行" not in text
