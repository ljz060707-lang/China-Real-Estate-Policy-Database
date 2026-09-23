from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_recent_launcher_defaults_to_complete_flow_with_bounded_fetching() -> None:
    script = (ROOT / "scripts" / "run_recent_5y_direct.ps1").read_text(
        encoding="utf-8"
    )

    assert "[switch]$SkipAI" in script
    assert "fetch_concurrency = $FetchConcurrency" in script
    assert "per_host_concurrency = $PerHostConcurrency" in script
    assert "api_concurrency = $ApiConcurrency" in script
    assert "$SkipAI = $true" in script
    assert script.count('"-SkipAI"') == 1
    assert '"-FetchConcurrency", [string]$FetchConcurrency' in script
    assert '"-PerHostConcurrency", [string]$PerHostConcurrency' in script
    assert '"-ApiConcurrency", [string]$ApiConcurrency' in script
    assert 'policydb_entrypoint.py' in script
