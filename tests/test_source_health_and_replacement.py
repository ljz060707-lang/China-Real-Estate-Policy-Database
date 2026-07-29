from policydb.crawl.models import RegisteredSource
from policydb.settings import Settings
from policydb.source_discovery import repair_sources


def test_source_health_and_replacement_are_explicit(tmp_path, monkeypatch):
    source = RegisteredSource(
        source_id="A",
        source_name="A",
        domain="a.gov.cn",
        source_type="government",
        source_role="canonical_candidate",
        official_status="official",
        crawl_enabled=True,
        source_health_score=40,
        replacement_source_id="B",
    )
    monkeypatch.setattr("policydb.source_discovery.load_registry", lambda _: [source])
    result = repair_sources(Settings(root=tmp_path))
    assert result["unhealthy_enabled_sources"] == ["A"]
    assert result["replacement_links"] == 1
    assert result["automatic_registry_changes"] == 0
