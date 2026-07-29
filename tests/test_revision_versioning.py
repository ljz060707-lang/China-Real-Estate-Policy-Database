from policydb.crawl.models import RegisteredSource


def test_replacement_source_does_not_replace_historical_source_id():
    source = RegisteredSource(
        source_id="OLD",
        source_name="旧入口",
        domain="old.gov.cn",
        source_type="government",
        source_role="canonical_candidate",
        official_status="official",
        replacement_source_id="NEW",
    )
    assert source.source_id == "OLD"
    assert source.replacement_source_id == "NEW"
