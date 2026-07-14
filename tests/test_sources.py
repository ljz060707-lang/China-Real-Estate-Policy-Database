from __future__ import annotations

import polars as pl

from policydb.crawl.dedup import canonicalize_url
from policydb.sources import _classify_domain


def test_url_normalization_removes_tracking_and_fragment():
    assert (
        canonicalize_url("HTTPS://WWW.GOV.CN/a/?utm_source=x&b=2#part")
        == "https://gov.cn/a?b=2"
    )


def test_official_government_domain_is_p0():
    result = _classify_domain("zfcxjst.gd.gov.cn")
    assert result["official_status"] == "official"
    assert result["priority"] == 0


def test_media_source_is_never_marked_official():
    result = _classify_domain("mp.weixin.qq.com")
    assert result["official_status"] == "secondary_only"
    assert result["source_role"] == "discovery_only"


def test_source_registry_and_policy_source_relations_exist(root):
    sources = pl.read_parquet(root / "data" / "curated" / "source_registry.parquet")
    relations = pl.read_parquet(root / "data" / "curated" / "policy_sources.parquet")
    assert sources.height >= 100
    assert sources["domain"].n_unique() == sources.height
    assert relations.height >= 3000

