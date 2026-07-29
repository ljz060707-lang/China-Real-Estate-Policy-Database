from policydb.crawl.dedup import content_sha256


def test_semantic_dedup_starts_with_exact_content_hash():
    assert content_sha256(b"same") == content_sha256(b"same")
    assert content_sha256(b"same") != content_sha256(b"different")
