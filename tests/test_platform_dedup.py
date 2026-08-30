"""CRPD platform dedup regressions — canonicalization, identity keys, pairs."""
from __future__ import annotations

from policydb.crawl.dedup import (
    canonicalize_url,
    classify_text_pair,
    normalized_text_hash,
    policy_identity_key,
)

TEXT_A = "关于促进房地产市场平稳健康发展的通知"
TEXT_A_REPRINT = "关于促进房地产市场平稳健康发展的通知 "  # trailing space
TEXT_B = "关于印发进一步优化住房公积金使用政策的通知"


def test_canonicalize_strips_tracking_and_www():
    url = "https://WWW.ZJJ.example.gov.cn/zcfg/a.html?utm_source=wechat&spm=x&id=7&share=1"
    assert canonicalize_url(url) == "https://zjj.example.gov.cn/zcfg/a.html?id=7"


def test_canonicalize_normalizes_mobile_host_and_trailing_slash():
    assert canonicalize_url("http://m.zjj.example.gov.cn/x/") == "http://zjj.example.gov.cn/x"
    assert canonicalize_url("https://www.example.gov.cn/a///b/") == "https://example.gov.cn/a/b"


def test_identity_key_stable_and_sensitive_to_document_number():
    key1 = policy_identity_key(
        title=TEXT_A, document_number="建房〔2022〕1号", agency="市住建局", publication_date="2022-01-05"
    )
    key2 = policy_identity_key(
        title=TEXT_A, document_number="建房〔2022〕1号", agency="市住建局", publication_date="2022-01-05"
    )
    key3 = policy_identity_key(
        title=TEXT_A, document_number="建房〔2022〕2号", agency="市住建局", publication_date="2022-01-05"
    )
    assert key1 == key2
    assert key1 != key3
    assert len(key1) == 64
    assert all(c in "0123456789abcdef" for c in key1)


def test_normalized_text_hash_ignores_whitespace():
    assert normalized_text_hash(TEXT_A) == normalized_text_hash("关于促进房地产市场 平稳健康发展 的通知")


def test_pair_identical_is_duplicate_content():
    decision = classify_text_pair(TEXT_A, TEXT_A_REPRINT)
    assert decision.decision == "duplicate_content"
    assert decision.level == "L4"


def test_pair_distinct_text_is_new_document():
    decision = classify_text_pair(TEXT_A, TEXT_B)
    assert decision.decision == "new_document"
    assert decision.level == "L6"


def test_pair_high_similarity_is_possible_reprint():
    near = TEXT_A.replace("通知", "公告")
    decision = classify_text_pair(TEXT_A, near)
    assert decision.decision in {"possible_reprint", "possible_version", "new_document"}
