from policydb.classify.rules import classify, infer_direction
from policydb.transform.normalization import (
    clean_text,
    content_hash,
    normalize_title,
    normalize_url,
    stable_id,
)


def test_clean_text():
    assert clean_text("  武汉\t市  ") == "武汉 市"


def test_clean_none():
    assert clean_text(None) is None


def test_normalize_title():
    assert normalize_title("《关于 城市更新 的通知》") == "关于城市更新的通知"


def test_normalize_url_tracking():
    assert "utm_" not in normalize_url("HTTPS://EXAMPLE.COM/a/?utm_source=x&b=2")


def test_stable_id_deterministic():
    assert stable_id("a", 1) == stable_id("a", 1)


def test_stable_id_prefix():
    assert stable_id("a", prefix="POL").startswith("POL_")


def test_content_hash_changes():
    assert content_hash("a") != content_hash("b")


def test_classifier_evidence():
    assert classify("推进城市更新工作")[0]["topic"] == "城市更新"


def test_direction_loosening():
    assert infer_direction("进一步放宽住房政策") == "loosening"


def test_direction_supportive():
    assert infer_direction("支持购房补贴") == "supportive"
