from policydb.transform.normalization import content_hash, normalize_title, normalize_url, stable_id


def test_same_content_same_hash():
    assert content_hash("政策文本") == content_hash("政策文本")


def test_whitespace_normalized_hash():
    assert content_hash("政策  文本") == content_hash("政策 文本")


def test_different_regions_distinct_ids():
    assert stable_id("武汉", "通知") != stable_id("广州", "通知")


def test_url_fragment_removed():
    assert normalize_url("https://a.cn/x#p") == "https://a.cn/x"


def test_title_punctuation_normalized():
    assert normalize_title("《通知》") == normalize_title("通知")
