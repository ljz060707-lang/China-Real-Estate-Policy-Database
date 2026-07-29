from policydb.settings import Settings
from policydb.taxonomy_v2 import load_taxonomy


def test_v3_taxonomy_has_exact_primary_and_secondary_labels(root):
    taxonomy = load_taxonomy(Settings.discover(root))
    assert taxonomy["version"] == "3.0.0"
    assert set(taxonomy["primary_categories"]) == {"D", "S", "F", "H", "G"}
    assert taxonomy["primary_categories"]["D"]["secondary"]["D09"] == (
        "人才、落户与家庭支持"
    )
    assert taxonomy["primary_categories"]["H"]["secondary"]["H12"] == "危旧房改造"
