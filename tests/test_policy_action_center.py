import shutil

from policydb import PolicyDB
from policydb.dashboard_queries import policy_list
from policydb.query.database import build_database
from policydb.settings import Settings


def test_action_center_preserves_legacy_lineage_without_using_it_as_filter(
    tmp_path, root
):
    target = tmp_path / "rebuilt"
    shutil.copytree(root / "data" / "curated", target / "data" / "curated")
    settings = Settings(root=target)
    build_database(settings)
    db = PolicyDB.open(target)
    fields = {
        row["column_name"]
        for row in db._query("DESCRIBE v_policy_action_center").to_dicts()
    }
    assert {
        "legacy_collection",
        "source_topic",
        "pdf_available",
        "full_text_available",
    } <= fields
    rows, _ = policy_list(
        db,
        {"primary_categories": ["D"]},
        page=1,
        page_size=5,
        sort_by="发布日期",
    )
    assert rows.height <= 5
