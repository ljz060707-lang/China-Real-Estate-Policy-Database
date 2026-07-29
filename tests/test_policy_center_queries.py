from policydb.dashboard_queries import (
    policy_distribution,
    policy_list,
    policy_metrics,
    policy_trend,
)


def test_policy_center_view_has_required_fields(db):
    fields = {row["column_name"] for row in db._query("DESCRIBE v_policy_action_center").to_dicts()}
    assert {"action_id", "record_id", "province", "city", "original_issuer", "archive_relative_path", "duplicate_cluster_id"} <= fields


def test_policy_center_metrics_and_list_share_filter(db):
    filters = {"start_date": "2018-01-01"}
    metrics = policy_metrics(db, filters)
    rows, total = policy_list(db, filters, page=1, page_size=20, sort_by="发布日期")
    assert metrics["policy_count"] == total
    assert rows.height <= 20
    assert policy_distribution(db, filters).height <= 5
    assert policy_trend(db, filters).height > 0
