from policydb.dashboard_queries import policy_detail, policy_list


def test_policy_detail_loads_after_record_selection(db):
    rows, _ = policy_list(db, {"start_date": "2018-01-01"}, page=1, page_size=1, sort_by="发布日期")
    record_id = rows[0, "record_id"]
    policy, actions, files = policy_detail(db, record_id)
    assert policy and policy["record_id"] == record_id
    assert actions.height >= 1
    assert files.columns
