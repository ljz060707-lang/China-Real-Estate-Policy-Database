import duckdb


def test_main_count(db):
    assert (
        db._query("select count(*) n from records where source_sheet='T1 房地产政策目录'")["n"][0]
        == 3011
    )


def test_date_range(db):
    r = db._query(
        "select min(record_date) a,max(record_date) b from records where source_sheet='T1 房地产政策目录'"
    ).row(0)
    assert str(r[0]) == "2003-06-05" and str(r[1]) == "2026-07-02"


def test_city_month_columns(db):
    cols = set(db.research.city_month_panel().columns)
    assert {"city_code", "year", "month", "policy_count", "source_quality_mean"} <= cols


def test_city_year_panel(db):
    assert "policy_count" in db.research.city_year_panel().columns


def test_event_window(db):
    assert "event_type" in db.research.event_window("purchase_restriction").columns


def test_quality_view(db):
    assert db._query("select * from v_data_quality").height == 1


def test_duckdb_independent(root):
    con = duckdb.connect(str(root / "database" / "policydb.duckdb"), read_only=True)
    assert con.execute("select count(*) from records").fetchone()[0] >= 3011
    con.close()


def test_required_views(db):
    names = set(
        db._query("select table_name from information_schema.views")["table_name"].to_list()
    )
    assert {
        "v_policy_master",
        "v_city_month_policy_panel",
        "v_policy_intensity_index",
        "v_data_quality",
    } <= names


def test_official_share_duckdb_compatible(db):
    value = db._query(
        "SELECT avg(CASE WHEN official_status IN ('official','official_reprint') "
        "THEN 1.0 ELSE 0.0 END) AS official_share FROM records"
    ).item()
    assert 0 <= value <= 1


def test_city_panel_has_normalized_provinces_and_no_boolean_avg(db):
    description = db._query("DESCRIBE v_city_month_policy_panel")
    assert "province" in description["column_name"].to_list()
    assert db._query(
        "SELECT count(*) FROM v_city_month_policy_panel WHERE province IS NOT NULL"
    ).item() > 0
    value = db._query(
        "SELECT avg(CASE WHEN official_policy_count > 0 THEN 1.0 ELSE 0.0 END) "
        "FROM v_city_month_policy_panel"
    ).item()
    assert 0 <= value <= 1


def test_105_city_research_views_have_complete_grid(db):
    panel = db.research.city_month_panel_105("2018-01-01", "2018-12-31")
    assert panel.height == 105 * 12
    assert panel["city_id"].n_unique() == 105


def test_105_city_year_panel_api(db):
    panel = db.research.city_year_panel_105("2018-01-01", "2026-12-31")
    assert panel["city_id"].n_unique() == 105
    assert "data_completeness_score" in panel.columns


def test_uncertain_province_relations_are_excluded_from_research_panel(db):
    long_count = db._query(
        "SELECT count(*) FROM v_policy_105_cities WHERE needs_review"
    ).item()
    assert long_count > 0
    confirmed = db._query(
        "SELECT count(*) FROM v_policy_105_cities WHERE NOT needs_review"
    ).item()
    assert db._query(
        "SELECT sum(policy_count) FROM v_city_month_policy_panel_105"
    ).item() == confirmed
