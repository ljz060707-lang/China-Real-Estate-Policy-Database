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
