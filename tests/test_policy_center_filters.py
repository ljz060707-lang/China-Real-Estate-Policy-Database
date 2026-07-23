from policydb.dashboard_queries import cities_for_province, filter_options


def test_primary_categories_are_the_five_v2_categories(db):
    options = filter_options(db)
    assert options["primary"] == ["D", "S", "F", "H", "G"]
    assert all(row["secondary_category_code"].startswith(row["primary_category_code"]) for row in options["secondary"])


def test_province_city_options_are_linked(db):
    options = filter_options(db)
    if options["provinces"]:
        cities = cities_for_province(db, options["provinces"][0])
        assert isinstance(cities, list)
