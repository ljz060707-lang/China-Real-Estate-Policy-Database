from __future__ import annotations

import polars as pl

from policydb.scope import load_cities_105, match_scope_cities
from policydb.settings import Settings


def test_city_scope_has_exactly_105_unique_cities(root):
    cities = load_cities_105(Settings.discover(root))
    assert cities.height == 105
    assert cities["city_id"].n_unique() == 105
    assert cities["city_code"].n_unique() == 105


def test_city_codes_are_six_digits(root):
    cities = load_cities_105(Settings.discover(root))
    assert cities["city_code"].cast(pl.String).str.contains(r"^\d{6}$").all()


def test_city_aliases_do_not_conflict(root):
    cities = load_cities_105(Settings.discover(root))
    alias_owner: dict[str, str] = {}
    for city in cities.iter_rows(named=True):
        for alias in city["aliases"].split("|"):
            assert alias_owner.get(alias, city["city_id"]) == city["city_id"]
            alias_owner[alias] = city["city_id"]


def test_district_maps_to_parent_scope_city(root):
    cities = load_cities_105(Settings.discover(root))
    match = match_scope_cities("河南省郑州市航空港区", cities)
    assert len(match) == 1
    assert match[0]["city_id"] == "CITY_410100"
    assert match[0]["jurisdiction_level"] == "district"
    assert match[0]["district_name"] == "航空港区"


def test_city_scope_links_to_existing_tiers_without_guessing(root):
    cities = load_cities_105(Settings.discover(root))
    assert cities["city_tier_existing"].is_not_null().sum() == 101
    missing = set(
        cities.filter(pl.col("city_tier_existing").is_null())["city_name"].to_list()
    )
    assert missing == {"昆山市", "义乌市", "慈溪市", "晋江市"}

