from pathlib import Path

import yaml


def test_all_105_cities_have_five_required_source_roles():
    data = yaml.safe_load(
        Path("data/reference/city_source_requirements.yaml").read_text(encoding="utf-8")
    )
    assert len(data["cities"]) == 105
    required = set(data["required_roles"])
    assert len(required) == 5
    assert all(set(city["required_roles"]) == required for city in data["cities"])
