from __future__ import annotations

import yaml

from policydb.scope import load_cities_105
from policydb.settings import Settings
from policydb.source_discovery import REQUIRED_ROLES


def main() -> None:
    settings = Settings.discover()
    cities = load_cities_105(settings)
    payload = {
        "version": 1,
        "scope": "CRPD 105 cities",
        "required_roles": list(REQUIRED_ROLES),
        "recommended_roles": [
            "development_reform_department",
            "local_financial_regulator",
            "tax_department",
            "public_resource_trading_center",
            "administrative_approval_department",
            "urban_renewal_or_expropriation_department",
        ],
        "supplemental_roles": [
            "statistics_department",
            "civil_affairs_department",
            "state_assets_department",
            "official_news_or_policy_interpretation",
        ],
        "cities": [
            {
                "city_id": row["city_id"],
                "city_name": row["city_name"],
                "province_name": row["province_name"],
                "required_roles": list(REQUIRED_ROLES),
            }
            for row in cities.sort(["province_name", "city_name"]).iter_rows(named=True)
        ],
    }
    path = settings.root / "data/reference/city_source_requirements.yaml"
    path.write_text(yaml.safe_dump(payload, allow_unicode=True, sort_keys=False), encoding="utf-8")
    print(path)


if __name__ == "__main__":
    main()
