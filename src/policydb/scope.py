from __future__ import annotations

from datetime import UTC, datetime

import polars as pl

from policydb.settings import Settings
from policydb.transform.normalization import clean_text, stable_id


def load_cities_105(settings: Settings | None = None) -> pl.DataFrame:
    settings = settings or Settings.discover()
    path = settings.root / "data" / "reference" / "cities_105.csv"
    frame = pl.read_csv(path, infer_schema_length=200, null_values=[""])
    if frame.height != 105 or frame["city_id"].n_unique() != 105:
        raise ValueError("cities_105.csv must contain exactly 105 unique city_id values")
    if frame["city_code"].cast(pl.String).str.contains(r"^\d{6}$").not_().any():
        raise ValueError("Every city_code must be a six-digit administrative code")
    return frame


def _aliases(city: dict) -> list[str]:
    return sorted(
        {
            alias.strip()
            for alias in str(city["aliases"]).split("|")
            if alias.strip()
        }
        | {city["city_name"], city["city_name_short"]},
        key=len,
        reverse=True,
    )


def _province_aliases(name: str) -> set[str]:
    aliases = {name}
    for suffix in ("壮族自治区", "回族自治区", "维吾尔自治区", "自治区", "省", "市"):
        if name.endswith(suffix):
            aliases.add(name.removesuffix(suffix))
    return aliases


def match_scope_cities(value: object, cities: pl.DataFrame) -> list[dict]:
    text = clean_text(value)
    if not text:
        return []
    province = next(
        (
            name
            for name in cities["province_name"].unique().to_list()
            if text in _province_aliases(name)
            or text.removesuffix("全省") in _province_aliases(name)
        ),
        None,
    )
    province_rows = cities.filter(pl.col("province_name") == province) if province else None
    is_municipality = bool(
        province_rows is not None
        and province_rows.height == 1
        and province_rows[0, "administrative_level"] == "municipality"
    )
    if province and text not in set(cities["city_name"].to_list()) and not is_municipality:
        return [
            {
                "city_id": city["city_id"],
                "match_method": "province_scope",
                "confidence": 0.75,
                "jurisdiction_level": "province",
                "district_name": None,
                "evidence": f"省级适用范围“{text}”；具体城市适用性待核验",
                "needs_review": True,
            }
            for city in cities.filter(pl.col("province_name") == province).iter_rows(named=True)
        ]
    matches: list[dict] = []
    seen: set[str] = set()
    for city in cities.iter_rows(named=True):
        alias = next((item for item in _aliases(city) if item in text), None)
        if alias and city["city_id"] not in seen:
            suffix = text[text.find(alias) + len(alias) :].strip(" ，,、/；;")
            district = suffix if suffix.endswith(("区", "县", "旗")) else None
            matches.append(
                {
                    "city_id": city["city_id"],
                    "match_method": "alias",
                    "confidence": 1.0 if text in _aliases(city) else 0.9,
                    "jurisdiction_level": "district" if district else "city",
                    "district_name": district,
                    "evidence": f"地域原文“{text}”命中别名“{alias}”",
                    "needs_review": False,
                }
            )
            seen.add(city["city_id"])
    if matches:
        return matches
    return matches


def materialize_city_scope(settings: Settings | None = None) -> dict:
    settings = settings or Settings.discover()
    cities = load_cities_105(settings)
    cities.write_parquet(settings.curated / "cities_105.parquet", compression="zstd")
    records = pl.read_parquet(settings.curated / "records.parquet").select(
        "record_id", "geography_original"
    )
    now = datetime.now(UTC).isoformat()
    rows: list[dict] = []
    for record in records.iter_rows(named=True):
        for match in match_scope_cities(record["geography_original"], cities):
            rows.append(
                {
                    "policy_applicable_city_id": stable_id(
                        record["record_id"],
                        match["city_id"],
                        match["jurisdiction_level"],
                        match["district_name"],
                        prefix="PAC",
                    ),
                    "record_id": record["record_id"],
                    "city_id": match["city_id"],
                    "jurisdiction_level": match["jurisdiction_level"],
                    "district_name": match["district_name"],
                    "relation_source": "deterministic_geography",
                    "match_method": match["match_method"],
                    "confidence": match["confidence"],
                    "needs_review": match["needs_review"],
                    "evidence": match["evidence"],
                    "created_at": now,
                    "updated_at": now,
                }
            )
    schema = {
        "policy_applicable_city_id": pl.String,
        "record_id": pl.String,
        "city_id": pl.String,
        "jurisdiction_level": pl.String,
        "district_name": pl.String,
        "relation_source": pl.String,
        "match_method": pl.String,
        "confidence": pl.Float64,
        "needs_review": pl.Boolean,
        "evidence": pl.String,
        "created_at": pl.String,
        "updated_at": pl.String,
    }
    relations = pl.DataFrame(rows, schema=schema).unique(
        subset=["policy_applicable_city_id"], keep="first"
    )
    relations.write_parquet(
        settings.curated / "policy_applicable_cities.parquet", compression="zstd"
    )
    return {
        "city_count": cities.height,
        "city_id_unique_count": cities["city_id"].n_unique(),
        "tier_match_count": cities["city_tier_existing"].is_not_null().sum(),
        "applicable_city_relation_count": relations.height,
        "direct_relation_count": relations.filter(~pl.col("needs_review")).height,
        "review_required_relation_count": relations.filter(pl.col("needs_review")).height,
    }
