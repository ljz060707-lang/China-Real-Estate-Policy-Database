from __future__ import annotations

import re
from datetime import UTC, datetime

import polars as pl

from policydb.scope import load_cities_105
from policydb.settings import Settings
from policydb.transform.normalization import clean_text, stable_id

PROVINCES = {
    "北京市": "11",
    "天津市": "12",
    "河北省": "13",
    "山西省": "14",
    "内蒙古自治区": "15",
    "辽宁省": "21",
    "吉林省": "22",
    "黑龙江省": "23",
    "上海市": "31",
    "江苏省": "32",
    "浙江省": "33",
    "安徽省": "34",
    "福建省": "35",
    "江西省": "36",
    "山东省": "37",
    "河南省": "41",
    "湖北省": "42",
    "湖南省": "43",
    "广东省": "44",
    "广西壮族自治区": "45",
    "海南省": "46",
    "重庆市": "50",
    "四川省": "51",
    "贵州省": "52",
    "云南省": "53",
    "西藏自治区": "54",
    "陕西省": "61",
    "甘肃省": "62",
    "青海省": "63",
    "宁夏回族自治区": "64",
    "新疆维吾尔自治区": "65",
    "香港特别行政区": "81",
    "澳门特别行政区": "82",
}
MUNICIPALITIES = {"北京市", "天津市", "上海市", "重庆市"}
SPECIAL_PARENT_CITIES = {
    "昆山市": "苏州市",
    "义乌市": "金华市",
    "慈溪市": "宁波市",
    "晋江市": "泉州市",
    "张家港市": "苏州市",
    "句容市": "镇江市",
}


def _short_province(name: str) -> str:
    for suffix in ("壮族自治区", "回族自治区", "维吾尔自治区", "自治区", "特别行政区", "省", "市"):
        if name.endswith(suffix):
            return name.removesuffix(suffix)
    return name


def _province_from_text(text: str) -> tuple[str | None, str]:
    for province in sorted(PROVINCES, key=len, reverse=True):
        aliases = (province, _short_province(province))
        for alias in aliases:
            if text.startswith(alias):
                return province, text[len(alias) :]
    return None, text


def _city_alias_index(cities: pl.DataFrame) -> dict[str, dict]:
    index: dict[str, dict] = {}
    for row in cities.iter_rows(named=True):
        aliases = set(str(row["aliases"]).split("|")) | {
            row["city_name"],
            row["city_name_short"],
        }
        for alias in aliases:
            if alias:
                index[alias] = row
    return index


def _learn_city_provinces(values: list[str], city_index: dict[str, dict]) -> dict[str, str]:
    candidates: dict[str, set[str]] = {}
    for raw in values:
        text = clean_text(raw) or ""
        province, rest = _province_from_text(text)
        if not province:
            continue
        match = re.match(r"^([^/、,，;；]{1,12}?(?:市|自治州|地区|盟))", rest)
        if match:
            city = match.group(1)
            candidates.setdefault(city, set()).add(province)
    for city in city_index.values():
        candidates.setdefault(city["city_name"], set()).add(city["province_name"])
    return {city: next(iter(provinces)) for city, provinces in candidates.items() if len(provinces) == 1}


def normalize_geography(
    value: object,
    cities: pl.DataFrame,
    learned_city_provinces: dict[str, str] | None = None,
) -> list[dict]:
    text = clean_text(value)
    if not text:
        return []
    if text in {"全国", "中央", "国家", "全国范围"}:
        return [
            {
                "geography_original": text,
                "name_normalized": "全国",
                "province_name": None,
                "province_code": None,
                "city_name": None,
                "city_code": None,
                "city_id": None,
                "county_name": None,
                "parent_city_name": None,
                "jurisdiction_level": "national",
                "match_method": "national_rule",
                "match_confidence": 1.0,
                "needs_review": False,
            }
        ]
    city_index = _city_alias_index(cities)
    learned_city_provinces = learned_city_provinces or {}
    parts = [part.strip() for part in re.split(r"[/、,，;；]", text) if part.strip()]
    results = []
    for part in parts:
        province, rest = _province_from_text(part)
        if province in MUNICIPALITIES and not rest:
            rest = province
        matched_city = None
        matched_alias = None
        for alias in sorted(city_index, key=len, reverse=True):
            if alias and (rest.startswith(alias) or part == alias):
                matched_city = city_index[alias]
                matched_alias = alias
                break
        city_name = matched_city["city_name"] if matched_city else None
        city_code = str(matched_city["city_code"]) if matched_city else None
        city_id = matched_city["city_id"] if matched_city else None
        administrative_level = matched_city["administrative_level"] if matched_city else None
        if matched_city:
            province = province or matched_city["province_name"]
        remaining = rest[len(matched_alias) :] if matched_alias and rest.startswith(matched_alias) else ""
        if not city_name:
            city_match = re.match(r"^(.{1,12}?(?:市|自治州|地区|盟))", rest or part)
            if city_match:
                city_name = city_match.group(1)
                remaining = (rest or part)[len(city_name) :]
                province = province or learned_city_provinces.get(city_name)
        if not city_name and (rest or part).endswith("市"):
            city_name = rest or part
            province = province or learned_city_provinces.get(city_name)
        county_match = re.match(r"^(.{1,12}?(?:区|县|旗|市))", remaining)
        county = county_match.group(1) if county_match else None
        parent_city = SPECIAL_PARENT_CITIES.get(city_name or "")
        if parent_city and parent_city in city_index:
            city_code = str(city_index[parent_city]["city_code"])
        if administrative_level == "county_level_city" or parent_city:
            level = "county_level_city"
            county = city_name
        elif county:
            level = "county"
        elif city_name:
            level = "city"
        elif province:
            level = "province"
        else:
            level = "unknown"
        confidence = 1.0 if matched_city else 0.9 if province and city_name else 0.8 if province else 0.65
        normalized_parts = [province]
        if city_name and city_name != province:
            normalized_parts.append(city_name)
        if county and county not in (city_name, province):
            normalized_parts.append(county)
        normalized_parts = [item for item in normalized_parts if item]
        results.append(
            {
                "geography_original": part,
                "name_normalized": "".join(normalized_parts) or part,
                "province_name": province,
                "province_code": PROVINCES.get(province),
                "city_name": city_name,
                "city_code": city_code,
                "city_id": city_id,
                "county_name": county,
                "parent_city_name": parent_city,
                "jurisdiction_level": level,
                "match_method": "city_alias" if matched_city else "province_city_parse" if city_name else "province_rule" if province else "unmatched",
                "match_confidence": confidence,
                "needs_review": confidence < 0.8,
            }
        )
    return results


def materialize_geography(settings: Settings | None = None) -> dict:
    settings = settings or Settings.discover()
    reference_cities = settings.root / "data" / "reference" / "cities_105.csv"
    cities = (
        load_cities_105(settings)
        if reference_cities.exists()
        else pl.read_parquet(settings.curated / "cities_105.parquet")
    )
    links = pl.read_parquet(settings.curated / "record_jurisdictions.parquet")
    jurisdictions = pl.read_parquet(settings.curated / "jurisdictions.parquet")
    values = [str(value) for value in links["geography_original"].drop_nulls().to_list()]
    city_index = _city_alias_index(cities)
    learned = _learn_city_provinces(values, city_index)
    now = datetime.now(UTC).isoformat()
    rows = []
    for link in links.iter_rows(named=True):
        for position, item in enumerate(
            normalize_geography(link["geography_original"], cities, learned), start=1
        ):
            rows.append(
                {
                    "record_geography_id": stable_id(
                        link["record_id"], link["jurisdiction_id"], position, prefix="RGEO"
                    ),
                    "record_id": link["record_id"],
                    "jurisdiction_id": link["jurisdiction_id"],
                    "relation_type": link["relation_type"],
                    **item,
                    "created_at": now,
                    "updated_at": now,
                }
            )
    schema = {
        "record_geography_id": pl.String,
        "record_id": pl.String,
        "jurisdiction_id": pl.String,
        "relation_type": pl.String,
        "geography_original": pl.String,
        "name_normalized": pl.String,
        "province_name": pl.String,
        "province_code": pl.String,
        "city_name": pl.String,
        "city_code": pl.String,
        "city_id": pl.String,
        "county_name": pl.String,
        "parent_city_name": pl.String,
        "jurisdiction_level": pl.String,
        "match_method": pl.String,
        "match_confidence": pl.Float64,
        "needs_review": pl.Boolean,
        "created_at": pl.String,
        "updated_at": pl.String,
    }
    normalized = pl.DataFrame(rows, schema=schema)
    normalized.write_parquet(
        settings.curated / "record_geographies_normalized.parquet", compression="zstd"
    )
    best = (
        normalized.sort(["jurisdiction_id", "match_confidence"], descending=[False, True])
        .unique("jurisdiction_id", keep="first")
        .select(
            "jurisdiction_id",
            "name_normalized",
            "province_name",
            "city_name",
            "county_name",
            "jurisdiction_level",
            "province_code",
            "city_code",
        )
    )
    cleaned = jurisdictions.join(best, on="jurisdiction_id", how="left", suffix="_clean")
    cleaned = cleaned.select(
        pl.col("jurisdiction_id"),
        pl.col("name"),
        pl.col("name_full"),
        pl.coalesce("name_normalized_clean", "name_normalized").alias("name_normalized"),
        pl.coalesce(
            pl.col("city_code").cast(pl.String),
            pl.when(pl.col("province_code").is_not_null())
            .then(pl.concat_str(pl.col("province_code"), pl.lit("0000")))
            .otherwise(None),
        ).alias("administrative_code"),
        pl.coalesce("jurisdiction_level", "level").alias("level"),
        pl.lit(None, dtype=pl.String).alias("parent_id"),
        pl.col("province_name").alias("province"),
        pl.col("city_name").alias("prefecture"),
        pl.col("county_name").alias("county"),
        pl.lit(None, dtype=pl.Float64).alias("longitude"),
        pl.lit(None, dtype=pl.Float64).alias("latitude"),
        pl.col("valid_from").cast(pl.String),
        pl.col("valid_to").cast(pl.String),
    )
    cleaned.write_parquet(settings.curated / "jurisdictions.parquet", compression="zstd")
    return {
        "relation_count": normalized.height,
        "normalized_count": normalized.filter(pl.col("jurisdiction_level") != "unknown").height,
        "unknown_count": normalized.filter(pl.col("jurisdiction_level") == "unknown").height,
        "province_matched_count": normalized["province_name"].is_not_null().sum(),
        "city_matched_count": normalized["city_name"].is_not_null().sum(),
        "county_count": normalized.filter(
            pl.col("jurisdiction_level").is_in(["county", "county_level_city"])
        ).height,
    }
