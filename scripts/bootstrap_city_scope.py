from __future__ import annotations

import csv
from io import StringIO
from pathlib import Path

import duckdb
import httpx
import pandas as pd

from policydb.transform.normalization import stable_id

WIKIPEDIA_TRANSCRIPTION = (
    "https://zh.wikipedia.org/wiki/"
    "%E4%B8%AD%E8%8F%AF%E4%BA%BA%E6%B0%91%E5%85%B1%E5%92%8C%E5%9C%8B"
    "%E5%9F%8E%E5%B8%82%E5%9F%8E%E5%8D%80%E5%B8%B8%E4%BD%8F%E4%BA%BA"
    "%E5%8F%A3%E6%8E%92%E5%90%8D?variant=zh-cn"
)
AUTHORITY_URL = (
    "https://ah.mof.gov.cn/lianzhengjianshe/202211/t20221114_3851397.htm"
)
ADMIN_DATA_URL = (
    "https://raw.githubusercontent.com/modood/Administrative-divisions-of-China/"
    "master/dist/cities.csv"
)
ADMIN_TREE_URL = (
    "https://raw.githubusercontent.com/modood/Administrative-divisions-of-China/"
    "master/dist/pcas-code.json"
)

PROVINCE_CODES = {
    "北京": "11",
    "天津": "12",
    "河北": "13",
    "山西": "14",
    "内蒙古": "15",
    "辽宁": "21",
    "吉林": "22",
    "黑龙江": "23",
    "上海": "31",
    "江苏": "32",
    "浙江": "33",
    "安徽": "34",
    "福建": "35",
    "江西": "36",
    "山东": "37",
    "河南": "41",
    "湖北": "42",
    "湖南": "43",
    "广东": "44",
    "广西": "45",
    "海南": "46",
    "重庆": "50",
    "四川": "51",
    "贵州": "52",
    "云南": "53",
    "西藏": "54",
    "陕西": "61",
    "甘肃": "62",
    "青海": "63",
    "宁夏": "64",
    "新疆": "65",
}
PROVINCE_FULL_NAMES = {
    "北京": "北京市",
    "天津": "天津市",
    "上海": "上海市",
    "重庆": "重庆市",
    "内蒙古": "内蒙古自治区",
    "广西": "广西壮族自治区",
    "西藏": "西藏自治区",
    "宁夏": "宁夏回族自治区",
    "新疆": "新疆维吾尔自治区",
}
COUNTY_LEVEL_CITIES = {"昆山市", "义乌市", "慈溪市", "晋江市"}


def main() -> None:
    root = Path(__file__).resolve().parents[1]
    client = httpx.Client(headers={"User-Agent": "PolicyDBResearch/0.1"}, timeout=60)
    census_table = pd.read_html(StringIO(client.get(WIKIPEDIA_TRANSCRIPTION).text))[3]
    census_table = census_table.iloc[:105]
    city_col, province_col, scale_col = (
        census_table.columns[2],
        census_table.columns[3],
        census_table.columns[5],
    )
    cities = pd.read_csv(
        StringIO(client.get(ADMIN_DATA_URL).content.decode("utf-8")), dtype=str
    )
    admin_tree = client.get(ADMIN_TREE_URL).json()
    county_codes = {
        district["name"]: district["code"]
        for province in admin_tree
        for prefecture in province.get("children", [])
        for district in prefecture.get("children", [])
    }
    with duckdb.connect(str(root / "database" / "policydb.duckdb"), read_only=True) as con:
        tiers = dict(
            con.execute(
                "SELECT jurisdiction_id,attribute_value FROM jurisdiction_attributes "
                "WHERE attribute_name='city_tier'"
            ).fetchall()
        )

    rows = []
    for _, source in census_table.iterrows():
        city = str(source[city_col]).strip()
        province_short = str(source[province_col]).strip()
        province_code = PROVINCE_CODES[province_short]
        if city in COUNTY_LEVEL_CITIES:
            city_code = county_codes[city]
            administrative_level = "county_city"
        elif province_short in {"北京", "天津", "上海", "重庆"}:
            city_code = province_code + "0000"
            administrative_level = "municipality"
        else:
            match = cities[
                (cities["name"] == city) & (cities["provinceCode"] == province_code)
            ]
            if len(match) != 1:
                raise ValueError(f"Administrative-code match failed: {city}, {province_short}")
            city_code = match.iloc[0]["code"] + "00"
            administrative_level = "prefecture"
        city_short = city.removesuffix("市")
        province_name = PROVINCE_FULL_NAMES.get(province_short, province_short + "省")
        rows.append(
            {
                "city_id": f"CITY_{city_code}",
                "city_name": city,
                "city_name_short": city_short,
                "province_name": province_name,
                "province_code": province_code,
                "city_code": city_code,
                "administrative_level": administrative_level,
                "aliases": f"{city_short}|{city}|{province_name}{city}",
                "city_tier_existing": tiers.get(stable_id(city_short, prefix="JUR")),
                "city_scale_2020": str(source[scale_col]),
                "is_large_city_105": "true",
                "scope_version": "2020-census-v1",
                "scope_source_name": "《2020中国人口普查分县资料》",
                "scope_source_date": "2020-11-01",
                "scope_source_url": AUTHORITY_URL,
                "valid_from": "2020-11-01",
                "valid_to": "",
            }
        )
    if len(rows) != 105 or len({row["city_id"] for row in rows}) != 105:
        raise ValueError("The large-city scope must contain exactly 105 unique cities")
    output = root / "data" / "reference" / "cities_105.csv"
    with output.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    print(f"wrote {len(rows)} cities to {output}")


if __name__ == "__main__":
    main()
