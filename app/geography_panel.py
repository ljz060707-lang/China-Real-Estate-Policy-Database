from __future__ import annotations

import html
import json

import polars as pl

from policydb import PolicyDB
from policydb.settings import Settings


class GeographyPanelUnavailable(RuntimeError):
    pass


def panel_health(db: PolicyDB) -> dict:
    try:
        columns = db._query("DESCRIBE v_city_month_policy_panel")
        count = int(db._query("SELECT count(*) FROM v_city_month_policy_panel").item())
    except Exception as exc:
        raise GeographyPanelUnavailable(
            "地区研究视图不存在或无法读取。请运行 `policydb build-database`；"
            "云端部署请确认 data/curated/*.parquet 已包含在发布包中。"
        ) from exc
    return {"row_count": count, "columns": columns["column_name"].to_list()}


def filter_options(db: PolicyDB) -> dict[str, list]:
    provinces = db._query(
        "SELECT DISTINCT province FROM v_policy_geography_base "
        "WHERE province IS NOT NULL ORDER BY 1"
    )["province"].to_list()
    cities = db._query(
        "SELECT DISTINCT city_name FROM v_policy_geography_base "
        "WHERE city_name IS NOT NULL ORDER BY 1"
    )["city_name"].to_list()
    years = db._query(
        "SELECT DISTINCT year(record_date)::INTEGER AS year FROM records "
        "WHERE record_date IS NOT NULL ORDER BY 1 DESC"
    )["year"].to_list()
    topics = db._query(
        "SELECT DISTINCT term_name FROM record_terms WHERE taxonomy_name='topic' ORDER BY 1"
    )["term_name"].to_list()
    return {"provinces": provinces, "cities": cities, "years": years, "topics": topics}


def query_region_panel(
    db: PolicyDB,
    level: str,
    *,
    province: str | None = None,
    city: str | None = None,
    year: int | None = None,
    topic: str | None = None,
    limit: int = 50,
    offset: int = 0,
) -> dict[str, pl.DataFrame | int]:
    if level not in {"全国", "省级", "地级市"}:
        raise ValueError(f"unsupported geography level: {level}")
    clauses = ["record_date IS NOT NULL"]
    params: list = []
    if province:
        clauses.append("province=?")
        params.append(province)
    if city:
        clauses.append("city_name=?")
        params.append(city)
    if year:
        clauses.append("year(record_date)=?")
        params.append(year)
    if topic:
        clauses.append(
            "EXISTS(SELECT 1 FROM record_terms t WHERE t.record_id=g.record_id "
            "AND t.taxonomy_name='topic' AND t.term_name=?)"
        )
        params.append(topic)
    where = " AND ".join(clauses)
    if level == "全国":
        region_expr = "'全国'"
    elif level == "省级":
        clauses.append("province IS NOT NULL")
        where = " AND ".join(clauses)
        region_expr = "province"
    else:
        clauses.extend(
            [
                "city_name IS NOT NULL",
                "jurisdiction_level IN ('city','county','county_level_city')",
            ]
        )
        where = " AND ".join(clauses)
        region_expr = "city_name"
    base = (
        "WITH base AS (SELECT DISTINCT g.record_id,g.record_date,g.province,g.city_name,"
        "g.direction,g.official_status,g.source_quality,g.policy_strength FROM "
        f"v_policy_geography_base g WHERE {where}) "
    )
    metrics = (
        "count(DISTINCT record_id)::BIGINT policy_count,"
        "count(DISTINCT CASE WHEN official_status IN ('official','official_reprint') "
        "THEN record_id END)::BIGINT official_policy_count,"
        "count(DISTINCT CASE WHEN direction='tightening' THEN record_id END)::BIGINT tightening_count,"
        "count(DISTINCT CASE WHEN direction IN ('loosening','supportive') THEN record_id END)::BIGINT easing_count,"
        "avg(CASE WHEN official_status IN ('official','official_reprint') "
        "THEN 1.0 ELSE 0.0 END)::DOUBLE official_share,"
        "avg(source_quality::DOUBLE)::DOUBLE source_quality_mean"
    )
    ranking_sql = (
        base
        + f"SELECT {region_expr} AS region,{metrics} FROM base GROUP BY 1 "
        "ORDER BY policy_count DESC,region LIMIT ? OFFSET ?"
    )
    ranking = db._query(ranking_sql, [*params, limit, offset])
    total = int(
        db._query(
            base + f"SELECT count(DISTINCT {region_expr}) FROM base", params
        ).item()
    )
    trend = db._query(
        base
        + "SELECT year(record_date)::INTEGER AS year,month(record_date)::INTEGER AS month,"
        + metrics
        + " FROM base GROUP BY 1,2 ORDER BY 1,2",
        params,
    )
    return {"ranking": ranking, "trend": trend, "total": total}


def tianditu_map_html(regions: list[str], settings: Settings | None = None) -> str | None:
    settings = settings or Settings.discover()
    token = (settings.tianditu_token or "").strip()
    if not token:
        return None
    approval = settings.tianditu_map_approval
    qualification = settings.tianditu_qualification
    safe_token = html.escape(token, quote=True)
    safe_approval = html.escape(approval)
    safe_qualification = html.escape(qualification)
    places = json.dumps(regions[:30], ensure_ascii=False)
    return f"""
    <div id="policy-map" style="height:520px;border:1px solid #e4d9e8"></div>
    <div style="font-size:12px;color:#6b5a72;margin-top:6px">
      天地图底图 · 审图号：{safe_approval} · 测绘资质：{safe_qualification}
    </div>
    <script src="https://api.tianditu.gov.cn/api?v=4.0&tk={safe_token}"></script>
    <script>
      const map = new T.Map('policy-map');
      map.centerAndZoom(new T.LngLat(104.2, 35.8), 4);
      map.addControl(new T.Control.Zoom());
      const names = {places};
      let index = 0;
      function locateNext() {{
        if (index >= names.length) return;
        const name = names[index++];
        const search = new T.LocalSearch(map, {{
          pageCapacity: 1,
          onSearchComplete: function(result) {{
            try {{
              const pois = result.getPois();
              if (pois && pois.length) {{
                const marker = new T.Marker(pois[0].lonlat);
                map.addOverLay(marker);
                marker.addEventListener('click', () => marker.openInfoWindow(
                  new T.InfoWindow(name, {{minWidth: 80}})));
              }}
            }} finally {{ locateNext(); }}
          }}
        }});
        search.search(name);
      }}
      locateNext();
    </script>
    """
