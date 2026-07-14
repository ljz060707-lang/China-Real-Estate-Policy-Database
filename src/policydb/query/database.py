from __future__ import annotations

from pathlib import Path

import duckdb

from policydb.settings import Settings

VIEW_ALIASES = [
    "v_policy_latest_version",
    "v_policy_source_quality",
    "v_policy_relations",
    "v_official_statements",
    "v_supply_side_measures",
    "v_demand_side_measures",
    "v_white_list_events",
    "v_psl_financing_events",
    "v_urban_renewal_policies",
]


def build_database(settings: Settings | None = None) -> Path:
    settings = settings or Settings.discover()
    settings.database.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(settings.database))
    try:
        for migration in sorted((settings.root / "migrations").glob("*.sql")):
            con.execute(migration.read_text(encoding="utf-8"))
        con.execute(
            """CREATE TABLE IF NOT EXISTS manual_review_tasks (
                task_id VARCHAR PRIMARY KEY,
                record_id VARCHAR,
                review_type VARCHAR NOT NULL CHECK (review_type IN (
                    'missing_title','missing_source','invalid_url','low_confidence',
                    'unmatched_t4','unexplained_t2','duplicate_record','other'
                )),
                field_name VARCHAR,
                source_sheet VARCHAR,
                source_cell VARCHAR,
                old_value VARCHAR,
                suggested_value VARCHAR,
                confidence DOUBLE,
                status VARCHAR NOT NULL DEFAULT 'pending' CHECK (status IN (
                    'pending','approved','corrected','rejected','ignored'
                )),
                review_note VARCHAR,
                evidence_url VARCHAR,
                created_at TIMESTAMP WITH TIME ZONE NOT NULL,
                updated_at TIMESTAMP WITH TIME ZONE NOT NULL
            )"""
        )
        for parquet in sorted(settings.curated.glob("*.parquet")):
            name = parquet.stem
            parquet_sql = str(parquet).replace("'", "''").replace("\\", "/")
            con.execute(
                f"CREATE OR REPLACE VIEW {name} AS SELECT * FROM read_parquet('{parquet_sql}')"
            )
        staging_excel = settings.root / "data" / "staging" / "excel"
        if any(staging_excel.glob("*.parquet")):
            staging_glob = str(staging_excel / "*.parquet").replace("'", "''").replace(
                "\\", "/"
            )
            con.execute(
                "CREATE OR REPLACE VIEW staging_excel_cells AS "
                f"SELECT * FROM read_parquet('{staging_glob}', union_by_name=true)"
            )
        con.execute("""CREATE OR REPLACE VIEW v_policy_master AS
            SELECT r.*, g.jurisdiction_name AS city_name, g.geography_original,
                   string_agg(DISTINCT t.term_name, '、') AS topics
            FROM records r LEFT JOIN record_jurisdictions g USING(record_id)
            LEFT JOIN record_terms t USING(record_id) GROUP BY ALL""")
        con.execute(
            "CREATE OR REPLACE VIEW v_policy_topic_long AS SELECT r.record_id,r.record_date,r.title,t.* FROM records r JOIN record_terms t USING(record_id)"
        )
        con.execute(
            "CREATE OR REPLACE VIEW v_policy_instrument_long AS SELECT * FROM v_policy_topic_long WHERE taxonomy_name='instrument'"
        )
        con.execute(
            "CREATE OR REPLACE VIEW v_policy_geography_long AS SELECT r.record_id,r.record_date,r.title,g.* FROM records r JOIN record_jurisdictions g USING(record_id)"
        )
        con.execute(
            "CREATE OR REPLACE VIEW v_policy_quantitative_measures AS SELECT * FROM quantitative_measures"
        )
        con.execute("""CREATE OR REPLACE VIEW v_city_policy_timeline AS SELECT g.jurisdiction_name city_name,r.record_date,r.record_id,r.title,r.direction,r.source_quality,t.term_name topic
            FROM records r JOIN record_jurisdictions g USING(record_id) LEFT JOIN record_terms t USING(record_id)""")
        con.execute("""CREATE OR REPLACE VIEW v_city_month_policy_panel AS SELECT
            g.jurisdiction_id city_code,g.jurisdiction_name city_name,NULL::VARCHAR province,
            year(r.record_date)::INTEGER AS "year",month(r.record_date)::INTEGER AS "month",count(DISTINCT r.record_id)::BIGINT policy_count,
            count(DISTINCT CASE WHEN r.official_status IN ('official','official_reprint') THEN r.record_id END)::BIGINT official_policy_count,
            count(DISTINCT CASE WHEN r.direction='tightening' THEN r.record_id END)::BIGINT tightening_count,
            count(DISTINCT CASE WHEN r.direction='loosening' THEN r.record_id END)::BIGINT loosening_count,
            count(DISTINCT CASE WHEN r.direction='supportive' THEN r.record_id END)::BIGINT supportive_count,
            count(DISTINCT CASE WHEN t.term_name='限购' THEN r.record_id END)::BIGINT purchase_restriction_count,
            count(DISTINCT CASE WHEN t.term_name='限售' THEN r.record_id END)::BIGINT sale_restriction_count,
            count(DISTINCT CASE WHEN t.term_name IN ('商业住房贷款','限贷') THEN r.record_id END)::BIGINT mortgage_count,
            count(DISTINCT CASE WHEN t.term_name='公积金' THEN r.record_id END)::BIGINT provident_fund_count,
            count(DISTINCT CASE WHEN t.term_name='购房补贴' THEN r.record_id END)::BIGINT subsidy_count,
            count(DISTINCT CASE WHEN t.term_name='人才住房' THEN r.record_id END)::BIGINT talent_policy_count,
            count(DISTINCT CASE WHEN r.legacy_category LIKE '%供给%' THEN r.record_id END)::BIGINT supply_side_count,
            count(DISTINCT CASE WHEN t.term_name IN ('城市更新','城中村改造','老旧小区改造') THEN r.record_id END)::BIGINT urban_renewal_count,
            sum(COALESCE(p.mandatory_strength,0))::DOUBLE policy_strength_sum,max(COALESCE(p.mandatory_strength,0))::DOUBLE policy_strength_max,
            avg(r.source_quality)::DOUBLE source_quality_mean
            FROM records r JOIN record_jurisdictions g USING(record_id) LEFT JOIN record_terms t USING(record_id) LEFT JOIN policies p USING(record_id)
            WHERE r.record_date IS NOT NULL GROUP BY 1,2,3,4,5""")
        con.execute(
            "CREATE OR REPLACE VIEW v_city_year_policy_panel AS SELECT city_code,city_name,province,year,sum(policy_count) policy_count,sum(official_policy_count) official_policy_count,sum(tightening_count) tightening_count,sum(loosening_count) loosening_count,sum(supportive_count) supportive_count,sum(purchase_restriction_count) purchase_restriction_count,sum(sale_restriction_count) sale_restriction_count,sum(mortgage_count) mortgage_count,sum(provident_fund_count) provident_fund_count,sum(subsidy_count) subsidy_count,sum(talent_policy_count) talent_policy_count,sum(supply_side_count) supply_side_count,sum(urban_renewal_count) urban_renewal_count,sum(policy_strength_sum) policy_strength_sum,max(policy_strength_max) policy_strength_max,avg(source_quality_mean) source_quality_mean FROM v_city_month_policy_panel GROUP BY ALL"
        )
        con.execute(
            "CREATE OR REPLACE VIEW v_province_month_policy_panel AS SELECT province,year,month,sum(policy_count) policy_count FROM v_city_month_policy_panel GROUP BY ALL"
        )
        con.execute(
            "CREATE OR REPLACE VIEW v_policy_event_study AS SELECT *,0::INTEGER event_time FROM v_city_month_policy_panel"
        )
        con.execute(
            "CREATE OR REPLACE VIEW v_policy_intensity_index AS SELECT *,policy_strength_sum*COALESCE(source_quality_mean,0)/5 standardized_intensity FROM v_city_month_policy_panel"
        )
        con.execute(
            "CREATE OR REPLACE VIEW v_data_quality AS SELECT count(*) record_count,count(*) FILTER(WHERE title IS NULL) missing_title_count,count(*) FILTER(WHERE full_text IS NULL) missing_full_text_count,count(*) FILTER(WHERE primary_source_url IS NULL) missing_url_count,count(*) FILTER(WHERE manual_review_status='pending') pending_review_count FROM records"
        )
        con.execute(
            "CREATE OR REPLACE VIEW v_official_statements AS SELECT * FROM records WHERE record_type IN ('official_statement','meeting_statement','government_report')"
        )
        con.execute(
            "CREATE OR REPLACE VIEW v_supply_side_measures AS SELECT * FROM v_policy_master WHERE legacy_category LIKE '%供给%'"
        )
        con.execute(
            "CREATE OR REPLACE VIEW v_demand_side_measures AS SELECT * FROM v_policy_master WHERE legacy_category NOT LIKE '%供给%' OR legacy_category IS NULL"
        )
        con.execute(
            "CREATE OR REPLACE VIEW v_white_list_events AS SELECT * FROM records WHERE source_sheet LIKE '房地产项目白名单%'"
        )
        con.execute(
            "CREATE OR REPLACE VIEW v_psl_financing_events AS SELECT * FROM records WHERE source_sheet='PSL专项贷款'"
        )
        con.execute(
            "CREATE OR REPLACE VIEW v_urban_renewal_policies AS SELECT DISTINCT r.* FROM records r JOIN record_terms t USING(record_id) WHERE t.term_name IN ('城市更新','城中村改造','老旧小区改造','危旧房改造')"
        )
        con.execute(
            "CREATE OR REPLACE VIEW v_policy_latest_version AS SELECT * FROM v_policy_master"
        )
        con.execute(
            "CREATE OR REPLACE VIEW v_policy_source_quality AS SELECT record_id,title,official_status,source_quality,primary_source_url FROM records"
        )
        con.execute("CREATE OR REPLACE VIEW v_policy_relations AS SELECT * FROM policy_relations")
        if (settings.curated / "record_collections.parquet").exists():
            con.execute(
                """CREATE OR REPLACE VIEW v_policy_collection_long AS
                   SELECT r.record_id,r.record_date,r.title,r.record_type,r.official_status,
                          r.direction,r.source_quality,r.primary_source_url,r.source_sheet,
                          c.collection_code,c.collection_name,c.subcollection_code,
                          c.subcollection_name,c.classification_source,c.confidence,
                          c.evidence_excerpt,c.review_status,c.is_primary
                   FROM records r JOIN record_collections c USING(record_id)"""
            )
            con.execute(
                """CREATE OR REPLACE VIEW v_policy_library_summary AS
                   SELECT collection_code,collection_name,subcollection_code,
                          subcollection_name,count(DISTINCT record_id)::BIGINT record_count,
                          avg(confidence)::DOUBLE confidence_mean,
                          count(DISTINCT CASE WHEN review_status='pending' THEN record_id END)::BIGINT
                              pending_review_count
                   FROM record_collections GROUP BY ALL
                   ORDER BY collection_name,subcollection_name NULLS LAST"""
            )
            con.execute(
                """CREATE OR REPLACE VIEW v_source_collection_coverage AS
                   SELECT s.source_sheet,s.collection_code,s.collection_name,
                          s.subcollection_code,s.subcollection_name,s.source_kind,
                          count(e.source_cell)::BIGINT staging_cell_count,
                          count(e.source_cell) FILTER(WHERE e.is_formula)::BIGINT formula_count,
                          count(e.source_cell) FILTER(WHERE e.is_merged)::BIGINT merged_cell_count
                   FROM source_sheet_collections s
                   LEFT JOIN staging_excel_cells e
                     ON s.source_sheet=e.source_sheet_name
                   WHERE s.mapping_role='primary'
                   GROUP BY ALL ORDER BY s.source_sheet"""
            )
            con.execute(
                """CREATE OR REPLACE VIEW v_information_completeness AS
                   SELECT
                     (SELECT count(DISTINCT source_sheet_name) FROM staging_excel_cells)::BIGINT
                       AS staging_sheet_count,
                     (SELECT count(*) FROM staging_excel_cells)::BIGINT AS staging_cell_count,
                     (SELECT count(DISTINCT source_sheet) FROM source_sheet_collections)::BIGINT
                       AS mapped_sheet_count,
                     (SELECT count(*) FROM records)::BIGINT AS record_count,
                     (SELECT count(DISTINCT record_id) FROM record_collections)::BIGINT
                       AS classified_record_count,
                     (SELECT count(*) FROM record_collections)::BIGINT
                       AS record_collection_relation_count"""
            )
        if (
            (settings.curated / "cities_105.parquet").exists()
            and (settings.curated / "policy_applicable_cities.parquet").exists()
        ):
            con.execute(
                """CREATE OR REPLACE VIEW v_policy_105_cities AS
                   SELECT r.record_id,r.record_date,r.publication_date,r.title,r.summary,
                          r.full_text,r.direction,r.official_status,r.source_quality,
                          r.primary_source_url,r.official_level,r.source_sheet,
                          c.city_id,c.city_name,c.province_name AS province,
                          c.city_tier_existing,c.city_scale_2020,
                          a.jurisdiction_level,a.district_name,a.match_method,
                          a.confidence AS geography_confidence,a.needs_review,
                          EXISTS(SELECT 1 FROM record_terms t WHERE t.record_id=r.record_id
                            AND t.term_name='限购') AS purchase_limit,
                          EXISTS(SELECT 1 FROM record_terms t WHERE t.record_id=r.record_id
                            AND t.term_name='限售') AS sale_limit,
                          EXISTS(SELECT 1 FROM record_terms t WHERE t.record_id=r.record_id
                            AND t.term_name IN ('商业住房贷款','限贷')) AS commercial_mortgage,
                          EXISTS(SELECT 1 FROM record_terms t WHERE t.record_id=r.record_id
                            AND t.term_name='公积金') AS hpf_policy,
                          EXISTS(SELECT 1 FROM record_terms t WHERE t.record_id=r.record_id
                            AND t.term_name='人才住房') AS talent_policy,
                          EXISTS(SELECT 1 FROM record_terms t WHERE t.record_id=r.record_id
                            AND t.term_name='购房补贴') AS subsidy,
                          EXISTS(SELECT 1 FROM record_collections x WHERE x.record_id=r.record_id
                            AND x.collection_code IN ('housing_urban_rural_development',
                              'natural_resources')) AS supply_side,
                          EXISTS(SELECT 1 FROM record_collections x WHERE x.record_id=r.record_id
                            AND x.subcollection_code='urban_renewal') AS urban_renewal,
                          EXISTS(SELECT 1 FROM record_collections x WHERE x.record_id=r.record_id
                            AND x.collection_code='financial_regulation') AS financing,
                          COALESCE(p.mandatory_strength,0) AS policy_strength
                   FROM records r JOIN policy_applicable_cities a USING(record_id)
                   JOIN cities_105 c USING(city_id) LEFT JOIN policies p USING(record_id)
                   WHERE r.record_date >= DATE '2018-01-01'"""
            )
            con.execute(
                """CREATE OR REPLACE VIEW v_city_month_policy_panel_105 AS
                   WITH months AS (
                     SELECT period::DATE month_start
                     FROM range(DATE '2018-01-01', current_date + INTERVAL 1 MONTH,
                                INTERVAL 1 MONTH) t(period)
                   ), grid AS (
                     SELECT c.*,m.month_start FROM cities_105 c CROSS JOIN months m
                   )
                   SELECT g.city_id,g.city_name,g.province_name AS province,
                          g.city_tier_existing,g.city_scale_2020,
                          year(g.month_start)::INTEGER AS year,
                          month(g.month_start)::INTEGER AS month,
                          count(DISTINCT p.record_id)::BIGINT policy_count,
                          count(DISTINCT CASE WHEN p.official_status IN
                            ('official','official_reprint') THEN p.record_id END)::BIGINT
                            official_policy_count,
                          count(DISTINCT CASE WHEN p.record_id IS NOT NULL AND p.official_status NOT IN
                            ('official','official_reprint') THEN p.record_id END)::BIGINT
                            secondary_only_count,
                          count(DISTINCT CASE WHEN p.direction IN ('loosening','supportive')
                            THEN p.record_id END)::BIGINT easing_count,
                          count(DISTINCT CASE WHEN p.direction='tightening'
                            THEN p.record_id END)::BIGINT tightening_count,
                          count(DISTINCT CASE WHEN p.direction='neutral'
                            THEN p.record_id END)::BIGINT neutral_count,
                          count(DISTINCT CASE WHEN p.purchase_limit THEN p.record_id END)::BIGINT
                            purchase_limit_count,
                          count(DISTINCT CASE WHEN p.sale_limit THEN p.record_id END)::BIGINT
                            sale_limit_count,
                          count(DISTINCT CASE WHEN p.commercial_mortgage THEN p.record_id END)::BIGINT
                            commercial_mortgage_count,
                          count(DISTINCT CASE WHEN p.hpf_policy THEN p.record_id END)::BIGINT
                            hpf_policy_count,
                          count(DISTINCT CASE WHEN p.talent_policy THEN p.record_id END)::BIGINT
                            talent_policy_count,
                          count(DISTINCT CASE WHEN p.subsidy THEN p.record_id END)::BIGINT subsidy_count,
                          count(DISTINCT CASE WHEN p.supply_side THEN p.record_id END)::BIGINT
                            supply_side_count,
                          count(DISTINCT CASE WHEN p.urban_renewal THEN p.record_id END)::BIGINT
                            urban_renewal_count,
                          count(DISTINCT CASE WHEN p.financing THEN p.record_id END)::BIGINT
                            financing_count,
                          COALESCE(sum(p.policy_strength),0)::DOUBLE policy_strength_sum,
                          avg(p.policy_strength)::DOUBLE policy_strength_mean,
                          (count(DISTINCT p.record_id)>0) AS has_policy,
                          CASE WHEN count(DISTINCT p.record_id)=0 THEN 0.0 ELSE
                            round(0.6*count(DISTINCT CASE WHEN p.official_status IN
                              ('official','official_reprint') THEN p.record_id END)
                              / count(DISTINCT p.record_id)
                              +0.2*count(DISTINCT CASE WHEN p.full_text IS NOT NULL
                                THEN p.record_id END)/count(DISTINCT p.record_id)
                              +0.2*count(DISTINCT CASE WHEN p.primary_source_url IS NOT NULL
                                THEN p.record_id END)/count(DISTINCT p.record_id),4)
                          END AS data_completeness_score
                   FROM grid g LEFT JOIN v_policy_105_cities p
                     ON g.city_id=p.city_id
                    AND date_trunc('month',p.record_date)=g.month_start
                    AND NOT p.needs_review
                   GROUP BY ALL"""
            )
            con.execute(
                """CREATE OR REPLACE VIEW v_city_year_policy_panel_105 AS
                   SELECT city_id,city_name,province,city_tier_existing,city_scale_2020,year,
                          sum(policy_count)::BIGINT policy_count,
                          sum(official_policy_count)::BIGINT official_policy_count,
                          sum(secondary_only_count)::BIGINT secondary_only_count,
                          sum(easing_count)::BIGINT easing_count,
                          sum(tightening_count)::BIGINT tightening_count,
                          sum(neutral_count)::BIGINT neutral_count,
                          sum(purchase_limit_count)::BIGINT purchase_limit_count,
                          sum(sale_limit_count)::BIGINT sale_limit_count,
                          sum(commercial_mortgage_count)::BIGINT commercial_mortgage_count,
                          sum(hpf_policy_count)::BIGINT hpf_policy_count,
                          sum(talent_policy_count)::BIGINT talent_policy_count,
                          sum(subsidy_count)::BIGINT subsidy_count,
                          sum(supply_side_count)::BIGINT supply_side_count,
                          sum(urban_renewal_count)::BIGINT urban_renewal_count,
                          sum(financing_count)::BIGINT financing_count,
                          sum(policy_strength_sum)::DOUBLE policy_strength_sum,
                          avg(policy_strength_mean)::DOUBLE policy_strength_mean,
                          bool_or(has_policy) AS has_policy,
                          avg(data_completeness_score)::DOUBLE data_completeness_score
                   FROM v_city_month_policy_panel_105 GROUP BY ALL"""
            )
    finally:
        con.close()
    return settings.database
