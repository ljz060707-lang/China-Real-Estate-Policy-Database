from __future__ import annotations

import json
import os
import shutil
from datetime import UTC, datetime
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


def build_database(
    settings: Settings | None = None, *, materialize_geography: bool = True
) -> Path:
    settings = settings or Settings.discover()
    geography_inputs = (
        settings.curated / "record_jurisdictions.parquet",
        settings.curated / "jurisdictions.parquet",
        settings.curated / "cities_105.parquet",
    )
    read_only_host = os.getenv("POLICYDB_READ_ONLY", "").lower() in {"1", "true", "yes"}
    if (
        materialize_geography
        and all(path.exists() for path in geography_inputs)
        and not read_only_host
    ):
        from policydb.geography import materialize_geography

        materialize_geography(settings)
    settings.database.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(settings.database))
    try:
        migrations = sorted((settings.root / "migrations").glob("*.sql"))
        deferred_migrations = [path for path in migrations if path.name >= "021_"]
        for migration in migrations:
            if migration not in deferred_migrations:
                con.execute(migration.read_text(encoding="utf-8"))
        con.execute(
            """CREATE TABLE IF NOT EXISTS manual_review_tasks (
                task_id VARCHAR PRIMARY KEY,
                record_id VARCHAR,
                review_type VARCHAR NOT NULL CHECK (review_type IN (
                    'missing_title','missing_source','invalid_url','low_confidence',
                    'unmatched_t4','unexplained_t2','duplicate_record','coverage_gap',
                    'source_scope_unresolved','field_conflict','low_field_confidence',
                    'source_health_issue','model_disagreement','glm_no_evidence',
                    'rule_glm_numeric_conflict','classifier_direction_conflict',
                    'low_frequency_instrument','out_of_distribution','action_duplicate',
                    'interpretation_false_positive','revision_uncertain','intensity_outlier','other'
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
        review_constraints = " ".join(
            str(row[0])
            for row in con.execute(
                "SELECT constraint_text FROM duckdb_constraints() "
                "WHERE table_name='manual_review_tasks'"
            ).fetchall()
        )
        if "model_disagreement" not in review_constraints:
            con.execute(
                "CREATE OR REPLACE TABLE manual_review_tasks_v2 AS "
                "SELECT * FROM manual_review_tasks"
            )
            con.execute("DROP TABLE manual_review_tasks")
            con.execute("ALTER TABLE manual_review_tasks_v2 RENAME TO manual_review_tasks")
        for parquet in sorted(settings.curated.glob("*.parquet")):
            name = parquet.stem
            parquet_sql = str(parquet).replace("'", "''").replace("\\", "/")
            con.execute(
                f"CREATE OR REPLACE VIEW {name} AS SELECT * FROM read_parquet('{parquet_sql}')"
            )
        staging_excel = settings.root / "data" / "staging" / "excel"
        has_staging_excel = any(staging_excel.glob("*.parquet"))
        if has_staging_excel:
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
        if (settings.curated / "record_geographies_normalized.parquet").exists():
            con.execute(
                "CREATE OR REPLACE VIEW v_policy_geography_long AS "
                "SELECT r.record_id,r.record_date,r.title,g.* FROM records r "
                "JOIN record_geographies_normalized g USING(record_id)"
            )
        else:
            con.execute(
                "CREATE OR REPLACE VIEW v_policy_geography_long AS "
                "SELECT r.record_id,r.record_date,r.title,g.* FROM records r "
                "JOIN record_jurisdictions g USING(record_id)"
            )
        con.execute(
            "CREATE OR REPLACE VIEW v_policy_quantitative_measures AS SELECT * FROM quantitative_measures"
        )
        if (settings.curated / "auto_t4_links.parquet").exists():
            con.execute("""CREATE OR REPLACE VIEW v_policy_features_resolved AS
                SELECT COALESCE(f.record_id,a.record_id) AS record_id,f.feature_name,
                       f.feature_value,f.source_sheet,f.source_cell,
                       CASE WHEN f.record_id IS NULL AND a.record_id IS NOT NULL
                            THEN 'deterministic_auto_match' ELSE 'curated' END AS link_source
                FROM policy_features f LEFT JOIN auto_t4_links a USING(source_cell)""")
        else:
            con.execute("""CREATE OR REPLACE VIEW v_policy_features_resolved AS
                SELECT *, 'curated'::VARCHAR AS link_source FROM policy_features""")
        has_normalized_geography = (
            settings.curated / "record_geographies_normalized.parquet"
        ).exists()
        if has_normalized_geography:
            con.execute("""CREATE OR REPLACE VIEW v_policy_geography_base AS
                SELECT r.record_id,r.record_date,r.title,r.record_type,r.direction,
                       r.official_status,r.source_quality,r.legacy_category,r.source_sheet,
                       g.geography_original,g.province_name AS province,
                       COALESCE(g.parent_city_name,g.city_name) AS city_name,
                       g.city_code,g.county_name,g.jurisdiction_level,
                       COALESCE(p.mandatory_strength,0)::DOUBLE AS policy_strength
                FROM records r JOIN record_geographies_normalized g USING(record_id)
                LEFT JOIN policies p USING(record_id)""")
        else:
            con.execute("""CREATE OR REPLACE VIEW v_policy_geography_base AS
                SELECT r.record_id,r.record_date,r.title,r.record_type,r.direction,
                       r.official_status,r.source_quality,r.legacy_category,r.source_sheet,
                       g.geography_original,NULL::VARCHAR AS province,
                       g.jurisdiction_name AS city_name,g.jurisdiction_id AS city_code,
                       NULL::VARCHAR AS county_name,'unknown'::VARCHAR AS jurisdiction_level,
                       COALESCE(p.mandatory_strength,0)::DOUBLE AS policy_strength
                FROM records r JOIN record_jurisdictions g USING(record_id)
                LEFT JOIN policies p USING(record_id)""")
        con.execute("""CREATE OR REPLACE VIEW v_city_policy_timeline AS
            SELECT g.city_name,r.record_date,r.record_id,r.title,r.direction,r.source_quality,
                   t.term_name topic
            FROM records r JOIN v_policy_geography_base g USING(record_id)
            LEFT JOIN record_terms t USING(record_id)""")
        con.execute("""CREATE OR REPLACE VIEW v_city_month_policy_panel AS
            WITH base AS (
              SELECT DISTINCT record_id,record_date,province,city_name,city_code,direction,
                     official_status,source_quality,legacy_category,policy_strength
              FROM v_policy_geography_base
              WHERE record_date IS NOT NULL AND city_name IS NOT NULL
                AND jurisdiction_level IN ('city','county','county_level_city')
            ) SELECT
              min(city_code)::VARCHAR AS city_code,city_name,province,
              year(record_date)::INTEGER AS "year",month(record_date)::INTEGER AS "month",
              count(DISTINCT record_id)::BIGINT AS policy_count,
              count(DISTINCT CASE WHEN official_status IN ('official','official_reprint') THEN record_id END)::BIGINT AS official_policy_count,
              count(DISTINCT CASE WHEN direction='tightening' THEN record_id END)::BIGINT AS tightening_count,
              count(DISTINCT CASE WHEN direction='loosening' THEN record_id END)::BIGINT AS loosening_count,
              count(DISTINCT CASE WHEN direction='supportive' THEN record_id END)::BIGINT AS supportive_count,
              count(DISTINCT CASE WHEN EXISTS(SELECT 1 FROM record_terms t WHERE t.record_id=base.record_id AND t.term_name='限购') THEN record_id END)::BIGINT AS purchase_restriction_count,
              count(DISTINCT CASE WHEN EXISTS(SELECT 1 FROM record_terms t WHERE t.record_id=base.record_id AND t.term_name='限售') THEN record_id END)::BIGINT AS sale_restriction_count,
              count(DISTINCT CASE WHEN EXISTS(SELECT 1 FROM record_terms t WHERE t.record_id=base.record_id AND t.term_name IN ('商业住房贷款','限贷')) THEN record_id END)::BIGINT AS mortgage_count,
              count(DISTINCT CASE WHEN EXISTS(SELECT 1 FROM record_terms t WHERE t.record_id=base.record_id AND t.term_name='公积金') THEN record_id END)::BIGINT AS provident_fund_count,
              count(DISTINCT CASE WHEN EXISTS(SELECT 1 FROM record_terms t WHERE t.record_id=base.record_id AND t.term_name='购房补贴') THEN record_id END)::BIGINT AS subsidy_count,
              count(DISTINCT CASE WHEN EXISTS(SELECT 1 FROM record_terms t WHERE t.record_id=base.record_id AND t.term_name='人才住房') THEN record_id END)::BIGINT AS talent_policy_count,
              count(DISTINCT CASE WHEN legacy_category LIKE '%供给%' THEN record_id END)::BIGINT AS supply_side_count,
              count(DISTINCT CASE WHEN EXISTS(SELECT 1 FROM record_terms t WHERE t.record_id=base.record_id AND t.term_name IN ('城市更新','城中村改造','老旧小区改造','危旧房改造')) THEN record_id END)::BIGINT AS urban_renewal_count,
              sum(policy_strength)::DOUBLE AS policy_strength_sum,
              max(policy_strength)::DOUBLE AS policy_strength_max,
              avg(source_quality::DOUBLE)::DOUBLE AS source_quality_mean
            FROM base GROUP BY city_name,province,year(record_date),month(record_date)""")
        con.execute(
            "CREATE OR REPLACE VIEW v_city_year_policy_panel AS SELECT city_code,city_name,province,year,sum(policy_count) policy_count,sum(official_policy_count) official_policy_count,sum(tightening_count) tightening_count,sum(loosening_count) loosening_count,sum(supportive_count) supportive_count,sum(purchase_restriction_count) purchase_restriction_count,sum(sale_restriction_count) sale_restriction_count,sum(mortgage_count) mortgage_count,sum(provident_fund_count) provident_fund_count,sum(subsidy_count) subsidy_count,sum(talent_policy_count) talent_policy_count,sum(supply_side_count) supply_side_count,sum(urban_renewal_count) urban_renewal_count,sum(policy_strength_sum) policy_strength_sum,max(policy_strength_max) policy_strength_max,avg(source_quality_mean) source_quality_mean FROM v_city_month_policy_panel GROUP BY ALL"
        )
        con.execute("""CREATE OR REPLACE VIEW v_province_month_policy_panel AS
            WITH base AS (
              SELECT DISTINCT record_id,record_date,province,direction,official_status,
                     source_quality,legacy_category,policy_strength
              FROM v_policy_geography_base
              WHERE record_date IS NOT NULL AND province IS NOT NULL
            ) SELECT province,year(record_date)::INTEGER AS "year",
              month(record_date)::INTEGER AS "month",
              count(DISTINCT record_id)::BIGINT AS policy_count,
              count(DISTINCT CASE WHEN official_status IN ('official','official_reprint') THEN record_id END)::BIGINT AS official_policy_count,
              count(DISTINCT CASE WHEN direction='tightening' THEN record_id END)::BIGINT AS tightening_count,
              count(DISTINCT CASE WHEN direction='loosening' THEN record_id END)::BIGINT AS loosening_count,
              count(DISTINCT CASE WHEN direction='supportive' THEN record_id END)::BIGINT AS supportive_count,
              sum(policy_strength)::DOUBLE AS policy_strength_sum,
              max(policy_strength)::DOUBLE AS policy_strength_max,
              avg(source_quality::DOUBLE)::DOUBLE AS source_quality_mean
            FROM base GROUP BY province,year(record_date),month(record_date)""")
        con.execute("""CREATE OR REPLACE VIEW v_national_month_policy_panel AS
            SELECT year(record_date)::INTEGER AS "year",month(record_date)::INTEGER AS "month",
              count(DISTINCT record_id)::BIGINT AS policy_count,
              count(DISTINCT CASE WHEN official_status IN ('official','official_reprint') THEN record_id END)::BIGINT AS official_policy_count,
              count(DISTINCT CASE WHEN direction='tightening' THEN record_id END)::BIGINT AS tightening_count,
              count(DISTINCT CASE WHEN direction='loosening' THEN record_id END)::BIGINT AS loosening_count,
              count(DISTINCT CASE WHEN direction='supportive' THEN record_id END)::BIGINT AS supportive_count,
              avg(source_quality::DOUBLE)::DOUBLE AS source_quality_mean
            FROM records WHERE record_date IS NOT NULL GROUP BY 1,2""")
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
            if has_staging_excel:
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
                staging_metrics = (
                    "(SELECT count(DISTINCT source_sheet_name) FROM staging_excel_cells),"
                    "(SELECT count(*) FROM staging_excel_cells)"
                )
            else:
                staging_metrics = "0::BIGINT,0::BIGINT"
            con.execute(
                f"""CREATE OR REPLACE VIEW v_information_completeness AS
                   SELECT
                     {staging_metrics.split(',')[0]}::BIGINT AS staging_sheet_count,
                     {staging_metrics.split(',', 1)[1]}::BIGINT AS staging_cell_count,
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
        if (
            (settings.curated / "policy_actions.parquet").exists()
            and (settings.curated / "policy_intensity_scores.parquet").exists()
        ):
            con.execute(
                """CREATE OR REPLACE VIEW v_policy_action_intensity AS
                   SELECT a.action_id,a.record_id,a.document_version_id,a.instrument,
                          a.direction,a.clause_text,a.evidence_start,a.evidence_end,
                          a.text_completeness,a.formal_eligible,a.action_status,
                          s.textual_policy_design_intensity,
                          s.textual_implementation_commitment_intensity,
                          s.instrument_calibration_intensity,
                          s.authority_adjusted_intensity,s.quality_adjusted_intensity,
                          s.weight_version,s.score_version,s.formal_status,
                          s.decision_confidence,s.review_required
                   FROM policy_actions a JOIN policy_intensity_scores s USING(action_id)"""
            )
            con.execute(
                """CREATE OR REPLACE VIEW v_policy_textual_intensity AS
                   SELECT r.record_id,r.record_date,r.title,r.official_status,r.official_level,
                          count(DISTINCT i.action_id)::BIGINT AS action_count,
                          avg(i.textual_policy_design_intensity)::DOUBLE
                            AS mean_textual_policy_design_intensity,
                          sum(i.textual_policy_design_intensity)::DOUBLE
                            AS gross_textual_policy_design_intensity,
                          avg(i.textual_implementation_commitment_intensity)::DOUBLE
                            AS textual_implementation_commitment_intensity,
                          avg(i.instrument_calibration_intensity)::DOUBLE
                            AS instrument_calibration_intensity,
                          bool_and(i.formal_status='formal') AS all_actions_formal,
                          bool_or(i.review_required) AS review_required,
                          min(i.decision_confidence)::DOUBLE AS minimum_decision_confidence
                   FROM records r JOIN v_policy_action_intensity i USING(record_id)
                   GROUP BY ALL"""
            )
        intensity_panel = settings.research / "city_month_policy_intensity.parquet"
        if intensity_panel.exists():
            panel_sql = str(intensity_panel).replace("'", "''").replace("\\", "/")
            con.execute(
                "CREATE OR REPLACE VIEW v_city_month_textual_policy_intensity AS "
                f"SELECT * FROM read_parquet('{panel_sql}')"
            )
        for migration in deferred_migrations:
            con.execute(migration.read_text(encoding="utf-8"))
    finally:
        con.close()
    return settings.database


class DatabaseSwapDeferred(RuntimeError):
    def __init__(self, temporary_database: Path) -> None:
        super().__init__(f"数据库正在被占用，临时数据库已保留：{temporary_database}")
        self.temporary_database = temporary_database


def build_database_atomic(settings: Settings, job_id: str) -> Path:
    """Build and validate a private database before replacing the stable file."""
    target = settings.database
    temporary = target.with_name(f"policydb.{job_id}.tmp.duckdb")
    temporary.unlink(missing_ok=True)
    if target.exists():
        shutil.copy2(target, temporary)
    temporary_settings = settings.model_copy(update={"database_path": temporary})
    build_database(temporary_settings, materialize_geography=False)
    connection = duckdb.connect(str(temporary), read_only=True)
    try:
        connection.execute("SELECT count(*) FROM records").fetchone()
        connection.execute("SELECT count(*) FROM v_data_quality").fetchone()
    finally:
        connection.close()
    try:
        os.replace(temporary, target)
    except PermissionError as exc:
        raise DatabaseSwapDeferred(temporary) from exc
    version_path = target.with_suffix(".version.json")
    temp_version = version_path.with_suffix(".json.tmp")
    temp_version.write_text(
        json.dumps(
            {
                "job_id": job_id,
                "updated_at": datetime.now(UTC).isoformat(),
                "database_mtime_ns": target.stat().st_mtime_ns,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    os.replace(temp_version, version_path)
    return target
