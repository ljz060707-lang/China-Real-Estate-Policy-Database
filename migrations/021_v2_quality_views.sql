CREATE OR REPLACE VIEW v_source_city_matrix AS
WITH explicit_city AS (
    SELECT s.source_id, s.source_name, s.scope_type, s.agency_type,
           s.required_level, s.crawl_enabled, s.is_valid,
           unnest(s.city_ids) AS city_id,
           s.coverage_start_date, s.coverage_end_date, s.expected_frequency
    FROM source_registry s
), national AS (
    SELECT s.source_id, s.source_name, s.scope_type, s.agency_type,
           s.required_level, s.crawl_enabled, s.is_valid, c.city_id,
           s.coverage_start_date, s.coverage_end_date, s.expected_frequency
    FROM source_registry s CROSS JOIN cities_105 c
    WHERE s.scope_type='national'
), provincial AS (
    SELECT s.source_id, s.source_name, s.scope_type, s.agency_type,
           s.required_level, s.crawl_enabled, s.is_valid, c.city_id,
           s.coverage_start_date, s.coverage_end_date, s.expected_frequency
    FROM source_registry s JOIN cities_105 c
      ON list_contains(s.province_codes, CAST(c.province_code AS VARCHAR))
    WHERE s.scope_type='provincial'
)
SELECT DISTINCT * FROM (
    SELECT * FROM explicit_city
    UNION ALL SELECT * FROM national
    UNION ALL SELECT * FROM provincial
) WHERE is_valid;

CREATE OR REPLACE VIEW v_city_month_coverage AS
WITH months AS (
    SELECT period::DATE AS month_start
    FROM range(DATE '2018-01-01', date_trunc('month', current_date) + INTERVAL 1 MONTH,
               INTERVAL 1 MONTH) t(period)
), grid AS (
    SELECT c.city_id, c.city_name, c.province_name, m.month_start
    FROM cities_105 c CROSS JOIN months m
), expected AS (
    SELECT g.*, m.source_id, m.required_level
    FROM grid g LEFT JOIN v_source_city_matrix m ON g.city_id=m.city_id
      AND (m.coverage_start_date IS NULL OR TRY_CAST(m.coverage_start_date AS DATE) <= last_day(g.month_start))
      AND (m.coverage_end_date IS NULL OR TRY_CAST(m.coverage_end_date AS DATE) >= g.month_start)
      AND m.crawl_enabled
), latest_windows AS (
    SELECT * EXCLUDE(rn) FROM (
      SELECT w.*, row_number() OVER (
        PARTITION BY source_id, city_id, date_trunc('month', CAST(period_start AS DATE))
        ORDER BY CAST(finished_at AS TIMESTAMP) DESC NULLS LAST
      ) rn FROM crawl_source_windows w
    ) WHERE rn=1
), aggregate AS (
    SELECT e.city_id, e.city_name, e.province_name, e.month_start,
           count(DISTINCT e.source_id) FILTER (WHERE e.source_id IS NOT NULL) expected_source_count,
           count(DISTINCT w.source_id) scanned_source_count,
           count(DISTINCT w.source_id) FILTER (WHERE w.is_complete) complete_source_count,
           count(DISTINCT w.source_id) FILTER (WHERE w.coverage_status='complete_policy_found') found_source_count,
           coalesce(sum(w.policy_count), 0)::BIGINT discovered_policy_count,
           coalesce(sum(w.error_count), 0)::BIGINT error_count
    FROM expected e LEFT JOIN latest_windows w
      ON e.source_id=w.source_id AND e.city_id=w.city_id
     AND date_trunc('month', CAST(w.period_start AS DATE))=e.month_start
    GROUP BY ALL
)
SELECT *,
       CASE
         WHEN expected_source_count=0 OR scanned_source_count=0 THEN 'not_scanned'
         WHEN complete_source_count=expected_source_count AND found_source_count>0 THEN 'complete_policy_found'
         WHEN complete_source_count=expected_source_count THEN 'complete_confirmed_zero'
         WHEN error_count>0 AND scanned_source_count=0 THEN 'failed'
         ELSE 'partial'
       END AS coverage_status,
       CASE WHEN expected_source_count=0 THEN NULL
            ELSE complete_source_count::DOUBLE/expected_source_count END AS coverage_rate
FROM aggregate;

CREATE OR REPLACE VIEW v_field_confidence_summary AS
SELECT record_id,
       count(*) AS scored_field_count,
       avg(confidence_score) AS mean_field_confidence,
       min(confidence_score) AS minimum_field_confidence,
       0.70*avg(confidence_score)+0.30*min(confidence_score) AS record_confidence,
       bool_or(review_required) AS review_required,
       count(*) FILTER (WHERE conflict_status<>'none') AS conflict_count
FROM field_confidence GROUP BY record_id;

CREATE OR REPLACE VIEW v_dedup_audit AS
SELECT dedup_level, decision, count(*) AS decision_count,
       avg(score) AS mean_score, min(created_at) AS first_decision_at,
       max(created_at) AS last_decision_at
FROM dedup_decisions GROUP BY dedup_level, decision;

CREATE OR REPLACE VIEW v_source_coverage_gaps AS
SELECT c.*, m.source_id, m.source_name, m.agency_type, m.required_level,
       CASE WHEN m.source_id IS NULL THEN 'unmapped_source'
            WHEN NOT m.crawl_enabled THEN 'source_disabled'
            WHEN c.coverage_status='not_scanned' THEN 'not_scanned'
            WHEN c.coverage_status='partial' THEN 'partial_scan'
            ELSE NULL END AS gap_type
FROM v_city_month_coverage c
LEFT JOIN v_source_city_matrix m USING(city_id)
WHERE c.coverage_status NOT IN ('complete_policy_found','complete_confirmed_zero');

INSERT OR IGNORE INTO schema_migrations (migration_id, description)
VALUES ('021_v2_quality_views', 'Create V2 coverage, dedup and confidence quality views');
