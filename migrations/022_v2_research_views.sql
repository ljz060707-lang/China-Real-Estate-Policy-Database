CREATE OR REPLACE VIEW v_city_month_policy_panel_research_ready AS
SELECT p.* EXCLUDE(policy_count, official_policy_count, secondary_only_count,
                   easing_count, tightening_count, neutral_count,
                   purchase_limit_count, sale_limit_count,
                   commercial_mortgage_count, hpf_policy_count,
                   talent_policy_count, subsidy_count, supply_side_count,
                   urban_renewal_count, financing_count, policy_strength_sum,
                   policy_strength_mean, has_policy, data_completeness_score),
       p.policy_count AS observed_policy_count,
       CASE WHEN c.coverage_status IN ('complete_policy_found','complete_confirmed_zero')
            THEN p.policy_count ELSE NULL END AS policy_count,
       CASE WHEN c.coverage_status IN ('complete_policy_found','complete_confirmed_zero')
            THEN p.official_policy_count ELSE NULL END AS official_policy_count,
       CASE WHEN c.coverage_status IN ('complete_policy_found','complete_confirmed_zero')
            THEN p.easing_count ELSE NULL END AS easing_count,
       CASE WHEN c.coverage_status IN ('complete_policy_found','complete_confirmed_zero')
            THEN p.tightening_count ELSE NULL END AS tightening_count,
       CASE WHEN c.coverage_status IN ('complete_policy_found','complete_confirmed_zero')
            THEN p.policy_strength_sum ELSE NULL END AS policy_strength_sum,
       c.coverage_status, c.coverage_rate, c.expected_source_count,
       c.scanned_source_count, c.complete_source_count, c.error_count,
       (c.coverage_status='complete_confirmed_zero') AS is_confirmed_zero
FROM v_city_month_policy_panel_105 p
JOIN v_city_month_coverage c
  ON p.city_id=c.city_id AND p.year=year(c.month_start) AND p.month=month(c.month_start);

CREATE OR REPLACE VIEW v_city_year_policy_panel_research_ready AS
SELECT city_id, city_name, province, city_tier_existing, city_scale_2020, year,
       sum(observed_policy_count)::BIGINT AS observed_policy_count,
       CASE WHEN count(*) FILTER (WHERE coverage_status IN ('complete_policy_found','complete_confirmed_zero'))=count(*)
            THEN sum(policy_count)::BIGINT ELSE NULL END AS policy_count,
       avg(coverage_rate) AS coverage_rate,
       count(*) FILTER (WHERE is_confirmed_zero) AS confirmed_zero_months,
       count(*) FILTER (WHERE coverage_status='not_scanned') AS not_scanned_months,
       count(*) FILTER (WHERE coverage_status='partial') AS partial_months
FROM v_city_month_policy_panel_research_ready GROUP BY ALL;

CREATE OR REPLACE VIEW v_policy_record_confidence AS
SELECT r.record_id, r.record_date, r.title, r.official_status,
       f.scored_field_count, f.mean_field_confidence, f.minimum_field_confidence,
       f.record_confidence, f.conflict_count,
       coalesce(f.review_required, true) AS review_required,
       CASE WHEN f.record_confidence>=0.85 AND r.official_status IN ('official','official_reprint')
                 AND f.conflict_count=0 THEN 'high'
            WHEN f.record_confidence>=0.65 AND f.conflict_count=0 THEN 'review'
            ELSE 'hold' END AS confidence_band
FROM records r LEFT JOIN v_field_confidence_summary f USING(record_id);

CREATE OR REPLACE VIEW v_policy_event_study_research_ready AS
SELECT e.*, c.coverage_status, c.coverage_rate,
       CASE WHEN c.coverage_status IN ('complete_policy_found','complete_confirmed_zero')
            THEN true ELSE false END AS analysis_ready
FROM v_policy_event_study e LEFT JOIN v_city_month_coverage c
  ON e.city_code=c.city_id AND e.year=year(c.month_start) AND e.month=month(c.month_start);

INSERT OR IGNORE INTO schema_migrations (migration_id, description)
VALUES ('022_v2_research_views', 'Create coverage-aware V2 research views');

