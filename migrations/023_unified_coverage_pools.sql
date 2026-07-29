CREATE OR REPLACE VIEW v_province_month_coverage AS
SELECT province_name AS province,
       year(month_start)::INTEGER AS year,
       month(month_start)::INTEGER AS month,
       count(DISTINCT city_id)::BIGINT AS city_count,
       sum(expected_source_count)::BIGINT AS registered_core_source_count,
       sum(complete_source_count)::BIGINT AS complete_window_count,
       sum(expected_source_count-complete_source_count)::BIGINT AS incomplete_window_count,
       count(*) FILTER (WHERE coverage_status='not_scanned')::BIGINT AS not_scanned_cells,
       count(*) FILTER (WHERE coverage_status='partial')::BIGINT AS partial_cells,
       count(*) FILTER (WHERE coverage_status='failed')::BIGINT AS failed_cells,
       sum(discovered_policy_count)::BIGINT AS policy_file_count,
       CASE WHEN sum(expected_source_count)=0 THEN NULL
            ELSE sum(complete_source_count)::DOUBLE/sum(expected_source_count) END AS time_coverage_rate
FROM v_city_month_coverage
GROUP BY ALL;

CREATE OR REPLACE VIEW v_province_year_coverage AS
SELECT province,year,
       max(city_count)::BIGINT AS city_count,
       sum(registered_core_source_count)::BIGINT AS registered_core_source_count,
       sum(complete_window_count)::BIGINT AS complete_window_count,
       sum(incomplete_window_count)::BIGINT AS incomplete_window_count,
       sum(not_scanned_cells)::BIGINT AS not_scanned_cells,
       sum(partial_cells)::BIGINT AS partial_cells,
       sum(failed_cells)::BIGINT AS failed_cells,
       sum(policy_file_count)::BIGINT AS policy_file_count,
       CASE WHEN sum(registered_core_source_count)=0 THEN NULL
            ELSE sum(complete_window_count)::DOUBLE/sum(registered_core_source_count) END AS time_coverage_rate
FROM v_province_month_coverage
GROUP BY ALL;

CREATE OR REPLACE VIEW v_source_role_coverage AS
SELECT m.agency_type AS source_role,
       count(DISTINCT m.city_id)::BIGINT AS mapped_city_count,
       count(DISTINCT m.source_id)::BIGINT AS registered_source_count,
       count(DISTINCT CASE WHEN m.crawl_enabled THEN m.source_id END)::BIGINT AS enabled_source_count,
       count(DISTINCT CASE WHEN w.is_complete THEN w.window_id END)::BIGINT AS complete_window_count,
       count(DISTINCT CASE WHEN w.coverage_status='partial' THEN w.window_id END)::BIGINT AS partial_window_count,
       count(DISTINCT CASE WHEN w.coverage_status='failed' THEN w.window_id END)::BIGINT AS failed_window_count
FROM v_source_city_matrix m
LEFT JOIN crawl_source_windows w
  ON m.source_id=w.source_id AND m.city_id=w.city_id
GROUP BY ALL;

CREATE OR REPLACE VIEW v_document_archive_coverage AS
SELECT content_type,
       count(*)::BIGINT AS document_count,
       count(*) FILTER (WHERE archive_status='archived')::BIGINT AS archived_count,
       count(*) FILTER (WHERE archive_status<>'archived')::BIGINT AS missing_or_failed_count,
       CASE WHEN count(*)=0 THEN NULL
            ELSE count(*) FILTER (WHERE archive_status='archived')::DOUBLE/count(*) END AS archive_rate
FROM policy_files
GROUP BY ALL;

INSERT OR IGNORE INTO schema_migrations (migration_id, description)
VALUES ('023_unified_coverage_pools', 'Add province, source-role and archive coverage views');
