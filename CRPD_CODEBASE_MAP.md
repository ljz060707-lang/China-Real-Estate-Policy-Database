# CRPD CODEBASE MAP

Generated: 2026-08-20T01:05:26.864566 (READ-ONLY takeover, no files modified)

## Repositories / roots
- Code repo: `E:\policy-database`
- Production data root: `E:\Data Set\CRPD` (database, raw, curated, outputs, archive, logs, automation state)
- Release bundle: `E:\policy-database` (csv/excel/parquet + manifests)
- Optional analysis workspace: `E:\Data Set\CRPD\analysis` (override with `CRPD_ANALYSIS_ROOT`)

## Source layout (src/policydb)
### app\__init__.py  (1 modules)
- app\__init__.py
### app\automation_center.py  (1 modules)
- app\automation_center.py
### app\crawl_center.py  (1 modules)
- app\crawl_center.py
### app\dashboard.py  (1 modules)
- app\dashboard.py
### app\dashboard_pages.py  (1 modules)
- app\dashboard_pages.py
### app\exhaustive_progress.py  (1 modules)
- app\exhaustive_progress.py
### app\geography_panel.py  (1 modules)
- app\geography_panel.py
### app\intensity_panel.py  (1 modules)
- app\intensity_panel.py
### app\overview.py  (1 modules)
- app\overview.py
### app\pages  (1 modules)
- app\pages\manual_review.py
### app\policy_center.py  (1 modules)
- app\policy_center.py
### app\quality_center.py  (1 modules)
- app\quality_center.py
### app\review_center.py  (1 modules)
- app\review_center.py
### app\settings_page.py  (1 modules)
- app\settings_page.py  [NET]
### app\setup_wizard.py  (1 modules)
- app\setup_wizard.py
### app\theme.py  (1 modules)
- app\theme.py
### app\ui.py  (1 modules)
- app\ui.py
### scripts\_finalize_fast_track_plan_041.py  (1 modules)
- scripts\_finalize_fast_track_plan_041.py  [ENTRY]
### scripts\_recover_existing_candidates_fast_track.py  (1 modules)
- scripts\_recover_existing_candidates_fast_track.py  [DBW,ENTRY]
### scripts\align_verified_and_enabled_sources.py  (1 modules)
- scripts\align_verified_and_enabled_sources.py  [ENTRY]
### scripts\analyze_probe_integrity_impact.py  (1 modules)
- scripts\analyze_probe_integrity_impact.py  [ENTRY]
### scripts\apply_manual_review_to_525.py  (1 modules)
- scripts\apply_manual_review_to_525.py  [DBW,ENTRY]
### scripts\audit_ep930_api_failures.py  (1 modules)
- scripts\audit_ep930_api_failures.py  [ENTRY]
### scripts\audit_ep930_timeout_chain.py  (1 modules)
- scripts\audit_ep930_timeout_chain.py  [ENTRY]
### scripts\audit_episode_930_blockers.py  (1 modules)
- scripts\audit_episode_930_blockers.py  [ENTRY]
### scripts\audit_post_dedupe_conflicts.py  (1 modules)
- scripts\audit_post_dedupe_conflicts.py  [ENTRY]
### scripts\audit_verified_probe_integrity.py  (1 modules)
- scripts\audit_verified_probe_integrity.py  [ENTRY]
### scripts\bootstrap.py  (1 modules)
- scripts\bootstrap.py
### scripts\bootstrap_city_scope.py  (1 modules)
- scripts\bootstrap_city_scope.py  [NET,ENTRY]
### scripts\build_database.py  (1 modules)
- scripts\build_database.py
### scripts\build_department_entry_review.py  (1 modules)
- scripts\build_department_entry_review.py  [ENTRY]
### scripts\build_department_entry_slot_shortlist.py  (1 modules)
- scripts\build_department_entry_slot_shortlist.py  [ENTRY]
### scripts\build_ep930_econometric_grade.py  (1 modules)
- scripts\build_ep930_econometric_grade.py  [ENTRY]
### scripts\build_source_525_action_queue.py  (1 modules)
- scripts\build_source_525_action_queue.py  [ENTRY]
### scripts\cleanup_cross_slot_candidates.py  (1 modules)
- scripts\cleanup_cross_slot_candidates.py  [ENTRY]
### scripts\cleanup_multi_enabled_sources.py  (1 modules)
- scripts\cleanup_multi_enabled_sources.py  [ENTRY]
### scripts\close_ep930_treatment_universe.py  (1 modules)
- scripts\close_ep930_treatment_universe.py  [ENTRY]
### scripts\create_release.py  (1 modules)
- scripts\create_release.py
### scripts\create_source_stable_baseline.py  (1 modules)
- scripts\create_source_stable_baseline.py  [ENTRY]
### scripts\crpd_autonomous_controller.py  (1 modules)
- scripts\crpd_autonomous_controller.py  [ENTRY]
### scripts\dashboard_operations_worker.py  (1 modules)
- scripts\dashboard_operations_worker.py  [ENTRY]
### scripts\diagnose_full_sync_blockers.py  (1 modules)
- scripts\diagnose_full_sync_blockers.py  [ENTRY]
### scripts\episode_930_monitor.py  (1 modules)
- scripts\episode_930_monitor.py  [ENTRY]
### scripts\exclude_remaining_candidate_variants.py  (1 modules)
- scripts\exclude_remaining_candidate_variants.py  [ENTRY]
### scripts\export_ep930_initial_collected.py  (1 modules)
- scripts\export_ep930_initial_collected.py  [ENTRY]
### scripts\generate_city_source_requirements.py  (1 modules)
- scripts\generate_city_source_requirements.py  [ENTRY]
### scripts\generate_inventory.py  (1 modules)
- scripts\generate_inventory.py
### scripts\generate_update_reports.py  (1 modules)
- scripts\generate_update_reports.py  [ENTRY]
### scripts\migrate_auto_t4_overlay.py  (1 modules)
- scripts\migrate_auto_t4_overlay.py  [ENTRY]
### scripts\migrate_excel.py  (1 modules)
- scripts\migrate_excel.py
### scripts\monitor_all_cities_since_2018.py  (1 modules)
- scripts\monitor_all_cities_since_2018.py  [ENTRY]
### scripts\native_capture.py  (1 modules)
- scripts\native_capture.py  [ENTRY]
### scripts\organize_collections.py  (1 modules)
- scripts\organize_collections.py  [ENTRY]
### scripts\patch_candidate_exclusion_semantics.py  (1 modules)
- scripts\patch_candidate_exclusion_semantics.py  [ENTRY]
### scripts\probe_candidates_by_candidate_105.py  (1 modules)
- scripts\probe_candidates_by_candidate_105.py  [ENTRY]
### scripts\probe_candidates_by_city_105.py  (1 modules)
- scripts\probe_candidates_by_city_105.py  [ENTRY]
### scripts\quarantine_invalid_probe_sources.py  (1 modules)
- scripts\quarantine_invalid_probe_sources.py  [ENTRY]
### scripts\reclassify_sjz_natural_resources_substitute.py  (1 modules)
- scripts\reclassify_sjz_natural_resources_substitute.py  [ENTRY]
### scripts\reclassify_source_completion_candidates.py  (1 modules)
- scripts\reclassify_source_completion_candidates.py  [DBW,ENTRY]
### scripts\repair_source_registry_enums.py  (1 modules)
- scripts\repair_source_registry_enums.py
### scripts\resolve_ep930_membership.py  (1 modules)
- scripts\resolve_ep930_membership.py  [ENTRY]
### scripts\resolve_safe_duplicate_shortlist.py  (1 modules)
- scripts\resolve_safe_duplicate_shortlist.py  [ENTRY]
### scripts\reverify_safe_duplicate_candidates.py  (1 modules)
- scripts\reverify_safe_duplicate_candidates.py  [ENTRY]
### scripts\run_fast_track_domain_batch.py  (1 modules)
- scripts\run_fast_track_domain_batch.py  [DBW,ENTRY]
### scripts\run_source_completion_to_525.py  (1 modules)
- scripts\run_source_completion_to_525.py  [ENTRY]
### scripts\run_targeted_source_recovery.py  (1 modules)
- scripts\run_targeted_source_recovery.py  [ENTRY]
### scripts\run_targeted_source_recovery_v2.py  (1 modules)
- scripts\run_targeted_source_recovery_v2.py  [ENTRY]
### scripts\source105_metrics.py  (1 modules)
- scripts\source105_metrics.py
### scripts\test_crawl_responsiveness.py  (1 modules)
- scripts\test_crawl_responsiveness.py  [NET,ENTRY]
### scripts\watch_full_sync_health.py  (1 modules)
- scripts\watch_full_sync_health.py  [ENTRY]
### src\policydb  (128 modules)
- src\policydb\ai.py  [NET,AI]
- src\policydb\ai_audit.py
- src\policydb\api.py
- src\policydb\archive.py
- src\policydb\autopilot.py  [ENTRY]
- src\policydb\autopilot_checkpoints.py
- src\policydb\autopilot_cli.py  [ENTRY]
- src\policydb\autopilot_runtime.py  [DBW,ENTRY]
- … +120 more
### tests\conftest.py  (1 modules)
- tests\conftest.py
### tests\test_ai_action_classification.py  (1 modules)
- tests\test_ai_action_classification.py
### tests\test_ai_confidence_router.py  (1 modules)
- tests\test_ai_confidence_router.py
### tests\test_ai_provider.py  (1 modules)
- tests\test_ai_provider.py
### tests\test_archive.py  (1 modules)
- tests\test_archive.py
### tests\test_audit_ep930_api_failures.py  (1 modules)
- tests\test_audit_ep930_api_failures.py
### tests\test_audit_ep930_timeout_chain.py  (1 modules)
- tests\test_audit_ep930_timeout_chain.py
### tests\test_autopilot.py  (1 modules)
- tests\test_autopilot.py
### tests\test_autopilot_cross_batch.py  (1 modules)
- tests\test_autopilot_cross_batch.py
### tests\test_autopilot_runtime.py  (1 modules)
- tests\test_autopilot_runtime.py  [DBW]
### tests\test_branch_integration_smoke.py  (1 modules)
- tests\test_branch_integration_smoke.py
### tests\test_city_scope.py  (1 modules)
- tests\test_city_scope.py
### tests\test_collections.py  (1 modules)
- tests\test_collections.py
### tests\test_confirmed_zero.py  (1 modules)
- tests\test_confirmed_zero.py
### tests\test_coverage_matrix.py  (1 modules)
- tests\test_coverage_matrix.py
### tests\test_coverage_window_completion.py  (1 modules)
- tests\test_coverage_window_completion.py
### tests\test_coverage_windows.py  (1 modules)
- tests\test_coverage_windows.py
### tests\test_crawl.py  (1 modules)
- tests\test_crawl.py  [NET]
### tests\test_crawl_fairness.py  (1 modules)
- tests\test_crawl_fairness.py
### tests\test_crawl_pagination.py  (1 modules)
- tests\test_crawl_pagination.py
### tests\test_crawl_scope_filters.py  (1 modules)
- tests\test_crawl_scope_filters.py
### tests\test_crpd_autonomous_controller.py  (1 modules)
- tests\test_crpd_autonomous_controller.py
### tests\test_d_drive_storage.py  (1 modules)
- tests\test_d_drive_storage.py
### tests\test_dashboard_jobs.py  (1 modules)
- tests\test_dashboard_jobs.py
### tests\test_dashboard_live_state.py  (1 modules)
- tests\test_dashboard_live_state.py
### tests\test_dashboard_metrics.py  (1 modules)
- tests\test_dashboard_metrics.py
### tests\test_dashboard_navigation.py  (1 modules)
- tests\test_dashboard_navigation.py
### tests\test_dashboard_policy_data.py  (1 modules)
- tests\test_dashboard_policy_data.py
### tests\test_dashboard_responsive.py  (1 modules)
- tests\test_dashboard_responsive.py
### tests\test_database_interface_storage.py  (1 modules)
- tests\test_database_interface_storage.py  [DBW]
### tests\test_dedup_identity.py  (1 modules)
- tests\test_dedup_identity.py
### tests\test_deduplication.py  (1 modules)
- tests\test_deduplication.py
### tests\test_deployment_reliability.py  (1 modules)
- tests\test_deployment_reliability.py
### tests\test_ep930_econometric_grade.py  (1 modules)
- tests\test_ep930_econometric_grade.py
### tests\test_ep930_treatment_universe_closure.py  (1 modules)
- tests\test_ep930_treatment_universe_closure.py
### tests\test_episode_930.py  (1 modules)
- tests\test_episode_930.py
### tests\test_episode_930_audit.py  (1 modules)
- tests\test_episode_930_audit.py
### tests\test_episode_930_autorun.py  (1 modules)
- tests\test_episode_930_autorun.py
### tests\test_episode_930_fast_analysis_ready.py  (1 modules)
- tests\test_episode_930_fast_analysis_ready.py
### tests\test_episode_930_monitor.py  (1 modules)
- tests\test_episode_930_monitor.py
### tests\test_episode_930_network_routing.py  (1 modules)
- tests\test_episode_930_network_routing.py
### tests\test_episode_930_production.py  (1 modules)
- tests\test_episode_930_production.py
### tests\test_exhaustive_audit.py  (1 modules)
- tests\test_exhaustive_audit.py  [NET,DBW,ENTRY]
### tests\test_exhaustive_postprocess.py  (1 modules)
- tests\test_exhaustive_postprocess.py
### tests\test_fast_bulk_ingest.py  (1 modules)
- tests\test_fast_bulk_ingest.py  [ENTRY]
### tests\test_fast_runtime_boundaries.py  (1 modules)
- tests\test_fast_runtime_boundaries.py  [ENTRY]
### tests\test_full_sync.py  (1 modules)
- tests\test_full_sync.py  [DBW,ENTRY]
### tests\test_full_sync_acceptance.py  (1 modules)
- tests\test_full_sync_acceptance.py  [DBW]
### tests\test_geography_panel.py  (1 modules)
- tests\test_geography_panel.py
### tests\test_glm.py  (1 modules)
- tests\test_glm.py  [NET]
### tests\test_historical_full_scan.py  (1 modules)
- tests\test_historical_full_scan.py
### tests\test_ingest.py  (1 modules)
- tests\test_ingest.py
### tests\test_job_responsiveness.py  (1 modules)
- tests\test_job_responsiveness.py  [DBW]
### tests\test_multiple_publication_sources.py  (1 modules)
- tests\test_multiple_publication_sources.py
### tests\test_normalization.py  (1 modules)
- tests\test_normalization.py
### tests\test_parquet_store.py  (1 modules)
- tests\test_parquet_store.py
### tests\test_pdf_pipeline.py  (1 modules)
- tests\test_pdf_pipeline.py  [DBW]
### tests\test_policy_action_center.py  (1 modules)
- tests\test_policy_action_center.py  [DBW]
### tests\test_policy_center_detail.py  (1 modules)
- tests\test_policy_center_detail.py
### tests\test_policy_center_filters.py  (1 modules)
- tests\test_policy_center_filters.py
### tests\test_policy_center_queries.py  (1 modules)
- tests\test_policy_center_queries.py
### tests\test_policy_document_extraction.py  (1 modules)
- tests\test_policy_document_extraction.py
### tests\test_policy_intensity.py  (1 modules)
- tests\test_policy_intensity.py
### tests\test_policy_taxonomy_v3.py  (1 modules)
- tests\test_policy_taxonomy_v3.py
### tests\test_promote_versions.py  (1 modules)
- tests\test_promote_versions.py
### tests\test_province_time_coverage.py  (1 modules)
- tests\test_province_time_coverage.py
### tests\test_query.py  (1 modules)
- tests\test_query.py
### tests\test_readme_commands.py  (1 modules)
- tests\test_readme_commands.py
### tests\test_recent_priority.py  (1 modules)
- tests\test_recent_priority.py
### tests\test_relevance_gate.py  (1 modules)
- tests\test_relevance_gate.py
### tests\test_research_cooldown.py  (1 modules)
- tests\test_research_cooldown.py
### tests\test_research_views.py  (1 modules)
- tests\test_research_views.py
### tests\test_review.py  (1 modules)
- tests\test_review.py
### tests\test_review_automation.py  (1 modules)
- tests\test_review_automation.py  [NET]
### tests\test_revision_versioning.py  (1 modules)
- tests\test_revision_versioning.py
### tests\test_rolling_24m.py  (1 modules)
- tests\test_rolling_24m.py
### tests\test_schedule.py  (1 modules)
- tests\test_schedule.py
### tests\test_search_provider_fallback.py  (1 modules)
- tests\test_search_provider_fallback.py
### tests\test_seed_source_candidates.py  (1 modules)
- tests\test_seed_source_candidates.py  [DBW]
### tests\test_semantic_dedup.py  (1 modules)
- tests\test_semantic_dedup.py
### tests\test_smart_crawl.py  (1 modules)
- tests\test_smart_crawl.py  [NET]
### tests\test_source_completion_ai_workflow.py  (1 modules)
- tests\test_source_completion_ai_workflow.py
### tests\test_source_completion_checkpoint.py  (1 modules)
- tests\test_source_completion_checkpoint.py
### tests\test_source_completion_fix.py  (1 modules)
- tests\test_source_completion_fix.py  [DBW]
### tests\test_source_completion_recovery.py  (1 modules)
- tests\test_source_completion_recovery.py  [DBW]
### tests\test_source_discovery_api.py  (1 modules)
- tests\test_source_discovery_api.py  [NET]
### tests\test_source_health_and_replacement.py  (1 modules)
- tests\test_source_health_and_replacement.py
### tests\test_source_matrix_105.py  (1 modules)
- tests\test_source_matrix_105.py
### tests\test_sources.py  (1 modules)
- tests\test_sources.py
### tests\test_stock_and_review_pools.py  (1 modules)
- tests\test_stock_and_review_pools.py
### tests\test_storage_resolver.py  (1 modules)
- tests\test_storage_resolver.py
### tests\test_storage_runtime_layout.py  (1 modules)
- tests\test_storage_runtime_layout.py
### tests\test_supervisor.py  (1 modules)
- tests\test_supervisor.py
### tests\test_t4_matching.py  (1 modules)
- tests\test_t4_matching.py
### tests\test_taxonomy_v2.py  (1 modules)
- tests\test_taxonomy_v2.py
### tests\test_test_evidence.py  (1 modules)
- tests\test_test_evidence.py
### tests\test_topic_fallback_removed.py  (1 modules)
- tests\test_topic_fallback_removed.py
### tests\test_ui_safe_pandas.py  (1 modules)
- tests\test_ui_safe_pandas.py
### tests\test_update_completeness_page.py  (1 modules)
- tests\test_update_completeness_page.py
### tests\test_v2.py  (1 modules)
- tests\test_v2.py  [NET]
### tests\test_verification_audit.py  (1 modules)
- tests\test_verification_audit.py  [DBW]
### tests\test_windows_launcher.py  (1 modules)
- tests\test_windows_launcher.py
### tests\test_windows_schedule.py  (1 modules)
- tests\test_windows_schedule.py

## Production runner layer (scripts/)
- `crpd_autonomous_controller.py` — main autonomous controller (state machine per AUTOMATION_CONFIG.json)
- `run_source_completion_to_525.py` — 525 source-slot completion runner (v2/v2_1/v2_2 PS1 variants exist)
- `run_targeted_source_recovery.py` (+ _v2, .bak) — targeted official recovery
- `run_all_cities_since_2018.ps1` / `CRPD_Run_One_City_Exhaustive*.ps1` — per-city batch runners
- EP930: `build_ep930_econometric_grade.py`, `close_ep930_treatment_universe.py`, `export_ep930_initial_collected.py`, `resolve_ep930_membership.py`, `audit_ep930_*.py`, `episode_930_monitor.py`
- Duplicate/versioned files found: see CRPD_DUPLICATE_FILENAME_CANDIDATES.csv (30 candidates, incl. .bak and _v2/_v2_1/_v2_2)

## Entry points
- Console: `policydb = policydb.cli:app` (pyproject [project.scripts])
- PS1/BAT launchers: start_policydb.cmd → scripts/launch_dashboard.ps1; CRPD_Autonomous_*.ps1; run_*.ps1; episode_930_*.ps1
- Python entrypoints with `if __name__ == '__main__'` / argparse: 113 files (see CRPD_ENTRYPOINT_MATRIX.csv)

## Data flow (verified from DB + automation config)
source slots (525) → source_registry (513) → source_candidates (5,054) → crawl_items (93,924) → documents (3,330) / policy_document_versions (10,076) / attachments (7,431) → records (4,883) → policy_actions (858) / policy_classifications (858) → coverage_gaps (3,971) → manual_review_tasks (7,457) → release
