# CRPD DEPENDENCY GRAPH (baseline 2026-08-20T01:05:26.864566)

- start_policydb.cmd → scripts/launch_dashboard.ps1 → streamlit app (app/)
- CRPD_Autonomous_Worker.ps1 / CRPD_Autonomous_Supervisor.ps1 →
  crpd_autonomous_controller.py → src/policydb.autopilot → jobs/ (managers, workers) →
  crawl/ (discovery, fetch, pipeline) → parse → classify → dedup → storage (single writer) →
  coverage → recovery → archive → release
- run_source_completion_to_525.py → seed_source_candidates / source completion → source_registry
- run_targeted_source_recovery*.py → recovery path
- EP930: episode_930_production.py + scripts → membership → actions → date audit → closure → export
- CLI: policydb.cli:app exposes research API + maintenance commands

Verified entrypoints: see CRPD_ENTRYPOINT_MATRIX.csv (113 files).
