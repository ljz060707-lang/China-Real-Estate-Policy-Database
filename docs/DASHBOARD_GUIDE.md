# Dashboard guide

Start locally:

```powershell
.\scripts\start_dashboard.ps1 -NoBrowser
.\scripts\check_dashboard.ps1
```

The URL is `http://127.0.0.1:<port>`. `check_dashboard.ps1` reports the PID, port, health endpoint and log path. Stop with `scripts/stop_dashboard.ps1`.

The overview reports numerators, denominators, percentages, definitions and update times. Tabs cover progress, city/role matrix, dynamic city-year coverage, source health/gaps, architecture, and the Gold disabled placeholder.

Operations require confirmation. Valid actions are `fast_bulk_ingest`, `city_fast_ingest`, `city_complete`, `source_resume`, `refresh_metrics`, and `research_snapshot`. City IDs are checked against the 525-slot registry; source roles are checked against the required role enum. The UI never accepts shell text.

The worker is one-shot and can be scheduled independently:

```powershell
.\.venv\Scripts\python.exe scripts\dashboard_operations_worker.py
```

It claims one queued job, writes status, and exits. Jobs and historical status remain under `D:\Data Set\CRPD\control\dashboard_jobs`.
