# Dashboard guide

The overview includes a PDF completeness tab with inventory, valid assets,
linked policy PDFs, downloads, parsed text, OCR-pending records and city-level
coverage. The automation center's PDF Pipeline tab submits bounded validated
jobs (`pdf_inventory`, `pdf_archive`, `pdf_discover`, `pdf_download`,
`pdf_parse`, `pdf_match`, `pdf_run`) to the local operations worker. It never
accepts a shell command or arbitrary path. Policy detail previews use the
content-addressed asset id and a path-under-root check.

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

It claims one queued job, writes status, and exits. Jobs and historical status remain under `E:\Data Set\CRPD\control\dashboard_jobs`.
