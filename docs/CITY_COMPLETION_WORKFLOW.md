# City completion workflow

Inspect a city:

```powershell
.\.venv\Scripts\python.exe -m policydb.autopilot_cli city status --city-id CITY_110000
.\.venv\Scripts\python.exe -m policydb.autopilot_cli city report --city-id CITY_110000
```

Run a bounded city pass:

```powershell
.\.venv\Scripts\python.exe -m policydb.autopilot_cli city fast-ingest --city-id CITY_110000 --apply
```

`city complete` uses the existing full-sync controller for the five roles and remains subject to source verification and crawl evidence. `source resume --source-id ... --apply` resumes one registered source. Dashboard city actions generate the same validated jobs and never edit source Parquet directly.

Choose city IDs from `source_requirement_slots.parquet`; do not invent a city identifier. A city may remain `SKIPPED_DEPENDENCY`, `RETRY_WAIT` or `HUMAN_REVIEW` while other cities progress.
