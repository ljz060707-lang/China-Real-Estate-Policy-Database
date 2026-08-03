# Operations

## Bounded fast coverage

```powershell
$env:CRPD_DATA_ROOT = "D:\Data Set\CRPD"
.\.venv\Scripts\python.exe -m policydb.autopilot_cli fast-bulk-ingest --config .\config\continuous_sync.yaml --dry-run
.\.venv\Scripts\python.exe -m policydb.autopilot_cli fast-bulk-ingest --max-cities 5 --apply --resume
```

The default run is bounded and single-writer at the source level. It saves the task result under `D:\Data Set\CRPD\outputs\fast_bulk_ingest` and the global status under the same directory. A STOP file yields at a safe pipeline checkpoint; it does not kill a writer.

## Dashboard

```powershell
.\scripts\start_dashboard.ps1 -NoBrowser
.\scripts\check_dashboard.ps1
.\scripts\stop_dashboard.ps1
.\scripts\restart_dashboard.ps1
```

## Safe recovery

Keep run directories, locks, checkpoints and failure records. If a lock is stale, confirm the recorded PID is not alive before removing only that exact lock. Resume the same run when possible. Do not edit Parquet manually and do not set verified/enabled fields by hand.

API credentials are loaded through the existing Settings/SecretStore path. Logs and job JSON are redacted and must not contain request headers or keys.
