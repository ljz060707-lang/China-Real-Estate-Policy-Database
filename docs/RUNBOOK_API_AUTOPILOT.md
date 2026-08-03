# API Autopilot runbook

All commands resolve the repository with `git rev-parse --show-toplevel` and
use `D:\Data Set\CRPD` as the runtime data root. This round deliberately does
not submit or push GitHub changes and does not start full source completion or
105-city crawling.

## Dry-run plan

```powershell
powershell -ExecutionPolicy Bypass -File scripts\run_api_autopilot.ps1 `
  -Mode source-to-full -MaxSlots 20 -MaxAiCalls 40 -Concurrency 4
```

Equivalent module entrypoint:

```powershell
python -m policydb.autopilot_cli plan --mode source-to-full
```

The plan command performs no AI request and no network search. It writes the
source queue, full-crawl shard plan, provider capability audit, and a dry-run
GO gate.

## Bounded execution after review

```powershell
powershell -ExecutionPolicy Bypass -File scripts\run_api_autopilot.ps1 `
  -Mode source-completion -Apply -Resume -RunId <run_id>
```

Use `scripts\status_api_autopilot.ps1 -RunId <run_id>` for status,
`scripts\stop_api_autopilot.ps1 -RunId <run_id>` for a safe stop, and
`scripts\resume_api_autopilot.ps1 -RunId <run_id>` after repair. A 401/403
provider error stops API work; a 402 balance error is checkpointed without
repeated paid calls; 429 and limited 5xx retry according to config.

## Full-crawl gate

`source-to-full` cannot enter full crawl unless `go_no_go.json` is `GO`:
525 required, verified, enabled, direct-healthy, and parser-ready slots;
zero unresolved and zero enabled-unverified slots; passed tests; no writer
conflict; and valid archive/AI gates. Development config keeps automatic full
crawl disabled. Even after GO, the full stage requires explicit
`-AutoFullCrawl`.
