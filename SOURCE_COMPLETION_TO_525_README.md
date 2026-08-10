# CRPD Source Completion Controller — 525/525

This bundle drives the repository's existing strict Autopilot in bounded,
resumable batches until all 525 source slots pass the exact source gates, or
until a deterministic/manual/external blocker is reached.

It **never** fabricates URLs or probe hashes, never bypasses
`verify_candidates` / `enable_source_strict`, never runs global promote/enable,
and never starts the full historical crawl.

## Install

Copy these files into the repository:

```text
scripts\run_source_completion_to_525.py
scripts\run_source_completion_to_525.ps1
```

## Preflight / plan

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass `
  -File .\scripts\run_source_completion_to_525.ps1
```

## Start or resume the unattended controller

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass `
  -File .\scripts\run_source_completion_to_525.ps1 `
  -Apply
```

The next launch automatically resumes the latest unfinished controller run.
Use `-NewRun` only to start a separate controller history.

## Status

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass `
  -File .\scripts\run_source_completion_to_525.ps1 `
  -StatusOnly
```

## Graceful stop

```powershell
New-Item `
  "D:\Data Set\CRPD\control\STOP_SOURCE_COMPLETION_TO_525" `
  -ItemType File -Force
```

Remove the stop file before resuming.

## Completion contract

A final 525 baseline is created only when all of these are true:

```text
required_slots                  525
slots_resolved                  525
slots_with_verified_candidate   525
slots_verified                  525
slots_with_enabled_source       525
slots_enabled                   525
slots_direct_healthy            525
slots_parser_ready              525
enabled_unverified_slots          0
slots_unresolved                  0
verified_coverage_pct           100
enabled_coverage_pct            100
```

The controller also requires:

- zero invalid verified probe records;
- zero multi-enabled slots;
- zero active cross-slot canonical URL conflicts;
- full `compileall`, Ruff, pytest, and `git diff --check` success.

## Safe blocker behavior

No truthful program can guarantee that every government site is reachable,
unambiguous, parser-compatible, and free of CAPTCHA/role changes. After six
successful batches with no increase in verified slots, the controller stops
without weakening gates and writes:

```text
D:\Data Set\CRPD\outputs\acceptance\source_completion_to_525\<run_id>\BLOCKERS.json
```

Resolve only the listed blockers, then launch the same command again to resume.
