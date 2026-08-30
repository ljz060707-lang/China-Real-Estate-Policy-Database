# CRPD runtime simplification and throughput rebuild

## Result

The fixed manifest contains 50 enabled official-source entry URLs, selected with unique hosts first. Both runs used manifest SHA-256 `3214473877ac5a2fd42dfe8f3e5b94e37ce0fdc30f3bcdaf12acfe7c92bd4d7f`, robots checks, TLS verification, one request attempt, 8-second read timeout and 4-second connect timeout.

| Metric | Serial baseline | Host-aware, global 6 / host 1 | Change |
|---|---:|---:|---:|
| Wall time | 727.738 s | 190.888 s | 3.812× faster |
| Requests/min | 4.122 | 15.716 | 3.813× |
| Successful documents/min | 1.237 | 4.086 | 3.303× |
| Successful responses | 15 | 13 | live-site variance; no gate relaxed |
| Failures | 35 | 37 | live-site variance; failures remain explicit |
| P95 request latency | 38.055 s | 30.395 s | host isolation reduced queue blocking |

Artifacts:

- `E:\Data Set\CRPD\test_artifacts\runtime_throughput_rebuild\fixed_50_manifest.json`
- `E:\Data Set\CRPD\test_artifacts\runtime_throughput_rebuild\serial_baseline.json`
- `E:\Data Set\CRPD\test_artifacts\runtime_throughput_rebuild\host_aware_optimized.json`

## Simplification

- Lifecycle statuses reduced from 15 stage-shaped values to 7 lifecycle values. Detailed work remains in `stage`.
- Existing `crawl_concurrency`, `document_concurrency` and Dashboard settings now share one fetch implementation.
- Per-item full-frame status rewrites were replaced by one update batch and one atomic commit.
- 13 tracked timestamped backups, totaling 17,896 lines, were deleted after zero-reference checks. No obsolete test was deleted without proof.
- No new broker, database, async framework, controller, watchdog or retry layer was added.

## Safety incident and repair

The first pipeline smoke exposed a real isolation bug: environment storage variables overrode the temporary root and wrote one bounded smoke run into production curated Parquet. The run was stopped at its natural end; no production worker was active during repair. Exact affected rows were backed up and removed under the single write lock with before/after SHA-256 verification. Raw immutable responses were retained rather than deleted.

Repair evidence:

- `E:\Data Set\CRPD\test_artifacts\runtime_throughput_rebuild\pipeline_smoke_20260826T214630\CONTAMINATION_REPAIR_AUDIT.json`
- `E:\Data Set\CRPD\test_artifacts\runtime_throughput_rebuild\pipeline_smoke_20260826T214630\contamination_backup\`

The root cause is fixed by an explicit `archive_path` storage seam and a fail-fast smoke check requiring every resolved path to remain below the test root.

The corrected isolated smoke subsequently passed at:

`E:\Data Set\CRPD\test_artifacts\runtime_throughput_rebuild\pipeline_smoke_isolated_20260826T215748\smoke_result.json`

It processed 12 real URLs at global concurrency 6 / host concurrency 1, persisted 2 unique DocumentVersions and their immutable evidence, reconciled all 12 items to terminal state, then resumed the same run with `fetched=0`. The production database was not touched. The production integrity spot check after repair found 93,924 crawl items and 10,076 versions, zero duplicate primary keys and zero remaining accidental smoke rows.
