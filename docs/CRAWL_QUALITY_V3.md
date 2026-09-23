# Collection quality v3

This upgrade keeps collection-only mode: no policy/action/intensity classification
is enabled. It does not replace the crawler or introduce a second database writer.

## Extraction contract

File signatures take precedence over Content-Type. PDF, DOCX and XLSX are parsed
by their existing dedicated parsers. Legacy OLE DOC/XLS, generic ZIP/RAR archives
and unsupported binaries retain their original bytes, but never become HTML text.
They remain `deferred_unsupported`, not parsed or complete. No archived executable
is run. Oversized Office containers are deferred. Scanned PDFs still require OCR;
an empty text layer is `partial`, not a successful OCR result.

HTML prefers explicit government body containers over larger surrounding layout
containers. Direct div text is retained and nested table/paragraph text is emitted
only once. Obvious titled error pages are `soft_error`. Short notices remain
available as `partial`; length is not evidence of policy correctness or completeness.
Parser version is now `3`. HTTP metadata records parser status, reason and selected
body container for new archive objects. Existing raw objects/metadata are immutable.

## Storage and migration

`policy_document_versions` remains the compatibility table. Reuse and archival read
metadata without deserializing the entire `extracted_text` column into Python. A
last-seen patch is joined under the same Parquet write lease as appending new versions;
unrelated fields and prior text are preserved. The table is still atomically rewritten
on append: partitioned storage and multi-process database writes are NOT enabled.

The offline migration is `python -m policydb.crawl.reparse --output <new-directory>`.
Default mode is PREVIEW. It selects binary-looking text, resolves a local archived
asset, verifies its SHA-256, and reparses it without HTTP/API calls. Missing/mismatched
raw assets remain unchanged and are reported. HTML text changes are not bulk-applied.

Apply is only available through `scripts/apply_crawl_quality_v3_when_idle.ps1`.
This launcher creates an owned STOP, waits for the existing write-capable process
chain to finish naturally, acquires `FAST_5Y_WRITER.lock`, applies the migration,
then removes only its own STOP and resumes the Windows scheduled city task. It does
not kill processes. An existing STOP or any failure blocks automatic resume.

Every apply directory contains a complete prior Parquet snapshot plus:

| File | Contract |
|---|---|
| `prior_extractions.parquet` | Full prior rows for changed document versions |
| `extraction_patches.parquet` | Same document_version_id; new extracted_text, parser_version, parse_status, normalized_text_hash, simhash64 |
| `extraction_audit.parquet` | Raw hash/path, before/after parser/text length, reason/status |
| `summary.json` | Input/output snapshot hash, counts, bytes, zero network calls, applied flag |

Empty deferred text has null normalized hash/simhash in the repair. Document identity,
raw hash, local path, title, first/last seen dates and all other evidence stay unchanged.
Previous corrupted extraction bytes remain recoverable in the backup; never restore
the whole backup over newer crawl writes. A rollback must merge affected extraction
fields under the writer lock, after reconciling newer parser revisions.

## Progress definitions

The minute monitor selects the active date range, counts only leaf shards (excludes
`split_parent`), and shows snapshot age. `docs` is the sum of per-shard version
references including cache reuse; it is NOT a distinct-policy count. The configured
writer policy is not reported as a measured process count.

Cached versions satisfy fetching; they are not proof of clean text or complete quality.
New per-shard extraction evidence records main_parsed/main_pending/attachment_pending.
Missing historical extraction evidence yields null, not an invented percentage.
AI-independent crawl progress and strict certification remain separate. Neither
partial text nor unresolved attachments silently become completed policies.

## Verification

Targeted suites cover binary signatures, wrong MIME, containers, nested text, cache
reuse, metadata patches, raw-preserving offline migration, idempotence, leaf/window
progress and existing single-writer/atomic replace guarantees. Production throughput
must be measured after natural worker turnover; synthetic speedups are not an ETA.
