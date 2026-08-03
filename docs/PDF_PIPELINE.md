# PDF pipeline

CRPD keeps HTML and PDF as separate, auditable representations. A PDF can be
the primary policy content only when the existing document version says that
the fetched content is PDF; an attachment discovered from an HTML policy page
is retained as `HTML+PDF` and never replaces the HTML record.

## Storage contract

The source file is never moved or overwritten. Existing files are inventoried
read-only and content-addressed copies are written to:

```text
D:\Data Set\CRPD\raw\pdf\objects\<sha256[:2]>\<sha256>.pdf
D:\Data Set\CRPD\raw\pdf\quarantine\<attachment_id>_<sha256>.bin
D:\Data Set\CRPD\derived\pdf_text\<sha256>.json
D:\Data Set\CRPD\manifests\existing_pdf_inventory.parquet
D:\Data Set\CRPD\manifests\pdf_archive_manifest.parquet
```

The curated layer contains `pdf_assets`, `document_attachments`,
`pdf_text_versions`, `pdf_discovery_evidence`, `pdf_download_audit` and
`pdf_processing_events`. `policy_files.parquet` receives a compatibility row
for a linked archived asset so existing policy views continue to expose PDF
availability.

## Bounded commands

All write commands require `--apply`. Limits are deliberate and can be used
for a real smoke test:

```powershell
\.venv\Scripts\python.exe -m policydb.autopilot_cli pdf inventory --root "D:\Data Set\CRPD"
\.venv\Scripts\python.exe -m policydb.autopilot_cli pdf archive --root "D:\Data Set\CRPD" --limit 20 --apply
\.venv\Scripts\python.exe -m policydb.autopilot_cli pdf discover --limit 20 --apply
\.venv\Scripts\python.exe -m policydb.autopilot_cli pdf match --apply
\.venv\Scripts\python.exe -m policydb.autopilot_cli pdf download --limit 10 --workers 4 --apply
\.venv\Scripts\python.exe -m policydb.autopilot_cli pdf parse --limit 10 --workers 2 --apply
\.venv\Scripts\python.exe -m policydb.autopilot_cli pdf report --output "D:\Data Set\CRPD\outputs\pdf_report.json"
```

`download` uses the existing direct Government client through
`RespectfulFetcher`, preserves the discovery-page Referer, validates HTTP
status, content type/magic bytes and SHA-256, writes a `.part` file before an
atomic replace, and keeps a bounded per-source and per-job budget. A non-PDF
response is quarantined for review. No proxy route is accepted as a successful
PDF download.

## Parsing and review

PyMuPDF is the default parser. Each page has a text row and hash, and the full
text JSON is content-addressed by the PDF SHA-256. OCR is intentionally
disabled. A likely image-only file is recorded as `OCR_PENDING`, not as a
successful parse. Missing document association, ambiguous role, or malformed
content stays in the attachment review fields with evidence and error reason.

## Automatic update integration

`FAST_BULK_INGEST` runs bounded PDF discovery/download/parse after the HTML
source stage when `pdf_enabled` is true in `config/continuous_sync.yaml`.
`FullSyncConfig` also exposes the same opt-in stages for a direct source run.
PDF failures are non-blocking for the HTML source result, but remain visible in
the source result, curated audit tables and Dashboard.

## Dashboard and safe viewing

The Streamlit Dashboard has a PDF Pipeline tab under the operation center and
a PDF Completeness tab under the overview. It reads curated Parquet and
manifests only, shows the inventory/link/download/parse funnel, city coverage,
failure reasons and OCR-disabled status, and creates validated jobs rather
than executing shell text. A policy detail view resolves a PDF only by its
`pdf_asset_id` and checks that the final path is under the configured object
root. Arbitrary paths and `..` traversal are rejected.

## Gold boundary

Policy-intensity measurement remains `DISABLED_PLACEHOLDER`. PDF extraction is
not a policy-intensity call and does not create intensity values. Empty or
unmeasured intensity fields remain null/explicitly disabled.
