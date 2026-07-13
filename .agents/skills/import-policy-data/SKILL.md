---
name: import-policy-data
description: Import Excel, CSV, URL, PDF, or local policy files into immutable Raw, traceable Staging, and validated Curated layers. Use for seed migrations and incremental manual ingestion.
---
# Import policy data
1. Read `references/contract.md`.
2. Hash the input before copying it to Raw; never overwrite a different hash.
3. Run `scripts/run.py <path>` for Excel input.
4. Confirm every sheet/cell is staged and the import manifest names the source hash and batch.
5. Run database validation before accepting Curated writes.
