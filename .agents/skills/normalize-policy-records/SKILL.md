---
name: normalize-policy-records
description: Normalize policy titles, dates, URLs, jurisdictions, organizations, taxonomy, direction, and identity while retaining raw values, evidence, confidence, and review status.
---
# Normalize policy records
1. Read `references/contract.md`.
2. Normalize without overwriting any original field.
3. Use deterministic IDs and URL/title normalization before similarity matching.
4. Save rule/model evidence and confidence; queue ambiguity for review.
5. Run `scripts/run.py` to rebuild and validate.
