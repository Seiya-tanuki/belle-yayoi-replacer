---
name: client-cache-builder
description: Update (append-only) clients/<CLIENT_ID>/artifacts/cache/client_cache.json from inputs/ledger_ref (with sha256+rename ingestion). Explicit invocation only.
---

# client-cache-builder

Updates the per-client append-only cache `clients/<CLIENT_ID>/artifacts/cache/client_cache.json` from historical finalized journal CSVs.

## Inputs
1. `clients/<CLIENT_ID>/inputs/ledger_ref/*.csv` (append-only batches)

## Outputs
1. `clients/<CLIENT_ID>/artifacts/cache/client_cache.json` (append-only cache; grows over time)
2. `clients/<CLIENT_ID>/artifacts/ingest/ledger_ref_ingested.json` (sha256 ingest manifest)
3. `clients/<CLIENT_ID>/artifacts/telemetry/client_cache_update_run_<TS>.json` (internal run log)

## Artifact policy
1. `artifacts/*` is system-managed.
2. Users should not manually edit files under `artifacts/`.

## Notes
1. Uses only:
   1. 摘要 (17th col) to derive keys (T-number, vendor_key, category)
   2. 借方勘定科目 (5th col) as the label distribution
2. Does NOT use memo (22nd col).
3. Will rename input files on ingest to `INGESTED_<UTC_TS>_<SHA8>.csv`.

## Execution
```bash
python .agents/skills/client-cache-builder/scripts/build_client_cache.py --client <CLIENT_ID>
```

