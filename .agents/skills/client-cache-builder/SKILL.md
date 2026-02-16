---
name: client-cache-builder
description: Update append-only client_cache.json from ledger_ref inbox files. Explicit invocation only.
---

# client-cache-builder

Updates `clients/<CLIENT_ID>/artifacts/cache/client_cache.json` from historical finalized journal CSV/TXT files.

## Inputs
1. Place new reference files in `clients/<CLIENT_ID>/inputs/ledger_ref/`.

## Outputs
1. `clients/<CLIENT_ID>/artifacts/cache/client_cache.json`
2. `clients/<CLIENT_ID>/artifacts/ingest/ledger_ref_ingested.json`
3. `clients/<CLIENT_ID>/artifacts/telemetry/client_cache_update_run_<TS>.json`

## Ingest behavior
1. `inputs/ledger_ref/` is an inbox only.
2. On ingest success, files are moved to:
   - `clients/<CLIENT_ID>/artifacts/ingest/ledger_ref/INGESTED_<UTC_TS>_<SHA8>.csv`
3. Duplicate sha files are moved to:
   - `clients/<CLIENT_ID>/artifacts/ingest/ledger_ref/IGNORED_DUPLICATE_<UTC_TS>_<SHA8>.csv`
4. Consumers read ingested files via paths recorded in `ledger_ref_ingested.json`.

## Notes
1. Uses only summary (17th col) and debit account (5th col).
2. Memo (22nd col) is never used.
3. `artifacts/*` is system-managed.

## Execution
```bash
python .agents/skills/client-cache-builder/scripts/build_client_cache.py --client <CLIENT_ID>
```
