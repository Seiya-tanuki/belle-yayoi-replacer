---
name: client-cache-builder
description: Update append-only client_cache.json from ledger_ref inbox files. Explicit invocation only.
---

# client-cache-builder

Updates per-line `client_cache.json` from historical finalized journal CSV/TXT files.

## Inputs
1. Preferred line layout:
   - `clients/<CLIENT_ID>/lines/receipt/inputs/ledger_ref/`
2. Receipt legacy fallback (deprecated):
   - `clients/<CLIENT_ID>/inputs/ledger_ref/`
3. `bank_statement` line layout (legacy fallbackなし):
   - `clients/<CLIENT_ID>/lines/bank_statement/inputs/training/ocr_kari_shiwake/`
   - `clients/<CLIENT_ID>/lines/bank_statement/inputs/training/reference_yayoi/`

## Outputs
1. `.../artifacts/cache/client_cache.json`
2. `.../artifacts/ingest/ledger_ref_ingested.json`
3. `.../artifacts/telemetry/client_cache_update_run_<TS>.json`

## Ingest behavior
1. `inputs/ledger_ref/` is an inbox only.
2. On ingest success, files are moved to:
   - `.../artifacts/ingest/ledger_ref/INGESTED_<UTC_TS>_<SHA8>.csv`
3. Duplicate sha files are moved to:
   - `.../artifacts/ingest/ledger_ref/IGNORED_DUPLICATE_<UTC_TS>_<SHA8>.csv`
4. Consumers read ingested files via paths recorded in `ledger_ref_ingested.json`.
5. `bank_statement` uses training ingest manifests (`training_ocr_ingested.json`, `training_reference_ingested.json`).
6. `receipt`, `bank_statement`, and `credit_card_statement` are implemented/runnable (line-specific contracts apply).

## Notes
1. Uses only summary (17th col) and debit account (5th col).
2. Memo (22nd col) is never used.
3. `artifacts/*` is system-managed.

## Execution
```bash
python .agents/skills/client-cache-builder/scripts/build_client_cache.py --client <CLIENT_ID> --line receipt
```

```bash
python .agents/skills/client-cache-builder/scripts/build_client_cache.py --client <CLIENT_ID> --line bank_statement
```
