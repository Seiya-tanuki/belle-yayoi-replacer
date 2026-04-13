---
name: client-cache-builder
description: Update append-only per-line client caches from line-specific teacher inputs. Explicit invocation only.
---

# client-cache-builder

Updates per-line `client_cache.json` from historical finalized teacher inputs. Line-specific learning contracts differ.

## Inputs
1. Preferred line layout:
   - `clients/<CLIENT_ID>/lines/receipt/inputs/ledger_ref/`
   - `clients/<CLIENT_ID>/lines/credit_card_statement/inputs/ledger_ref/`
2. Receipt legacy fallback (deprecated):
   - `clients/<CLIENT_ID>/inputs/ledger_ref/`
3. `bank_statement` line layout (legacy fallbackなし):
   - `clients/<CLIENT_ID>/lines/bank_statement/inputs/training/ocr_kari_shiwake/`
   - `clients/<CLIENT_ID>/lines/bank_statement/inputs/training/reference_yayoi/`

## Outputs
1. `.../artifacts/cache/client_cache.json`
2. line-specific ingest manifest(s)
3. `.../artifacts/telemetry/client_cache_update_run_<TS>.json`

Additional credit-card managed outputs:
1. `.../artifacts/derived/cc_teacher/<RAW_SHA256>__cc_teacher.csv`
2. `.../artifacts/derived/cc_teacher_manifest.json`

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
1. `receipt`:
   - raw teacher input is `inputs/ledger_ref/`
   - learning uses summary (17th col) + debit account (5th col)
   - memo (22nd col) is not used
2. `bank_statement`:
   - raw teacher inputs are `inputs/training/ocr_kari_shiwake/` and `inputs/training/reference_yayoi/`
   - learning uses paired bank training data and training ingest manifests
3. `credit_card_statement`:
   - raw teacher input is `inputs/ledger_ref/`
   - learning uses derived teacher rows only; raw `ledger_ref` rows are not learned directly
   - system-managed derived assets live under `artifacts/derived/cc_teacher/`
   - `artifacts/derived/cc_teacher_manifest.json` is the derived-teacher manifest used for raw-to-derived provenance and cache application state
   - telemetry/reporting includes `raw_rows_observed_added`, `derived_rows_selected_added`, `rows_total_added`, `rows_used_added`, and cache `canonical_payable_status`
4. Tracked repository baseline contains no per-client `client_cache.json`; the builder creates the file from client state when absent and then updates it append-only.
5. `artifacts/*` is system-managed.
6. Receipt loads `clients/<CLIENT_ID>/lines/receipt/config/receipt_line_config.json` directly.

## Execution
```bash
python .agents/skills/client-cache-builder/scripts/build_client_cache.py --client <CLIENT_ID> --line receipt
```

```bash
python .agents/skills/client-cache-builder/scripts/build_client_cache.py --client <CLIENT_ID> --line bank_statement
```

```bash
python .agents/skills/client-cache-builder/scripts/build_client_cache.py --client <CLIENT_ID> --line credit_card_statement
```
