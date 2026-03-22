# T0006 Implementation Report

## Summary

Fixed a runner ordering bug for `receipt` and `bank_statement` where the target `inputs/kari_shiwake/*.csv` could be ingested and moved into `artifacts/ingest/kari_shiwake/` before pre-replacer setup had fully succeeded.

After this change, both in-scope runners delay target ingestion until the pre-replacer setup phase has already succeeded, so pre-replacer failures no longer consume the target inbox file or advance kari_shiwake ingest state.

This task is explicitly limited to `receipt` and `bank_statement`.
`credit_card_statement` was not changed.

## Exact Files Changed

- `belle/line_runners/receipt.py`
- `belle/line_runners/bank_statement.py`
- `tests/test_kari_shiwake_ingest.py`
- `exports/T0006_receipt_bank_pre_replacer_target_commit_order.md`

## Receipt Runner Order

### Before

1. `ensure_client_system_dirs(...)`
2. ingest target kari_shiwake file
3. load lexicon/defaults/overrides
4. load runtime config JSON
5. `ensure_client_cache_updated(...)`
6. `ensure_lexicon_candidates_updated_from_ledger_ref(...)`
7. create run dir / latest path
8. `replace_yayoi_csv(...)`

### After

1. `ensure_client_system_dirs(...)`
2. load lexicon/defaults/overrides
3. load runtime config JSON
4. `ensure_client_cache_updated(...)`
5. `ensure_lexicon_candidates_updated_from_ledger_ref(...)`
6. create run dir / latest path
7. ingest target kari_shiwake file
8. `replace_yayoi_csv(...)`

### New Receipt Commit Point

For `receipt`, the target kari_shiwake input is now ingested only after:

- runtime/config JSON load succeeds
- `ensure_client_cache_updated(...)` succeeds
- `ensure_lexicon_candidates_updated_from_ledger_ref(...)` succeeds
- run directory creation succeeds

The target commit point is the `_ingest_single_kari_input(...)` call immediately before `replace_yayoi_csv(...)`.

## Bank Runner Order

### Before

1. `ensure_client_system_dirs(...)`
2. ingest target kari_shiwake file
3. `ensure_bank_client_cache_updated(...)`
4. load bank runtime config JSON
5. create run dir / latest path / cache path
6. `replace_bank_yayoi_csv(...)`

### After

1. `ensure_client_system_dirs(...)`
2. `ensure_bank_client_cache_updated(...)`
3. load bank runtime config JSON
4. create run dir / latest path / cache path
5. ingest target kari_shiwake file
6. `replace_bank_yayoi_csv(...)`

### New Bank Commit Point

For `bank_statement`, the target kari_shiwake input is now ingested only after:

- `ensure_bank_client_cache_updated(...)` succeeds
- bank runtime/config JSON load succeeds
- run directory creation succeeds

The target commit point is the `_ingest_single_kari_input(...)` call immediately before `replace_bank_yayoi_csv(...)`.

## Covered Pre-Replacer Failure Paths And Final On-Disk State

All covered failure-path assertions are limited to the target kari_shiwake input and its kari_shiwake ingest state. No post-replacer rollback semantics were added or changed.

### 1. Receipt invalid runtime/config JSON

- Trigger: invalid JSON in receipt runtime config before cache update / replacer entry.
- Target source inbox file: remains at the original `clients/<CLIENT_ID>/lines/receipt/inputs/kari_shiwake/target.csv` path.
- kari_shiwake stored ingest files: no new file is created under `clients/<CLIENT_ID>/lines/receipt/artifacts/ingest/kari_shiwake/`.
- kari_shiwake ingest manifest/state: `clients/<CLIENT_ID>/lines/receipt/artifacts/ingest/kari_shiwake_ingested.json` remains unchanged; in the regression test it remains absent.

### 2. Receipt `ensure_client_cache_updated(...)` failure

- Trigger: pre-replacer cache update failure.
- Target source inbox file: remains at the original `clients/<CLIENT_ID>/lines/receipt/inputs/kari_shiwake/target.csv` path.
- kari_shiwake stored ingest files: no new file is created under `clients/<CLIENT_ID>/lines/receipt/artifacts/ingest/kari_shiwake/`.
- kari_shiwake ingest manifest/state: `clients/<CLIENT_ID>/lines/receipt/artifacts/ingest/kari_shiwake_ingested.json` remains unchanged; in the regression test it remains absent.

### 3. Receipt `ensure_lexicon_candidates_updated_from_ledger_ref(...)` failure

- Trigger: pre-replacer lexicon autogrow failure.
- Target source inbox file: remains at the original `clients/<CLIENT_ID>/lines/receipt/inputs/kari_shiwake/target.csv` path.
- kari_shiwake stored ingest files: no new file is created under `clients/<CLIENT_ID>/lines/receipt/artifacts/ingest/kari_shiwake/`.
- kari_shiwake ingest manifest/state: `clients/<CLIENT_ID>/lines/receipt/artifacts/ingest/kari_shiwake_ingested.json` remains unchanged; in the regression test it remains absent.

### 4. Bank `ensure_bank_client_cache_updated(...)` failure

- Trigger: pre-replacer bank cache update failure.
- Target source inbox file: remains at the original `clients/<CLIENT_ID>/lines/bank_statement/inputs/kari_shiwake/target.csv` path.
- kari_shiwake stored ingest files: no new file is created under `clients/<CLIENT_ID>/lines/bank_statement/artifacts/ingest/kari_shiwake/`.
- kari_shiwake ingest manifest/state: `clients/<CLIENT_ID>/lines/bank_statement/artifacts/ingest/kari_shiwake_ingested.json` remains unchanged; in the regression test it remains absent.

### 5. Bank training-pair fail-closed / zero-usable-pair setup failure

- Trigger: real bank training setup raises fail-closed `SystemExit` because pairing produces zero usable pairs before replacer entry.
- Target source inbox file: remains at the original `clients/<CLIENT_ID>/lines/bank_statement/inputs/kari_shiwake/target.csv` path.
- kari_shiwake stored ingest files: no new file is created under `clients/<CLIENT_ID>/lines/bank_statement/artifacts/ingest/kari_shiwake/`.
- kari_shiwake ingest manifest/state: `clients/<CLIENT_ID>/lines/bank_statement/artifacts/ingest/kari_shiwake_ingested.json` remains unchanged; in the regression test it remains absent.

### 6. Bank invalid runtime/config JSON

- Trigger: invalid JSON in `bank_line_config.json` before replacer entry.
- Target source inbox file: remains at the original `clients/<CLIENT_ID>/lines/bank_statement/inputs/kari_shiwake/target.csv` path.
- kari_shiwake stored ingest files: no new file is created under `clients/<CLIENT_ID>/lines/bank_statement/artifacts/ingest/kari_shiwake/`.
- kari_shiwake ingest manifest/state: `clients/<CLIENT_ID>/lines/bank_statement/artifacts/ingest/kari_shiwake_ingested.json` remains unchanged; in the regression test it remains absent.

## Post-Replacer Semantics

This task does not change post-replacer failure semantics, strict-stop behavior after replacement, output cleanup behavior, or any rollback behavior after the replacer has been entered.

## Tests Added / Updated

Updated:

- `tests/test_kari_shiwake_ingest.py`

Added coverage:

- receipt invalid runtime/config JSON keeps target in inbox and does not advance kari ingest state
- receipt `ensure_client_cache_updated(...)` failure keeps target in inbox and does not advance kari ingest state
- receipt `ensure_lexicon_candidates_updated_from_ledger_ref(...)` failure keeps target in inbox and does not advance kari ingest state
- bank `ensure_bank_client_cache_updated(...)` failure keeps target in inbox and does not advance kari ingest state
- bank zero-usable-pair fail-closed setup failure keeps target in inbox and does not advance kari ingest state
- bank invalid runtime/config JSON keeps target in inbox and does not advance kari ingest state

Existing normal behavior coverage retained:

- receipt success path in `tests/test_kari_shiwake_ingest.py`
- bank success paths in `tests/test_bank_line_skill_wiring_smoke.py`

## Test Commands And Results

1. Focused touched-behavior tests

```powershell
python -m unittest tests.test_kari_shiwake_ingest -v
```

Result: `Ran 10 tests in 0.186s` / `OK`

2. Broader receipt/bank-related tests

```powershell
python -m unittest -v tests.test_kari_shiwake_ingest tests.test_bank_line_skill_wiring_smoke tests.test_bank_line_config_passthrough tests.test_bank_cache_pair_learning tests.test_bank_replacer tests.test_input_discovery_hardening tests.test_ledger_ref_ingest_move tests.test_lexicon_autogrow tests.test_receipt_queue_generation_contract
```

Result: `Ran 59 tests in 3.235s` / `OK`

3. Canonical repository validation

```powershell
python tools/run_tests.py
```

Result: `Ran 153 tests in 9.423s` / `OK`

## Residual Risks / Limitations

- The fix is intentionally narrow and only protects pre-replacer failures in `receipt` and `bank_statement`.
- No generic transaction framework was introduced.
- No rollback was added for failures that happen after `replace_yayoi_csv(...)` or `replace_bank_yayoi_csv(...)` has been entered.
- `credit_card_statement` still uses its existing behavior unchanged.

## Final Commit SHA

`PENDING_COMMIT_SHA`
