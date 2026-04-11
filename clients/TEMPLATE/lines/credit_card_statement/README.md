# credit_card_statement operator guide

This line runs with strict single-statement assumptions and audit-first failure handling.

Base path:
1. `clients/<CLIENT_ID>/lines/credit_card_statement/`

## Contract A (Required)

1. One run target file must represent exactly ONE statement for ONE card.
2. Mixed multi-card target CSV in a single file is not supported in the initial implementation.
3. If a source export includes multiple cards, split by statement/card before running.

## Inputs

### `inputs/kari_shiwake`

Strict file-count behavior:
1. `0` files: SKIP (normal no-op)
2. `1` file: RUN
3. `2+` files: FAIL (fail-closed)

### `inputs/ledger_ref`

1. `0+` files are accepted.
2. This directory is append-only historical teacher input for learning.

### `artifacts/derived/cc_teacher`

1. This managed directory is reserved for derived teacher rows extracted from raw `ledger_ref`.
2. The extraction ruleset is tracked at `rulesets/credit_card_statement/teacher_extraction_rules_v1.json`.
3. Cache learning materializes one derived teacher CSV per raw `ledger_ref` source and learns from those derived rows only.
4. Raw `ledger_ref` files remain preserved as the source-of-truth ingest input.

## Learning + Dedup

1. `ledger_ref` (Yayoi finalized export) is the only teacher input.
2. Ingestion uses per-file SHA256 dedupe.
3. Applied SHA256 tracking prevents double-learning, even if you rerun many times with the same teacher files.
4. Add new historical files over time; previously applied files will not increment counts again.
5. Credit-card learning now includes target-side tax division conditioned on `merchant_key + target_account`.

## Credit-card tax replacement

1. Placeholder-side tax division is decided before the shared Yayoi tax postprocess runs.
2. The placeholder side may be `debit` or `credit`; tax replacement writes to that same side.
3. Learned tax routes use `merchant_key + target_account` evidence.
4. Static fallback routes can use non-empty `target_tax_division` from:
   1. `defaults/credit_card_statement/category_defaults.json`
   2. `clients/<CLIENT_ID>/lines/credit_card_statement/config/category_overrides.json`
   3. shared `global_fallback`
5. Blank fallback tax values mean "no fallback" and preserve the current cell.
6. Shared tax postprocess runs after placeholder-side tax replacement and may fill target-side tax amount in the same run.

## Operator config

1. Runtime config path is `config/credit_card_line_config.json`.
2. `target_payable_placeholder_names` is required explicitly for runtime payable-side placeholder detection.
3. `teacher_extraction.canonical_payable_thresholds` is required explicitly for derived-teacher cache learning.
4. Credit-card tax decision thresholds are configured under `tax_division_thresholds`.
5. Learned tax routes currently covered by config are:
   1. `merchant_key_target_account_exact`
   2. `merchant_key_target_account_partial`
6. Shared tax postprocess config path is `clients/<CLIENT_ID>/config/yayoi_tax_config.json`.
7. New clients inherit `clients/TEMPLATE/config/yayoi_tax_config.json`, and the tracked template currently sets `enabled: true`.
8. Runtime canonical payable authority comes from cache `canonical_payable`; raw target placeholder names are not authoritative final payable accounts.

## Failure modes + How to fix

### When strict stop triggers

Strict stop is triggered when either of the following occurs:
1. payable-side subaccount fill is required for at least one row, but file-level card inference is not `OK`
2. payable side is required, but cache `canonical_payable` is not safely usable

Runner exits with code `2`.

### Artifacts to inspect

Under `outputs/runs/<RUN_ID>/`:
1. `run_manifest.json` (runner-level status, strict-stop flag, reasons)
2. replacer manifest JSON (`reports.manifest_json` path from run manifest)
3. review report CSV (`reports.review_report_csv` from replacer manifest)

Artifacts are kept even when strict stop fails the run.

### Typical fixes

1. Split mixed statements so each target file is one card statement (Contract A).
2. Add/accumulate `inputs/ledger_ref` files that cover missing merchants/card patterns.
3. Verify payable subaccount is intentionally blank only where auto-fill should run.
