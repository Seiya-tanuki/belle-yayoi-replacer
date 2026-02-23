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

## Learning + Dedup

1. `ledger_ref` (Yayoi finalized export) is the only teacher input.
2. Ingestion uses per-file SHA256 dedupe.
3. Applied SHA256 tracking prevents double-learning, even if you rerun many times with the same teacher files.
4. Add new historical files over time; previously applied files will not increment counts again.

## Failure modes + How to fix

### When strict stop triggers

Strict stop is triggered when payable-side subaccount fill is required for at least one row, but file-level card inference is not `OK`.
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
