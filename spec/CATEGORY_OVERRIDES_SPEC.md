# CATEGORY_OVERRIDES_SPEC (belle.category_overrides.v2)

## Purpose

`clients/<CLIENT_ID>/lines/<line_id>/config/category_overrides.json` is a per-client, per-line editable overlay for shared target-side fallback rules.
The generated file is full-expanded from the current shared `lexicon/lexicon.json` category keys
and the bookkeeping-mode-selected tracked defaults variant for the line, but runtime loading is best-effort and applies
only validated rows. These generated files are not tracked as repository assets in the current
baseline.
The live row contract is `target_account` / `target_tax_division`; older v1-style `debit_account`
wording is not part of the current runtime contract.
Phase D line scope:
1. `receipt`: used.
2. `credit_card_statement`: used.
3. `bank_statement`: not used.

## Schema

Top-level keys:
1. `schema`: string, must be `belle.category_overrides.v2`
2. `client_id`: string
3. `generated_at`: ISO-8601 UTC timestamp
4. `note_ja`: guidance text
5. `overrides`: object mapping `category_key` -> `{ "target_account": "<ACCOUNT>", "target_tax_division": "<TAX_DIVISION_OR_EMPTY>" }`

### Example

```json
{
  "schema": "belle.category_overrides.v2",
  "client_id": "ACME",
  "generated_at": "2026-02-14T00:00:00Z",
  "note_ja": "target_account と target_tax_division の文字列値のみ編集してください。キーや構造は変更しないでください。",
  "overrides": {
    "restaurant_izakaya": { "target_account": "交際費", "target_tax_division": "" },
    "utilities": { "target_account": "水道光熱費", "target_tax_division": "" },
    "banks_credit_unions": { "target_account": "支払手数料", "target_tax_division": "" }
  }
}
```

## Editing rules

1. Users may edit only `overrides.<category_key>.target_account` and `overrides.<category_key>.target_tax_division` string values.
2. Adding/removing/renaming category keys is discouraged (missing keys fall back, extra keys are ignored, and invalid rows are warned/ignored).
3. Do not change schema/version fields.
4. `target_account` must be a non-empty string.
5. `target_tax_division` must be a string and may be blank.
6. `receipt` may use non-empty `target_tax_division` as a fallback route after learned receipt tax evidence.
7. `credit_card_statement` may use non-empty `target_tax_division` as a placeholder-side fallback after learned merchant-key tax evidence.

## Runtime validation semantics (best-effort)

1. Missing file:
   1. `receipt` and `credit_card_statement` runtimes auto-generate a full-expanded file from current lexicon keys + line defaults, then load best-effort.
   2. If still unavailable, continue with empty overrides.
2. UTF-8 BOM at file head:
   1. BOM is removed in-place and loading continues.
3. File-level invalid states (JSON decode error, top-level non-object, schema mismatch, `overrides` non-object):
   1. Ignore the entire file (equivalent to empty overrides).
   2. Continue run using defaults/global fallback.
4. Key-set mismatch against lexicon categories:
   1. Missing keys are allowed and fall back to defaults/global fallback.
   2. Extra keys are ignored.
5. Per-key invalid rows:
   1. Missing row or non-object row is ignored for that key.
   2. Missing/empty/non-string `target_account` is ignored for that key.
   3. Missing/non-string `target_tax_division` is ignored for that key.
   4. Extra row keys are warned and ignored.
6. Loader returns only validated overrides that include both `target_account` and `target_tax_division`.

## Runtime merge behavior

1. Load global defaults from the bookkeeping-mode-selected tracked defaults file for the line.
   1. `receipt`: `defaults/receipt/category_defaults_tax_<mode>.json`
   2. `credit_card_statement`: `defaults/credit_card_statement/category_defaults_tax_<mode>.json`
2. Load best-effort overrides from `clients/<CLIENT_ID>/lines/<line_id>/config/category_overrides.json`.
3. Build `effective_defaults` by replacing `target_account` and `target_tax_division` for validated override keys.
4. Keep global `confidence`, `priority`, `reason_code`, and `global_fallback` unchanged.
5. For keys without a valid override, use global defaults/global fallback.
6. This merge contract applies to both `receipt` and `credit_card_statement`.

## Line interpretation

1. `receipt` reads the merged rule's `target_account` as the debit-side fallback account.
2. `credit_card_statement` reads the merged rule's `target_account` as the placeholder-side fallback account.
3. `receipt` reads `target_tax_division` as a debit-side tax-division fallback only when the value is non-empty.
4. `credit_card_statement` reads `target_tax_division` as a placeholder-side tax-division fallback only when the value is non-empty.

## Warnings and manifest

1. Loader warnings are emitted to stdout as `[WARN]` at run end.
2. The same warnings are recorded in:
   1. `run_manifest["category_overrides"]["warnings"]`
   2. `run_manifest["warnings"]` (top-level aggregate)
3. `run_manifest["category_overrides"]` also records `path`, `applied_count`, and `expected_count`.

## Legacy compatibility (receipt only, deprecated)

1. If `clients/<CLIENT_ID>/lines/receipt/` is absent, receipt scripts may use:
   1. `clients/<CLIENT_ID>/config/category_overrides.json`
2. Non-receipt lines must never use legacy fallback.
