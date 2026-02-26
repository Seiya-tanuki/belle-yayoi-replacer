# CATEGORY_OVERRIDES_SPEC (belle.category_overrides.v1)

## Purpose

`clients/<CLIENT_ID>/lines/<line_id>/config/category_overrides.json` is a per-client, per-line editable overlay for debit account defaults.
The generated file is full-expanded (all `category_key`s), but runtime loading is best-effort and may apply only valid rows.
Phase D line scope:
1. `receipt`: used.
2. `credit_card_statement`: used.
3. `bank_statement`: not used.

## Schema

Top-level keys:
1. `schema`: string, must be `belle.category_overrides.v1`
2. `client_id`: string
3. `generated_at`: ISO-8601 UTC timestamp
4. `note_ja`: guidance text
5. `overrides`: object mapping `category_key` -> `{ "debit_account": "<ACCOUNT_NAME_STRING>" }`

### Example

```json
{
  "schema": "belle.category_overrides.v1",
  "client_id": "ACME",
  "generated_at": "2026-02-14T00:00:00Z",
  "note_ja": "Edit ONLY debit_account string values. Do not change keys/structure.",
  "overrides": {
    "office_supplies": { "debit_account": "消耗品費" }
  }
}
```

## Editing rules

1. Users may edit only `overrides.<category_key>.debit_account` string values.
2. Adding/removing/renaming category keys is discouraged (runtime will ignore invalid parts with warnings).
3. Do not change schema/version fields.

## Runtime validation semantics (best-effort)

1. Missing file:
   1. `receipt` and `credit_card_statement` runtimes auto-generate a full-expanded file, then load best-effort.
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
   2. Missing/empty/non-string `debit_account` is ignored for that key.
6. Loader returns only validated, non-empty debit account overrides.

## Runtime merge behavior

1. Load global defaults from `defaults/<line_id>/category_defaults.json`.
2. Load best-effort overrides from `clients/<CLIENT_ID>/lines/<line_id>/config/category_overrides.json`.
3. Build `effective_defaults` by replacing only `debit_account` for validated override keys.
4. Keep global `confidence`, `priority`, `reason_code`, and `global_fallback` unchanged.
5. For keys without a valid override, use global defaults/global fallback.
6. This merge contract applies to both `receipt` and `credit_card_statement`.

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
