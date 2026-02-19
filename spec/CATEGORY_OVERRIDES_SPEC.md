# CATEGORY_OVERRIDES_SPEC (belle.category_overrides.v1)

## Purpose

`clients/<CLIENT_ID>/lines/<line_id>/config/category_overrides.json` is a per-client, per-line editable overlay for debit account defaults.
The file is full-expanded: it always contains every `category_key` from `lexicon/<line_id>/lexicon.json`.

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
2. Do not add/remove/rename category keys.
3. Do not change schema/version fields.

## Validation rules

1. JSON must parse.
2. `schema` must equal `belle.category_overrides.v1`.
3. `overrides` key set must match lexicon `category_key` set exactly (no missing, no extra).
4. Every `overrides[k].debit_account` must be a non-empty string.

## Runtime merge behavior

1. Load global defaults from `defaults/<line_id>/category_defaults.json`.
2. Load validated overrides from `clients/<CLIENT_ID>/lines/<line_id>/config/category_overrides.json`.
3. Build `effective_defaults` by replacing only `debit_account` for each category.
4. Keep global `confidence`, `priority`, `reason_code`, and `global_fallback` unchanged.

## Missing-file behavior

If `category_overrides.json` is missing, the system auto-generates a full-expanded file using global defaults and continues.

## Invalid-file behavior

If `category_overrides.json` exists but is invalid, replacer must fail-closed (non-zero exit) and must not create `outputs/runs/<RUN_ID>/`.

## Legacy compatibility (receipt only, deprecated)

1. If `clients/<CLIENT_ID>/lines/receipt/` is absent, receipt scripts may use:
   1. `clients/<CLIENT_ID>/config/category_overrides.json`
2. Non-receipt lines must never use legacy fallback.
