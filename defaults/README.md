# defaults/

Per-line global fallback rules per category.
The shared contract is now side-neutral:
- `target_account`
- `target_tax_division`

Line interpretation:
- `receipt`: target side is the debit side.
- `credit_card_statement`: target side is the placeholder side.

Tracked defaults layout:
- `receipt` uses `category_defaults_tax_excluded.json` and `category_defaults_tax_included.json`.
- `credit_card_statement` uses `category_defaults_tax_excluded.json` and `category_defaults_tax_included.json`.
- `bank_statement` has no tracked category defaults asset.

For `receipt` / `credit_card_statement`, runtime selects the tracked defaults variant from the client shared tax config `bookkeeping_mode`.
This phase changes tracked asset resolution only. Tax learning and tax replacement order remain unchanged.
See `spec/CATEGORY_DEFAULTS_SPEC.md`.
