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
This document describes tracked defaults asset structure and bookkeeping-mode-based resolution only.
Live runtime tax-routing semantics are defined by the selected line's replacer spec; for `receipt`, `target_tax_division` fallback is relevant only after the original-tax gate in `spec/REPLACER_SPEC.md` passes.
See `spec/CATEGORY_DEFAULTS_SPEC.md` and `spec/REPLACER_SPEC.md`.
