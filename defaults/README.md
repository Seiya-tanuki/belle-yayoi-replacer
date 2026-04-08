# defaults/

Per-line global fallback rules per category.
The shared contract is now side-neutral:
- `target_account`
- `target_tax_division`

Line interpretation:
- `receipt`: target side is the debit side.
- `credit_card_statement`: target side is the placeholder side.

Phase A resets only the shared fallback schema. Tax learning and runtime tax-division replacement are deferred.
See `spec/CATEGORY_DEFAULTS_SPEC.md`.
