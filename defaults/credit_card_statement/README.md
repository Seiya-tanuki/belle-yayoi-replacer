# credit_card_statement defaults

Shared category fallback rules for `credit_card_statement`.

- Target side is the placeholder side.
- `category_defaults.json` uses `target_account` and `target_tax_division`.
- Phase A stores `target_tax_division` in the shared contract only; credit-card tax learning and placeholder-side tax-division write logic are deferred.
