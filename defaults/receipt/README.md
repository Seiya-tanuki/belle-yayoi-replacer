# receipt defaults

Shared category fallback rules for `receipt`.

- Target side is the Yayoi debit side.
- `category_defaults.json` uses `target_account` and `target_tax_division`.
- Phase A stores `target_tax_division` in the shared contract only; receipt tax learning and tax-division write logic are deferred.
