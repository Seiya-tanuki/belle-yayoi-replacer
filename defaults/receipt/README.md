# receipt defaults

Shared category fallback rules for `receipt`.

- Target side is the Yayoi debit side.
- `category_defaults.json` uses `target_account` and `target_tax_division`.
- Receipt tax learning can override tax division from learned evidence first.
- Non-empty `target_tax_division` values act only as conservative fallback rules after learned receipt tax routes.
