# receipt defaults

Shared category fallback rules for `receipt`.

- Target side is the Yayoi debit side.
- Tracked defaults files are:
  `category_defaults_tax_excluded.json` and `category_defaults_tax_included.json`.
- Both files use `target_account` and `target_tax_division`.
- Runtime/bootstrap selects the file from `clients/<CLIENT_ID>/config/yayoi_tax_config.json` `bookkeeping_mode`.
- Receipt tax learning can override tax division from learned evidence first.
- Non-empty `target_tax_division` values act only as conservative fallback rules after learned receipt tax routes.
- This phase changes the tracked asset split only; receipt learning and tax replacement logic are unchanged.
