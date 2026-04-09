# credit_card_statement defaults

Shared category fallback rules for `credit_card_statement`.

- Target side is the placeholder side.
- Tracked defaults files are:
  `category_defaults_tax_excluded.json` and `category_defaults_tax_included.json`.
- Both files use `target_account` and `target_tax_division`.
- Runtime/bootstrap selects the file from `clients/<CLIENT_ID>/config/yayoi_tax_config.json` `bookkeeping_mode`.
- Credit-card runtime now writes placeholder-side `target_tax_division` before the shared Yayoi tax postprocess runs.
- `target_tax_division` fallback is conservative by policy:
  learned `merchant_key + target_account` evidence is preferred, then non-empty per-category overrides/defaults, then non-empty global fallback.
- Blank `target_tax_division` values in tracked defaults or overrides mean "no tax fallback"; they do not blank existing tax-division cells.
- This phase changes the tracked asset split only; credit-card learning and tax replacement logic are unchanged.
