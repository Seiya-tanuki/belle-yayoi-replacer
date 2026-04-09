# credit_card_statement defaults

Shared category fallback rules for `credit_card_statement`.

- Target side is the placeholder side.
- `category_defaults.json` uses `target_account` and `target_tax_division`.
- Credit-card runtime now writes placeholder-side `target_tax_division` before the shared Yayoi tax postprocess runs.
- `target_tax_division` fallback is conservative by policy:
  learned `merchant_key + target_account` evidence is preferred, then non-empty per-category overrides/defaults, then non-empty global fallback.
- Blank `target_tax_division` values in tracked defaults or overrides mean "no tax fallback"; they do not blank existing tax-division cells.
