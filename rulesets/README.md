# rulesets/

This folder contains versioned configuration snapshots used by deterministic scripts.

- `rulesets/receipt/replacer_config_v1_15.json` is the active tracked receipt ruleset.
- Legacy GPTs ruleset is kept under `legacy/` for audit/comparison.

Notes:
- `rulesets/receipt/replacer_config_v1_15.json` contains the receipt tax-threshold config used by runtime.
- `tax_division_thresholds` gates learned receipt debit-side tax-division routing.
- `tax_division_confidence` controls reported confidence for those receipt debit-side tax routes.
- Operators tune receipt tax routing at that file path; `bank_statement` and `credit_card_statement` use their own line configs instead.
