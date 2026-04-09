# rulesets/

This folder contains versioned configuration snapshots used by deterministic scripts.

- `receipt/replacer_config_v1_15.json` is the active default configuration in Phase 1.
- Legacy GPTs ruleset is kept under `legacy/` for audit/comparison.

Notes:
- v1_15 adds gated routes for:
  1) T-number × category
  2) T-number
- The same tracked receipt ruleset now also contains dedicated `tax_division_thresholds` and `tax_division_confidence` sections for receipt debit-side tax routing.
