# rulesets/

This folder contains tracked versioned rule/config snapshots still needed by deterministic scripts or audit comparison.

- `rulesets/credit_card_statement/teacher_extraction_rules_v1.json` is the active tracked credit-card teacher-extraction ruleset.
- `rulesets/replacer_config_v1_14.json` is kept as a legacy audit snapshot.

Notes:
- Receipt no longer uses a repo-scoped ruleset. The active receipt config contract is `clients/<CLIENT_ID>/lines/receipt/config/receipt_line_config.json`.
- The tracked provisioning baseline for receipt is `clients/TEMPLATE/lines/receipt/config/receipt_line_config.json`.
- `credit_card_statement` teacher extraction remains repo-tracked under `rulesets/credit_card_statement/teacher_extraction_rules_v1.json`.
