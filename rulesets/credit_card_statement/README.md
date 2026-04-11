# credit_card_statement rulesets

This line is implemented.
Store line-specific replacement ruleset assets for credit-card processing here.
Tracked teacher extraction ruleset:
1. `teacher_extraction_rules_v1.json`

`teacher_extraction_rules_v1.json` is the tracked lexical ruleset for derived teacher extraction only.
This phase keeps the canonical-payable v2 runtime/cache contract unchanged and applies lexical tuning only.

Field meanings:
1. `hard_include_terms`: strong positive lexical evidence; a hit selects unless manual or hard-negative precedence rejects first.
2. `soft_include_terms`: positive lexical evidence that still requires the configured soft-match thresholds.
3. `exclude_terms`: hard negatives; a hit rejects immediately unless the subaccount is manually included.
4. `soft_negative_terms`: discouraging lexical evidence; a hit rejects when there is no sufficient positive evidence, but does not automatically defeat strong positive card evidence.

Current precedence:
1. `manual_exclude_subaccounts`
2. `manual_include_subaccounts`
3. `exclude_terms`
4. positive lexical selection (`hard_include_terms` / threshold-qualified `soft_include_terms`)
5. `soft_negative_terms` blocks only when positive evidence is absent or insufficient

This ruleset does not implement the richer proposal model from Plan C.
