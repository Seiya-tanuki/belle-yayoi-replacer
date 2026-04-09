# CREDIT_CARD_CLIENT_CACHE_SPEC (`credit_card_statement`)

## Scope

This spec applies only to `line_id=credit_card_statement`.

Current implementation status:
1. `credit_card_statement` cache learning is implemented.
2. Cache update source is line-scoped `inputs/ledger_ref/`.
3. No backward compatibility or migration support is provided for older credit-card cache schema versions in this phase.

Related specs:
1. `spec/CREDIT_CARD_LINE_INPUTS_SPEC.md`
2. `spec/CREDIT_CARD_REPLACER_SPEC.md`
3. `spec/FILE_LAYOUT.md`

## Cache location and schema

Canonical path:
1. `clients/<CLIENT_ID>/lines/credit_card_statement/artifacts/cache/client_cache.json`

Schema:
1. `schema = "belle.cc_client_cache.v1"`

Version:
1. `version = "0.2"`

## Update model

1. Cache update is append-only.
2. Each ingested `ledger_ref` file is deduped by SHA256.
3. Already-applied SHA256 entries must not increment learned counts again.
4. Normal updates must not destructively rebuild or delete historical evidence.

## Required top-level fields

1. `schema`
2. `version`
3. `client_id`
4. `line_id`
5. `created_at`
6. `updated_at`
7. `append_only`
8. `decision_thresholds`
9. `applied_ledger_ref_sha256`
10. `card_subaccount_candidates`
11. `merchant_key_account_stats`
12. `merchant_key_payable_sub_stats`
13. `merchant_key_target_account_tax_stats`
14. `payable_sub_global_stats`

## Learned evidence blocks

### `merchant_key_account_stats`

Purpose:
1. Predict placeholder-side target account.

Shape:
1. key: `merchant_key`
2. value: `StatsEntry`

Expected `StatsEntry` fields:
1. `sample_total`
2. `top_account`
3. `top_count`
4. `p_majority`
5. `debit_account_counts`

### `merchant_key_payable_sub_stats`

Purpose:
1. Support file-level payable-side subaccount inference.

Shape:
1. key: `merchant_key`
2. value: `ValueStatsEntry`

Expected `ValueStatsEntry` fields:
1. `sample_total`
2. `top_value`
3. `top_count`
4. `p_majority`
5. `value_counts`

### `merchant_key_target_account_tax_stats`

Purpose:
1. Learn target-side tax division conditioned on the chosen target account.
2. Provide at minimum `merchant_key + target_account -> tax_division distribution`.

Shape:
1. outer key: `merchant_key`
2. inner key: `target_account`
3. value: `ValueStatsEntry`

Interpretation:
1. `top_value` = learned target-side tax division
2. `value_counts` = tax-division vote distribution for that exact `merchant_key + target_account`

Learning constraints:
1. Learning uses the same `merchant_key` normalization as account learning.
2. Training reads the opposite/payable-counter side account and tax division from `ledger_ref`.
3. Tax learning must skip rows when any of the following are true:
   1. summary is blank
   2. `merchant_key` cannot be derived
   3. target account is blank
   4. target tax division is blank

### `card_subaccount_candidates`

Purpose:
1. Candidate list/map for payable subaccount identities inferred from historical data.
2. Gate file-level credit-card payable-subaccount inference.

Expected entry fields:
1. `total_count`
2. `unique_merchants`
3. `unique_counter_accounts`
4. `is_candidate`
5. `counter_accounts_seen`
6. optional `notes`

### `payable_sub_global_stats`

Purpose:
1. Global fallback distribution over payable subaccounts across ingested data.

## Decision-threshold snapshot

`decision_thresholds` records the normalized config used when the cache was built.

Required credit-card tax section:
1. `tax_division_thresholds`
2. `tax_division_thresholds.merchant_key_target_account_exact`
3. `tax_division_thresholds.merchant_key_target_account_partial`

## Explicit exclusions

1. Receipt tax learning is out of scope here.
2. Bank tax learning is out of scope here.
3. This phase does not provide compatibility shims for legacy credit-card cache schema versions.
