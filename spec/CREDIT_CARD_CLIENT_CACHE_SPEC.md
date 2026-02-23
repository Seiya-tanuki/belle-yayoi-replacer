# CREDIT_CARD_CLIENT_CACHE_SPEC (belle.credit_card_client_cache.v0)

## Scope

This spec applies only to `line_id=credit_card_statement`.
It defines the Phase-0 cache contract for future credit-card learning/replacement flows.
Runtime implementation is not included in this phase.

Implementation status:
1. `receipt`: implemented/runnable via explicit skills.
2. `bank_statement`: implemented/runnable via explicit skills.
3. `credit_card_statement`: UNIMPLEMENTED (must remain fail-closed in current runtime).

Related specs:
1. `spec/CREDIT_CARD_LINE_INPUTS_SPEC.md`
2. `spec/CREDIT_CARD_REPLACER_SPEC.md`
3. `spec/FILE_LAYOUT.md`

## Cache location and schema

Canonical path:
1. `clients/<CLIENT_ID>/lines/credit_card_statement/artifacts/cache/client_cache.json`

Schema string:
1. `belle.credit_card_client_cache.v0`

Version:
1. `version = "0.1"`

## Update model (append-only + SHA dedupe)

1. Cache update is append-only.
2. Update source is `inputs/ledger_ref/` ingestion.
3. Idempotency is SHA256-based:
   1. each ingested `ledger_ref` file SHA256 is recorded
   2. if SHA256 is already applied, stat updates are skipped for that file
4. Normal updates must not destructively rebuild or delete learned counts.

## Required top-level fields

1. `schema` (`belle.credit_card_client_cache.v0`)
2. `version`
3. `client_id`
4. `line_id` (`credit_card_statement`)
5. `created_at`
6. `updated_at`
7. `append_only`
8. `applied_ledger_ref_sha256` (array of applied file SHA256)
9. `card_subaccount_candidates`
10. `merchant_key_account_stats`
11. `merchant_key_payable_sub_stats`
12. `payable_sub_global_stats`

## Required stats blocks (high level)

### `card_subaccount_candidates`

Candidate list/map for payable subaccount identities inferred from historical data.
Each candidate entry keeps evidence counters, for example:
1. `sample_total`
2. `merchant_unique_count`
3. `merchant_vote_count`
4. `p_majority`

This block is used for file-level card identity inference.

### `merchant_key_account_stats`

Map:
1. key: `merchant_key`
2. value: account distribution and vote counts (for replacing `仮払金`)

Expected value shape:
1. `sample_total`
2. `top_account`
3. `top_count`
4. `p_majority`
5. `account_counts`

### `merchant_key_payable_sub_stats`

Map:
1. key: `merchant_key`
2. value: payable subaccount distribution (for account `未払金` subaccount fill)

Expected value shape:
1. `sample_total`
2. `top_payable_subaccount`
3. `top_count`
4. `p_majority`
5. `payable_subaccount_counts`

### `payable_sub_global_stats`

Global fallback distribution over payable subaccounts across ingested data.
This block supports strict-vote diagnostics and future fallback policy design.

## Phase-0 explicit exclusions

1. Tax handling is out of scope in this phase.
2. No tax-specific learning fields are required.
3. Runtime behavior is unchanged (line remains fail-closed until later phases).

