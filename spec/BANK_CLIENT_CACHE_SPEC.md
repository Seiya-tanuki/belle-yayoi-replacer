# BANK_CLIENT_CACHE_SPEC (belle.bank_client_cache.v0)

## Scope

This spec applies only to `line_id=bank_statement`.
It defines the v0 client cache contract used by implemented bank runtime flows.

Implementation status:
1. `receipt`: implemented/runnable via explicit skills.
2. `bank_statement`: implemented/runnable via explicit skills.
3. `credit_card_statement`: UNIMPLEMENTED (must remain fail-closed).

Related specs:
1. `spec/BANK_LINE_INPUTS_SPEC.md`
2. `spec/BANK_REPLACER_SPEC.md`
3. `spec/FILE_LAYOUT.md`

## Cache location and schema

Canonical path:
1. `clients/<CLIENT_ID>/lines/bank_statement/artifacts/cache/client_cache.json`

Schema string:
1. `belle.bank_client_cache.v0`

## Update model (append-only + idempotent)

1. Cache is append-only; existing learned counts must not be deleted by normal updates.
2. Update unit is an ingested training pair set built from:
   1. training OCR file(s)
   2. training reference file(s)
   3. deterministic row pairing defined in `spec/BANK_LINE_INPUTS_SPEC.md`
3. Idempotency key is a pair-set SHA256 derived from the ingested training OCR/reference SHA256 set.
4. If the same pair-set SHA256 is already applied, update must be skipped without changing counts.

## Learned label and stats keys

Learned label tuple:
1. `corrected_summary`
2. `counter_account`
3. `counter_subaccount`
4. `counter_tax_division`

Independent learned value (separate from label tuple):
1. `bank_account_subaccount` (bank-side subaccount where account name is `bank_account_name`, usually `普通預金`)
2. This must be learned/stored in dedicated value stats, not merged into `BankLabel`.
3. Rationale: avoid label fragmentation and preserve existing counter-label replacement behavior.

Stats are keyed by:
1. Strong key: `kana_key + sign + amount`
2. Weak key: `kana_key + sign`

`sign` and `kana_key` definitions follow `spec/BANK_LINE_INPUTS_SPEC.md`.

## Decision thresholds and fail-closed gates

Threshold parameters:
1. `min_count`
2. `p_majority`

Gate rule for each route:
1. `sample_total >= min_count`
2. `p_majority >= threshold.p_majority`
3. top label must be unique (`top_count` tie is fail-closed)

If any gate fails:
1. No suggestion is emitted from that route.

## Required top-level fields (auditability)

Required fields:
1. `schema`: `belle.bank_client_cache.v0`
2. `version`
3. `client_id`
4. `line_id` (`bank_statement`)
5. `created_at`
6. `updated_at`
7. `append_only`
8. `decision_thresholds`
9. `applied_training_sets`
10. `label_dictionary`
11. `stats`
12. `bank_account_subaccount_stats`

## `applied_training_sets` (required telemetry)

Each entry keyed by pair-set SHA256 must include:
1. source SHA info:
   1. training OCR SHA256 set
   2. training reference SHA256 set
2. pairing counters:
   1. `rows_total_ocr`
   2. `rows_total_reference`
   3. `pairs_used`
   4. `pairs_skipped_collision`
   5. `pairs_skipped_missing`
3. `applied_at`

This is required for deterministic audit and re-run idempotency.

## `label_dictionary` (required telemetry)

`label_dictionary` stores unique labels and usage counts.
Each label entry includes:
1. `label_id` (stable internal key)
2. `corrected_summary`
3. `counter_account`
4. `counter_subaccount`
5. `counter_tax_division`
6. `count_total`

## `stats` structure (high level)

1. `strong_by_kana_sign_amount`:
   1. key format: `<kana_key>|<sign>|<amount>`
   2. value includes:
      1. `sample_total`
      2. `top_label_id`
      3. `top_count`
      4. `p_majority`
      5. `label_counts`
2. `weak_by_kana_sign`:
   1. key format: `<kana_key>|<sign>`
   2. same value schema as strong

All counters are monotonic under append-only updates.

## `bank_account_subaccount_stats` structure (high level)

`bank_account_subaccount_stats` is independent from `stats` (counter label statistics).
It stores value statistics over the teacher-row bank-side subaccount string.

1. `kana_sign_amount`:
   1. key format: `<kana_key>|<sign>|<amount>`
   2. value schema (`ValueStatsEntry`):
      1. `sample_total`
      2. `top_value`
      3. `top_count`
      4. `p_majority`
      5. `value_counts`
2. `kana_sign`:
   1. key format: `<kana_key>|<sign>`
   2. same `ValueStatsEntry` schema

Learning source for this block:
1. training teacher row bank-side subaccount (the side where account name equals `bank_account_name`)
2. extraction is deterministic from teacher row fields
3. if bank-side subaccount is empty, implementation may skip updating this block (fail-closed / no guess)
