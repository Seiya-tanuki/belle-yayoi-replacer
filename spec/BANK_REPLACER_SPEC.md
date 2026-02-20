# BANK_REPLACER_SPEC (bank_statement line v0)

## Scope and status

This spec applies only to `line_id=bank_statement`.
It defines v0 replacer behavior for implemented bank runtime flows.

Implementation status:
1. `receipt`: implemented/runnable via explicit skills.
2. `bank_statement`: implemented/runnable via explicit skills.
3. `credit_card_statement`: UNIMPLEMENTED (must remain fail-closed).

Related specs:
1. `spec/BANK_LINE_INPUTS_SPEC.md`
2. `spec/BANK_CLIENT_CACHE_SPEC.md`
3. `spec/FILE_LAYOUT.md`

## Goal

Transform target OCR draft rows into teacher-like fields, with replacements limited to:
1. summary (17th column)
2. counter-side account
3. counter-side subaccount
4. counter-side tax division
5. bank-account-side subaccount (ordinary-deposit side subaccount), when deterministically inferred

Counter-side means the side currently containing placeholder account `仮払金`.
Side selection must be done by account-name detection, not fixed column positions.

All other columns must remain unchanged.

## v0 detection assumptions (explicit)

These are v0 assumptions and configurable later:
1. Placeholder account name: `仮払金`
2. Bank account name: `普通預金`

Fail-closed conditions:
1. Placeholder side cannot be uniquely identified -> no replacement
2. Bank account side cannot be determined and memo SIGN is unavailable -> no replacement

## Feature extraction from target OCR

For each row:
1. `kana_key` from summary normalization (`spec/BANK_LINE_INPUTS_SPEC.md`)
2. `sign` with precedence:
   1. derive from bank-account side when `普通預金` side is unique
   2. fallback to OCR memo `SIGN` when derivation is impossible
3. `amount` from the amount field of the placeholder side (`仮払金` side)

`sign` mapping for side-derivation (bank-statement meaning):
1. bank account on debit side -> `credit` (deposit)
2. bank account on credit side -> `debit` (withdrawal)

Amount consistency check:
1. The keyed amount must match the placeholder-side amount.
2. Inconsistent or non-deterministic amount -> no replacement.

## Decision order (deterministic)

1. Strong route: `kana_key + sign + amount`
   1. use only when threshold gate passes
2. Weak route: `kana_key + sign`
   1. use only when threshold gate passes
3. Else:
   1. no replacement

Threshold gates and `p_majority` semantics follow `spec/BANK_CLIENT_CACHE_SPEC.md`.

## Bank-account-side subaccount replacement (普通預金側補助科目)

This replacement is independent from counter-label replacement.
It may apply even when counter-label replacement applies; it must not reduce counter replacement coverage.

Learning source:
1. `cache.bank_account_subaccount_stats["kana_sign_amount"]` (strong)
2. `cache.bank_account_subaccount_stats["kana_sign"]` (weak fallback)

Runtime config (`bank_line_config.json`):
1. `bank_side_subaccount.enabled` (default `true`)
2. `bank_side_subaccount.weak_enabled` (default `true`)
3. `bank_side_subaccount.weak_min_count` (default `3`, enforced minimum `3`)

Apply order and policy:
1. evaluate strong key first: `kana_key + sign + amount` (`kana_sign_amount`)
2. if strong is not applied and weak fallback is enabled, evaluate weak key: `kana_key + sign` (`kana_sign`)
3. deterministic-only (both strong and weak):
   1. stats entry must exist
   2. `top_value` must be non-empty
   3. `top_count == sample_total` (equivalent to `p_majority == 1.0`)
4. weak-only additional safety gate: `sample_total >= weak_min_count`
5. ambiguous/non-deterministic keys fail-closed (no bank-side subaccount overwrite)

Apply target and non-target guarantees:
1. only the bank-account-side subaccount column is writable
2. bank-account-side account name is NOT changed
3. bank-account-side tax division is NOT changed

## Replacement apply contract

When a route returns a label:
1. replace summary (17th column) with `corrected_summary`
2. replace counter-side account/subaccount/tax-division with label values
3. keep all non-target fields unchanged

Bank-account-side subaccount replacement is evaluated independently using the rule above.

When no route returns a valid label and bank-side subaccount is not deterministically inferred:
1. keep the row unchanged
2. emit review evidence explaining why no replacement occurred

## Required outputs

Per run output set:
1. replaced CSV
2. review report (CSV)
3. manifest (JSON)

Review report must include at least:
1. route used (`strong` / `weak` / `none`)
2. lookup key
3. `sample_total`
4. `p_majority`
5. `top_count`
6. selected label (if any)
7. fail reason (if no replacement)
8. bank-side subaccount before/after and evidence fields:
   1. `bank_sub_evidence`: `bank_sub_kana_sign_amount` / `bank_sub_kana_sign` / `none`
   2. `bank_sub_sample_total`, `bank_sub_p_majority`, `bank_sub_top_count`

Manifest must include run metadata and input artifact references, including:
1. `bank_side_subaccount_changed_count`
2. optional bank-side subaccount evidence counts, e.g. `{"strong": N, "weak": M}`

## Fail-closed invariants

1. If required features are missing/ambiguous, do not replace.
2. If thresholds fail, do not replace.
3. If tie in top label counts, do not replace.
4. Unsupported lines (e.g. `credit_card_statement`) remain unimplemented and fail-closed.
