# BANK_REPLACER_SPEC (bank_statement line v0)

## Scope and status

This spec applies only to `line_id=bank_statement`.
It defines v0 replacer behavior for implemented bank runtime flows.

Implementation status:
1. `receipt`: implemented/runnable via explicit skills.
2. `bank_statement`: implemented/runnable via explicit skills.
3. `credit_card_statement`: implemented/runnable via explicit skills (see credit-card specs).

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
5. bank-account-side subaccount (ordinary-deposit side subaccount), when file-level inference is `OK`

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

Learning sources for vote candidates:
1. `cache.bank_account_subaccount_stats["kana_sign_amount"]` (strong)
2. `cache.bank_account_subaccount_stats["kana_sign"]` (weak fallback)

File-level decision gates (`clients/<CLIENT_ID>/lines/bank_statement/config/bank_line_config.json`):
1. `bank_side_subaccount.enabled` (default `true`)
2. `thresholds.file_level_bank_sub_inference.min_votes` (default `3`)
3. `thresholds.file_level_bank_sub_inference.min_p_majority` (default `0.9`)

File-level apply policy:
1. infer one bank-side subaccount identity per target CSV from row vote evidence
2. if inference status is `OK`, apply the SAME inferred subaccount to ALL rows that require bank-side subaccount fill
3. partial fill is forbidden; hybrid per-row bank-side subaccount outcomes are not allowed
4. if inference status is not `OK` and required-fill rows exist:
   1. do not fill any required bank-side subaccount row
   2. set `bank_sub_fill_required_failed = true` in replacer manifest
5. if no rows require bank-side subaccount fill, keep rows unchanged and do not raise this failure flag

Apply target and non-target guarantees:
1. only the bank-account-side subaccount column is writable
2. bank-account-side account name is NOT changed
3. bank-account-side tax division is NOT changed

## Replacement apply contract

When a route returns a label:
1. replace summary (17th column) with `corrected_summary`
2. replace counter-side account/subaccount/tax-division with label values
3. keep all non-target fields unchanged

Bank-account-side subaccount replacement is evaluated independently using the file-level rule above.

When no route returns a valid label:
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
2. `file_bank_sub_inference` object (`status`, `value`, `votes_total`, `top_count`, `p_majority`, `reasons`)
3. `bank_sub_fill_required_failed`

## Runner strict-stop contract

1. `bank_statement` runner reads `bank_sub_fill_required_failed` from replacer manifest.
2. When `bank_sub_fill_required_failed == true`, artifacts are kept (run directory and manifests are preserved) and the runner terminates with `SystemExit(2)` (exit code `2`).
3. This strict-stop means required bank-side subaccount fill existed but file-level bank inference was not `OK`.

## Fail-closed invariants

1. If required features are missing/ambiguous, do not replace.
2. If thresholds fail, do not replace.
3. If tie in top label counts, do not replace.
4. If required bank-side subaccount fill exists and file-level inference is not `OK`, set `bank_sub_fill_required_failed = true` (runner strict-stop is handled separately).
5. Cross-line behavior is out of scope here; `credit_card_statement` rules are defined in credit-card specs.
