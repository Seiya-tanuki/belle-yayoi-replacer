# BANK_REPLACER_SPEC (bank_statement line v0)

## Scope and status

This spec applies only to `line_id=bank_statement`.
It defines v0 replacer behavior for future implementation.

Implementation status:
1. `bank_statement`: UNIMPLEMENTED (must remain fail-closed)

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

## Replacement apply contract

When a route returns a label:
1. replace summary (17th column) with `corrected_summary`
2. replace counter-side account/subaccount/tax-division with label values
3. keep all non-target fields unchanged

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

Manifest must include run metadata and input artifact references.

## Fail-closed invariants

1. If required features are missing/ambiguous, do not replace.
2. If thresholds fail, do not replace.
3. If tie in top label counts, do not replace.
4. This phase does not implement bank runtime code.
