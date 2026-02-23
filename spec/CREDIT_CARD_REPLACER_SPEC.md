# CREDIT_CARD_REPLACER_SPEC (credit_card_statement line v0)

## Scope and status

This spec applies only to `line_id=credit_card_statement`.
It defines the Phase-0 behavior contract for a future replacer implementation.
No runtime implementation is added in this phase.

Implementation status:
1. `receipt`: implemented/runnable via explicit skills.
2. `bank_statement`: implemented/runnable via explicit skills.
3. `credit_card_statement`: UNIMPLEMENTED (must remain fail-closed in current runtime).

Related specs:
1. `spec/CREDIT_CARD_LINE_INPUTS_SPEC.md`
2. `spec/CREDIT_CARD_CLIENT_CACHE_SPEC.md`
3. `spec/FILE_LAYOUT.md`

## Replacement goals (future implementation target)

1. Replace placeholder account `仮払金` with predicted account name.
2. Fill payable-side subaccount when account name is `未払金` and subaccount is empty.

All other non-target fields must remain unchanged.

## Placeholder targeting rule

Target side must be detected by account-name matching:
1. locate side (debit or credit) whose account name equals `仮払金`
2. do not rely on fixed column positions
3. if side is ambiguous or absent, fail-closed for that row

## Required replacements and threshold gate

### A) Placeholder account replacement

1. predict account from learned `merchant_key_account_stats`
2. apply only when configured thresholds pass
3. if thresholds fail or top label is non-unique, do not replace (fail-closed)

### B) Payable-side subaccount fill (`未払金`)

1. find side where account name equals `未払金`
2. apply only when that side subaccount is empty
3. fill with learned card subaccount inferred at file level
4. if confidence is insufficient, treat file as invalid for required fill policy

## File-level card inference policy

The run must infer one payable subaccount identity for the whole target file:
1. voting unit: `merchant_key` evidence from target rows
2. select single top payable subaccount only when confidence gates pass
3. confidence gates use configured `min_votes` and `min_p_majority`
4. ties or low-confidence outcomes are invalid for required fill policy

Strict Phase-0 contract intent:
1. insufficient confidence is a strict invalid condition (fail-closed intent)
2. later phases may refine warn/stop runtime handling, but this spec is strict by default

## Candidate extraction knobs (config contract)

File-level card candidate extraction is controlled by config knobs such as:
1. `min_rows`
2. `min_unique_merchants`
3. `min_merchant_coverage`

These knobs gate whether a file has enough evidence for card-level inference.

## Training pollution exclusion rule

At training time, rows whose counter account is a known transfer/bank account
(for example `普通預金` or `当座預金`) must be excluded from learning stats.

Rationale:
1. avoid contaminating merchant-to-account and payable-subaccount mappings with non-purchase transfers

## Inference source constraint

Inference uses summary only:
1. summary column is 17th column (1-based)
2. memo column must not be used as inference signal

