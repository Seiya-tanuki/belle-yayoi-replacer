# CREDIT_CARD_REPLACER_SPEC (credit_card_statement line)

## Scope and status

This spec applies only to `line_id=credit_card_statement`.
It defines replacer + runner operational contracts for the credit-card line.

Implementation status:
1. `receipt`: implemented/runnable via explicit skills.
2. `bank_statement`: implemented/runnable via explicit skills.
3. `credit_card_statement`: implemented/runnable via explicit skills.

Related specs:
1. `spec/CREDIT_CARD_LINE_INPUTS_SPEC.md`
2. `spec/CREDIT_CARD_CLIENT_CACHE_SPEC.md`
3. `spec/FILE_LAYOUT.md`

## Replacement goals

1. Replace placeholder account `仮払金` with a predicted account.
2. Fill payable-side subaccount when account name is `未払金` and subaccount is empty.

All non-target fields must remain unchanged.

## Placeholder targeting rule

Target side must be detected by account-name matching:
1. locate side (debit or credit) whose account name equals `仮払金`
2. do not rely on fixed column positions
3. if side is ambiguous or absent, no account replacement is applied for that row

## Required replacements and threshold gate

### A) Placeholder account replacement

1. predict account from learned `merchant_key_account_stats`
2. apply only when configured thresholds pass
3. if thresholds fail or top label is non-unique, do not replace that row

### B) Payable-side subaccount fill (`未払金`)

1. detect side where account name equals `未払金`
2. apply only when that side subaccount is empty
3. fill from file-level inferred payable subaccount when file inference status is `OK`
4. if inference is not `OK`, rows that require fill remain unresolved and are marked for strict-stop evaluation

## File-level card inference policy (strict)

For each target file, infer one payable subaccount identity for the whole file:
1. voting unit: `merchant_key` evidence from target rows
2. select a single top payable subaccount only when confidence gates pass
3. confidence gates use configured `min_votes` and `min_p_majority`
4. ties or low-confidence outcomes are treated as non-`OK` file inference

This policy assumes Contract A input (one statement, one card per target file).

## Runner strict-stop contract

Strict stop is enforced at runner layer.

Condition:
1. replacer reports `payable_sub_fill_required_failed=true`
2. this means payable-side subaccount fill was required for at least one row but file-level card inference was not `OK`

Runner behavior:
1. write run outputs and manifests first
2. mark run manifest with `strict_stop_applied=true` and `exit_status=FAIL`
3. terminate with `SystemExit(2)` (exit code `2`)

## Audit artifact retention on failure

Strict-stop failure must remain auditable. Generated artifacts are kept under the run directory:
1. replaced CSV output
2. replacer manifest JSON (`reports.manifest_json`)
3. per-row review report CSV (`reports.review_report_csv`)
4. runner manifest (`run_manifest.json`)

## Candidate extraction knobs (config contract)

File-level card candidate extraction is controlled by config knobs such as:
1. `min_total_count`
2. `min_unique_merchants`
3. `min_unique_counter_accounts`
4. `manual_allow`

These knobs gate whether a payable subaccount becomes an eligible candidate.

Hard-gate contract:
1. eligible set for file-level payable-subaccount inference is only payable subaccounts where `is_candidate == true`
2. if zero payable subaccounts are flagged as candidates, file-level inference must not return `OK`
3. in that zero-candidate case, no payable subaccount may be inferred and there is no fallback to all observed payable subaccounts
4. `manual_allow` remains an explicit override that can force candidacy even when numeric thresholds are not met

Default candidate-extraction policy for operators using defaults:
1. `min_total_count = 5`
2. `min_unique_merchants = 3`
3. `min_unique_counter_accounts = 2`

## Candidate extraction alias policy

1. Canonical key is `candidate_extraction.min_total_count`.
2. Backward-compatible alias `candidate_extraction.min_rows` is accepted by loader normalization.
3. TEMPLATE config uses canonical keys going forward.

## Partial match safe fallback contract

1. Partial match is attempted only when exact `merchant_key` lookup misses.
2. Allowed direction is `cache_key_in_input` only (`cache_key` must be a substring of input key).
3. Resolver is fail-closed:
   1. choose only the unique longest matched key
   2. if longest tie exists, reject partial match
4. Minimum match length is `4` (`min_match_len` baseline).
5. Partial-match candidate keys must satisfy strong stats gates before use:
   1. `sample_total >= 10`
   2. `p_majority >= 0.95`
   3. top label must exist (`top_account` for account replacement, `top_value` for payable-sub inference)
6. Even after partial key resolution, normal row/file thresholds are still enforced.

## Partial match observability (run manifest)

Replacer manifest includes additive partial-match diagnostics:
1. `partial_match.account_partial_rows_used`
2. `partial_match.votes_partial_used`
3. `partial_match.examples` (input key and matched cache key pairs)

## Training pollution exclusion rule

At training time, rows whose counter account is a known transfer/bank account
(for example `普通預金` or `当座預金`) must be excluded from learning stats.

Rationale:
1. avoid contaminating merchant-to-account and payable-subaccount mappings with non-purchase transfers

## Inference source constraint

Inference uses summary only:
1. summary column is 17th column (1-based)
2. memo column must not be used as inference signal
