# CREDIT_CARD_REPLACER_SPEC (`credit_card_statement`)

## Scope

This spec applies only to `line_id=credit_card_statement`.

Current implementation status:
1. Target-side account replacement is implemented.
2. Target-side tax-division replacement is implemented.
3. Payable-side canonical account rewrite is implemented.
4. Payable-side subaccount fill is implemented.
4. Shared Yayoi tax postprocess runs after credit-card tax-division replacement.

Related specs:
1. `spec/CREDIT_CARD_LINE_INPUTS_SPEC.md`
2. `spec/CREDIT_CARD_CLIENT_CACHE_SPEC.md`
3. `spec/FILE_LAYOUT.md`

## Replacement goals

1. Replace placeholder account `仮払金` on the target side with a predicted target account.
2. Replace target-side tax division before the shared tax postprocess runs.
3. Detect the payable side from target-side placeholder names plus cache `canonical_payable`.
4. Rewrite the payable-side output account to the canonical payable account when cache `canonical_payable.status == OK`.
5. Fill payable-side subaccount after canonical payable rewrite when the payable side is uniquely detected and subaccount is empty.
6. In the same run, the shared tax postprocess may later fill tax amount fields when configured/applicable.
7. Preserve all other non-target fields.

## Target-side rule

1. Credit-card target side is the placeholder side.
2. Placeholder side may be either `debit` or `credit`.
3. If placeholder side is ambiguous or absent, target-side account and tax replacement are both no-op.
4. Inference uses summary only; memo is not an inference source for this line.

## Account decision

Intent remains unchanged:
1. First prefer learned `merchant_key_account_stats`.
2. If that does not resolve, category-default fallback may supply `target_account`.
3. Payable-side detection must not reuse teacher-side candidate logic directly; it uses target-side placeholder names plus runtime canonical payable only.

## Payable-side detection and canonical rewrite

1. Raw target payable placeholder names are not authoritative final output accounts.
2. Runtime payable-side detection uses:
   1. `target_payable_placeholder_names`
   2. cache `canonical_payable.account_name` only when `canonical_payable.status == OK`
3. Detection outcomes are explicit:
   1. unique debit-side match -> payable side is `debit`
   2. unique credit-side match -> payable side is `credit`
   3. both sides match -> `ambiguous`
   4. neither side matches -> `none`
4. When payable side is uniquely detected and `canonical_payable.status == OK`, runtime rewrites that side's account cell in output to `canonical_payable.account_name`.
5. If the raw payable-side account is already equal to `canonical_payable.account_name`, runtime records a no-op rewrite rather than a failure.
6. Review/report must preserve raw payable-side before-values even when final output writes the canonical payable account.

## Payable-side ordering

1. Payable-side detection runs before payable-side canonical rewrite.
2. Payable-side subaccount fill runs after canonical rewrite and targets that same payable side.
3. Ambiguous payable-side detection must not silently choose a side for rewrite or subaccount fill.

## Tax-division decision timing

1. Credit-card tax-division decision runs only after the target account has been chosen/predicted for the row.
2. Credit-card tax-division replacement happens before the shared `belle.tax_postprocess` step.
3. If the chosen tax division is a supported inner-tax division, the shared tax postprocess may then fill tax amount in the same run.

## Tax-division route order

Exact route order:
1. `merchant_key_target_account_exact`
2. `merchant_key_target_account_partial`
3. `category_default`
4. `global_fallback`
5. unresolved / no-op

Route requirements:
1. Learned routes must use tax samples for the predicted target account only.
2. Partial tax matching must reuse the same merchant normalization and partial-candidate policy as account partial matching.
3. If account replacement used a partial candidate, tax partial must reuse that same resolved lookup key and must not select a different candidate.
4. Static fallback routes must treat blank `target_tax_division` as no fallback.
5. Unresolved tax decision preserves the existing target-side tax-division cell.

## Tax write target

1. If placeholder side is `debit`, write `COL_DEBIT_TAX_DIVISION`.
2. If placeholder side is `credit`, write `COL_CREDIT_TAX_DIVISION`.
3. Existing target-side tax division is preserved when no tax route resolves.

## Config contract

Runtime config path:
1. `clients/<CLIENT_ID>/lines/credit_card_statement/config/credit_card_line_config.json`

Required tax section:
1. `tax_division_thresholds`
2. `tax_division_thresholds.merchant_key_target_account_exact`
3. `tax_division_thresholds.merchant_key_target_account_partial`

Each learned tax route entry currently uses:
1. `min_count`
2. `min_p_majority`

Partial-route note:
1. Thresholds above gate the chosen tax label after lookup.
2. Partial candidate eligibility still reuses the shared `partial_match` policy used by account replacement.

## Category defaults / overrides

1. Effective category defaults come from the bookkeeping-mode-selected tracked credit-card defaults file.
   1. `tax_excluded` -> `defaults/credit_card_statement/category_defaults_tax_excluded.json`
   2. `tax_included` -> `defaults/credit_card_statement/category_defaults_tax_included.json`
2. Effective category defaults and overrides may supply `target_tax_division`.
3. A non-empty category `target_tax_division` may resolve the `category_default` tax route.
4. A non-empty `global_fallback.target_tax_division` may resolve the `global_fallback` tax route.
5. Blank fallback tax values mean no tax fallback.

## Review report observability

Credit-card review report adds these columns immediately before the shared tax-amount appendix columns:
1. `payable_side_detected`
2. `payable_account_before_raw`
3. `payable_account_after_canonical`
4. `payable_account_rewritten`
5. `payable_account_rewrite_reason`
6. `canonical_payable_status`
7. `target_tax_side`
8. `target_tax_division_before`
9. `target_tax_division_after`
10. `target_tax_division_changed`
11. `tax_evidence_type`
12. `tax_lookup_key`
13. `tax_confidence`
14. `tax_sample_total`
15. `tax_p_majority`
16. `tax_reasons`

## Manifest observability

Replacer manifest includes additive block:
1. `tax_division_replacement`
2. `canonical_payable`

Required fields:
1. `changed_count`
2. `route_counts`
3. `unresolved_count`
4. `partial_match_applied_count`
5. `category_default_applied_count`
6. `global_fallback_applied_count`
7. `target_side_counts`

`canonical_payable` required fields:
1. `cache_snapshot`
2. `rewrite_count`
3. `noop_count`
4. `required_failed_count`
5. `status_counts`
6. `rewrite_reason_counts`

`changed_count` semantics:
1. Final row diff is recomputed after account replacement, tax-division replacement, payable-subaccount fill, and shared tax postprocess.
2. Tax-only row changes must therefore be visible in final `changed` and `changed_count`.

## Strict-stop contract

1. `payable_sub_fill_required_failed == true` still triggers the existing runner-level strict stop for payable-subaccount inference failure.
2. `canonical_payable_required_failed == true` triggers a distinct runner-level strict stop when payable side is required but cache canonical payable is not safely usable.
3. Tax replacement alone does not introduce a new strict-stop condition.

## Runtime summary

The current live credit-card runtime performs all of the following within one run:
1. target-side account replacement
2. target-side tax-division replacement on the placeholder side
3. payable-side canonical account rewrite when cache canonical payable is `OK`
4. payable-side subaccount fill
5. shared tax postprocess tax-amount fill when configured/applicable

## Explicit exclusions

1. Receipt tax behavior is unchanged in this phase.
2. Bank tax behavior is unchanged in this phase.
3. No backward compatibility or migration support is provided for older credit-card manifests in this phase.
