# TAX_POSTPROCESS_SPEC

## Scope

This module defines a shared postprocess/finalizer layer intended for all implemented Yayoi lines:
1. `receipt`
2. `bank_statement`
3. `credit_card_statement`

The shared finalizer is wired into the live runtime for all implemented lines above.
It runs after line-specific replacement/inference is complete and before:
1. CSV write
2. review report write
3. replacer manifest write
4. runner manifest write

## Shared Config

Shared client config path:
1. `clients/<CLIENT_ID>/config/yayoi_tax_config.json`

Missing config behavior:
1. Missing file must not fail.
2. Missing file must resolve to the default disabled config.

Current config contract:
1. `schema: "belle.yayoi_tax_config.v1"`
2. `version: "1.0"`
3. `enabled: false|true`
4. `bookkeeping_mode: "tax_excluded" | "tax_included"`
5. `rounding_mode: "floor"`

Runner manifest observability:
1. Each line runner includes a `yayoi_tax_config` block.
2. That block includes config path plus the resolved `enabled`, `bookkeeping_mode`, and `rounding_mode`.

## Runtime Support Scope (v1)

Supported v1 auto-fill is intentionally narrow:
1. bookkeeping mode is `tax_excluded`
2. tax amount cell is blank
3. tax division is parseable as inner-tax equivalent
4. rounding mode is `floor`
5. amount is parseable as an integer

Existing tax amount cells are always preserved.
The runtime must never overwrite an existing tax amount value.

Unsupported calc modes in v1:
1. `outer`
2. `separate`
3. `inclusive`
4. all other non-target / non-parseable modes

Those modes are no-op / non-auto-filled in v1.

## Tax Division Parsing

Tax division parsing must normalize text with `unicodedata.normalize("NFKC", ...)`.

The parser classifies calc mode into:
1. `inner`
2. `outer`
3. `separate`
4. `inclusive`
5. `other`

Before extracting the base tax rate, the parser must repeatedly strip recognized trailing suffix markers from the tail:
1. `適格`
2. `区分100%`
3. `区分80%`
4. `区分50%`
5. `控不`

Rate parsing rule:
1. Extract the base rate from the stripped/base tax-division text.
2. Suffixes such as `区分80%` must not be misread as the base tax rate.

Examples:
1. `課対仕入内10%適格` => mode `inner`, rate `10`
2. `課対仕入内10%区分80%` => mode `inner`, rate `10`
3. `課税売上内軽減8%` => mode `inner`, rate `8`

## Phase 1 Calculation

V1 runtime supports integer calculation for `inner` mode only.

Rounding:
1. `floor` only

Formula:
1. `tax_amount = (amount * rate_percent) // (100 + rate_percent)`

Floating-point arithmetic must not be used.

## Runtime Observability

Review reports append the following tax-amount audit columns in this exact order:
1. `debit_tax_amount_before`
2. `debit_tax_amount_after`
3. `debit_tax_fill_status`
4. `debit_tax_rate`
5. `debit_tax_calc_mode`
6. `credit_tax_amount_before`
7. `credit_tax_amount_after`
8. `credit_tax_fill_status`
9. `credit_tax_rate`
10. `credit_tax_calc_mode`

Replacer manifests include a top-level `tax_postprocess` block that summarizes:
1. resolved enablement and modes
2. tax-postprocess row changes
3. filled counts by side
4. per-side status counts

Changed-count contract:
1. final row `changed` flags are recomputed from original input row vs final output row
2. `changed_count` uses that same recomputed final truth
3. tax-only row changes are therefore visible in review/manifests
