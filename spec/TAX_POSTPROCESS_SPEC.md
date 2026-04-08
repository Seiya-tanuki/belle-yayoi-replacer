# TAX_POSTPROCESS_SPEC

## Scope

This module defines a shared postprocess/finalizer layer intended for all implemented Yayoi lines:
1. `receipt`
2. `bank_statement`
3. `credit_card_statement`

Phase 1 adds only the shared foundation logic and shared client config contract.
Phase 1 does not wire this logic into any live replacer, line runner, review report, manifest, or CSV output path yet.

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

## Phase 1 Supported Auto-Fill Scope

Supported v1 auto-fill is intentionally narrow:
1. bookkeeping mode is `tax_excluded`
2. tax amount cell is blank
3. tax division is parseable as inner-tax equivalent
4. rounding mode is `floor`
5. amount is parseable as an integer

Existing tax amount cells are always preserved.
Phase 1 must never overwrite an existing tax amount value.

Unsupported calc modes in v1:
1. `outer`
2. `separate`
3. `inclusive`
4. all other non-target / non-parseable modes

Those modes are not auto-filled in v1.

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

Phase 1 supports integer calculation for `inner` mode only.

Rounding:
1. `floor` only

Formula:
1. `tax_amount = (amount * rate_percent) // (100 + rate_percent)`

Floating-point arithmetic must not be used.

## Future Integration

Runtime integration is deferred to a later phase.
That later phase is expected to wire the shared postprocess before CSV write and before review/report/manifest emission.
