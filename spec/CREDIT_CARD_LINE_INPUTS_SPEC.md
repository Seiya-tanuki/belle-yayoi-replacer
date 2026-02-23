# CREDIT_CARD_LINE_INPUTS_SPEC (credit_card_statement line v0)

## Scope and status

This spec defines v0 input contracts for `line_id=credit_card_statement` only.
This line remains unimplemented in Phase 0 and must stay fail-closed at runtime.

Implementation status:
1. `receipt`: implemented/runnable via explicit skills.
2. `bank_statement`: implemented/runnable via explicit skills.
3. `credit_card_statement`: UNIMPLEMENTED (must remain fail-closed in current runtime).

Related specs:
1. `spec/CREDIT_CARD_CLIENT_CACHE_SPEC.md`
2. `spec/CREDIT_CARD_REPLACER_SPEC.md`
3. `spec/FILE_LAYOUT.md`

## Inputs contract (per client line path)

Canonical base path:
1. `clients/<CLIENT_ID>/lines/credit_card_statement/`

Allowed input directories:
1. target draft:
   1. `inputs/kari_shiwake/`
2. training reference:
   1. `inputs/ledger_ref/`

## `kari_shiwake` cardinality (single target file)

`inputs/kari_shiwake/` accepts CSV targets with strict file-count behavior:
1. `0` files:
   1. no-op SKIP (normal)
2. `1` file:
   1. accepted as one run target
3. `2+` files:
   1. fail-closed error (`SystemExit`)

Non-target files/extensions are ignored for counting.

## `ledger_ref` cardinality and learning source

`inputs/ledger_ref/` accepts `0+` files.

Rules:
1. ingestion is append-only
2. dedupe is by per-file SHA256
3. already-applied SHA256 entries must not update learned stats again
4. multiple historical files are allowed and may be accumulated over time

## Operational contract A (single statement per run)

The accepted target `kari_shiwake` file must represent exactly one credit-card statement.

Strict policy for Phase 0 spec:
1. mixed-card target CSV (multiple card identities in one target file) is out of scope
2. such mixed input is invalid for required payable-subaccount fill policy
3. later phases may extend behavior, but v0 contract defines this as invalid

## Inference field constraint

Inference must use only summary text:
1. summary column is the 17th column (1-based index)

Non-summary fields are not inference signals for this line contract.
Memo (22nd column) is not an inference source.

