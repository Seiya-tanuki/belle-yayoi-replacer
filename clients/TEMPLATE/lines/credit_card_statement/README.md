# credit_card_statement template (Phase 0 scaffold)

This line scaffold is for future `credit_card_statement` implementation.
Current runtime is intentionally fail-closed (not implemented).

## Inputs

Base path:
1. `clients/<CLIENT_ID>/lines/credit_card_statement/`

Put files here:
1. target draft CSV:
   1. `inputs/kari_shiwake/`
2. historical finalized CSVs for learning:
   1. `inputs/ledger_ref/`

## Contract A: one statement per run

`inputs/kari_shiwake/` accepts at most one target file per run:
1. `0` file: SKIP (no-op)
2. `1` file: valid
3. `2+` files: invalid (fail-closed)

The target file must represent exactly one credit-card statement.
Mixed-card target CSV is out of scope for this contract.

## Reusing `ledger_ref`

`inputs/ledger_ref/` can be reused across runs.
Learning ingestion is SHA-deduped, so the same historical file is not double-counted.

