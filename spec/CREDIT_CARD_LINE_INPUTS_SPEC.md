# CREDIT_CARD_LINE_INPUTS_SPEC (credit_card_statement line)

## Scope and status

This spec defines input contracts for `line_id=credit_card_statement`.

Implementation status:
1. `receipt`: implemented/runnable via explicit skills.
2. `bank_statement`: implemented/runnable via explicit skills.
3. `credit_card_statement`: implemented/runnable via explicit skills.

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
2. raw learning teacher:
   1. `inputs/ledger_ref/`
3. managed derived teacher artifacts:
   1. `artifacts/derived/cc_teacher/`
4. required line config:
   1. `config/credit_card_line_config.json` must exist; if missing, PLAN must fail-closed (`FAIL`).
5. tracked extraction ruleset:
   1. `rulesets/credit_card_statement/teacher_extraction_rules_v1.json`

## Contract A (required): single statement per target file

The accepted `kari_shiwake` target file must represent exactly one card statement (one card).

Current scope:
1. mixed multi-card target CSV in one file is unsupported
2. when this contract is violated, required payable-subaccount fill may become invalid and strict stop can occur

## `kari_shiwake` cardinality policy (0/1/2+)

`inputs/kari_shiwake/` is validated by strict file-count behavior:
1. `0` files:
   1. SKIP (normal no-op)
2. `1` file:
   1. accepted as one run target
3. `2+` files:
   1. fail-closed before replacement (`multiple target inputs`)

## `ledger_ref` cardinality and learning model

`inputs/ledger_ref/` accepts `0+` files.

Learning rules:
1. `ledger_ref` is the raw teacher input (Yayoi finalized exports)
2. learning updates are append-only
3. ingestion dedupe is by per-file SHA256
4. applied SHA256 tracking prevents double-learning on re-run with the same teacher file
5. multiple historical files can be accumulated over time

## Derived teacher artifacts

Raw `ledger_ref` remains the source-of-truth input, but cache learning now flows through managed derived teacher artifacts:
1. `artifacts/derived/cc_teacher/<RAW_SHA256>__cc_teacher.csv` is materialized for each ingested raw `ledger_ref` source.
2. `artifacts/derived/cc_teacher_manifest.json` is the deterministic manifest index for those derived outputs.
3. `rulesets/credit_card_statement/teacher_extraction_rules_v1.json` defines the tracked extraction ruleset.
4. `config/credit_card_line_config.json` carries `teacher_extraction` settings, including:
   1. payable candidate accounts
   2. soft-match thresholds
   3. canonical payable thresholds
5. Cache learning must use derived teacher rows only; raw `ledger_ref` rows are not learned directly.
6. A raw source that yields zero selected rows must still appear in `cc_teacher_manifest.json` and contributes zero learned rows.
7. Raw target payable account names in `inputs/kari_shiwake/` are placeholders for payable-side detection and are not authoritative final output account names.
8. Final authoritative payable-side output account comes from cache `canonical_payable` when runtime can safely use it.

## Inference field constraint

Inference must use summary text only:
1. summary column is the 17th column (1-based)

Non-summary fields are not inference signals for this line contract.
Memo (22nd column) is not an inference source.
