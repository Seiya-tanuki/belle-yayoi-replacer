# BANK_LINE_INPUTS_SPEC (bank_statement line v0)

## Scope and status

This spec defines v0 input contracts for `line_id=bank_statement` only.
This contract is implemented in current line-split runtime behavior.

Implementation status:
1. `receipt`: implemented/runnable via explicit skills.
2. `bank_statement`: implemented/runnable via explicit skills.
3. `credit_card_statement`: UNIMPLEMENTED (must remain fail-closed).

Related specs:
1. `spec/BANK_CLIENT_CACHE_SPEC.md`
2. `spec/BANK_REPLACER_SPEC.md`
3. `spec/FILE_LAYOUT.md`

## Allowed data sources (only these three)

Per-client canonical paths:
1. Past OCR draft (training OCR):
   1. `clients/<CLIENT_ID>/lines/bank_statement/inputs/training/ocr_kari_shiwake/`
2. Teacher reference (training reference):
   1. `clients/<CLIENT_ID>/lines/bank_statement/inputs/training/reference_yayoi/`
3. Current OCR draft (target):
   1. `clients/<CLIENT_ID>/lines/bank_statement/inputs/kari_shiwake/`

No other source is allowed for bank v0 learning/inference.
Forbidden paths for bank v0:
1. `clients/<CLIENT_ID>/lines/bank_statement/inputs/ledger_ref/**`
2. `clients/<CLIENT_ID>/lines/bank_statement/artifacts/ingest/ledger_ref/**`

## Training pair concept (before/after)

Training uses before/after row pairs built from:
1. Before: row from training OCR (`ocr_kari_shiwake`)
2. After: matching row from training reference (`reference_yayoi`)

Pairing key:
1. `(date, sign, amount)`

Uniqueness rule (fail-closed):
1. A key is usable only when it appears exactly once in training OCR and exactly once in training reference.
2. If a key collides on either side (2 or more rows), the key is skipped.
3. If a key exists on only one side, it is skipped.
4. Skipped rows never enter cache statistics.

## Field definitions for pairing

### date

Transaction date from the Yayoi row date field, normalized to one canonical date representation before keying.

### sign

`sign` is bank-statement sign:
1. `debit`: withdrawal
2. `credit`: deposit

Consistency requirements:
1. OCR rows should use OCR memo `SIGN` when present.
2. Teacher rows derive sign from which side contains the identified bank account.
3. If sign cannot be determined deterministically for a row, that row is not pairable.

### amount

Normalized numeric amount used as part of the key.
If the row does not provide a deterministic single amount for keying, it is not pairable.

## `kana_key` definition

`kana_key` is a normalized summary string derived from OCR summary text.
Normalization steps:
1. Unicode NFKC normalization
2. trim leading/trailing spaces
3. remove internal full-width/half-width spaces
4. normalize punctuation variants to a canonical form
5. uppercase/lowercase folding for stable matching where applicable

`kana_key` is used by cache/replacer lookups, not as a standalone pairing key.

## Fail-closed requirements

1. Ambiguous pairing keys are skipped, never guessed.
2. Missing sign/date/amount needed for pairing causes skip.
3. Unsupported lines (e.g. `credit_card_statement`) remain unimplemented and fail-closed.

## Teacher-side bank subaccount learning note

1. Teacher rows also provide bank-side subaccount (the side whose account equals `bank_account_name`).
2. This value is used only for `bank_account_subaccount_stats` learning in bank client cache.
3. It is independent from counter-label (`BankLabel`) statistics.
