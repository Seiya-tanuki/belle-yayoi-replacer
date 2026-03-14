# Belle Yayoi Skillpack (Codex)

Deterministic toolchain for line-aware Yayoi 25-column import CSV replacement.

## Implemented lines

1. `receipt`
2. `bank_statement`
3. `credit_card_statement`

## Core behavior

1. `receipt`: replace only debit account column (column 5); inference uses summary column (column 17) only; memo column (column 22) is not used.
2. `bank_statement`: replace only the fields defined by `spec/BANK_REPLACER_SPEC.md`; summary may be rewritten and memo `SIGN` may be used as a fallback signal.
3. `credit_card_statement`: replace placeholder account and payable-side subaccount per `spec/CREDIT_CARD_REPLACER_SPEC.md`; inference uses summary and does not use memo.
4. Keep everything offline (no network dependency).

## Active skills

1. `$client-register`
2. `$yayoi-replacer`
3. `$client-cache-builder`
4. `$lexicon-extract`
5. `$lexicon-apply`
6. `$export-lexicon-review-pack`
7. `$backup-assets`
8. `$restore-assets`
9. `$system-diagnose`
10. `$collect-outputs`
11. `$migrate-line-layout`

## Canonical line inputs (current)

1. `receipt`: `clients/<CLIENT_ID>/lines/receipt/inputs/kari_shiwake/` and `clients/<CLIENT_ID>/lines/receipt/inputs/ledger_ref/`
2. `bank_statement`: `clients/<CLIENT_ID>/lines/bank_statement/inputs/kari_shiwake/`, `clients/<CLIENT_ID>/lines/bank_statement/inputs/training/ocr_kari_shiwake/`, and `clients/<CLIENT_ID>/lines/bank_statement/inputs/training/reference_yayoi/`
3. `credit_card_statement`: `clients/<CLIENT_ID>/lines/credit_card_statement/inputs/kari_shiwake/` and `clients/<CLIENT_ID>/lines/credit_card_statement/inputs/ledger_ref/`
4. Legacy receipt fallback (deprecated): `clients/<CLIENT_ID>/inputs/*`

## Receipt lexicon pending workflow

1. `$yayoi-replacer` updates `client_cache` from `ledger_ref`.
2. `$yayoi-replacer` then auto-grows `lexicon/receipt/pending/label_queue.csv` from `ledger_ref`.
3. `$lexicon-extract` can run the same autogrow manually.
4. `$lexicon-apply` applies only `action=ADD` rows.

All queue/state mutation is protected by:
1. `lexicon/receipt/pending/locks/label_queue.lock`

## Phase 2 migration utility

Migrate legacy receipt layout into canonical line-scoped layout with fail-closed safety checks.

```bash
python .agents/skills/migrate-line-layout/scripts/migrate_line_layout.py --client ALL --dry-run true --line receipt
```

Apply migration (copy mode):

```bash
python .agents/skills/migrate-line-layout/scripts/migrate_line_layout.py --client ALL --mode copy --apply --dry-run false --line receipt
```

## Specs

See `spec/` for canonical contracts.

credit-card line references:
1. `spec/CREDIT_CARD_LINE_INPUTS_SPEC.md`
2. `spec/CREDIT_CARD_CLIENT_CACHE_SPEC.md`
3. `spec/CREDIT_CARD_REPLACER_SPEC.md`
