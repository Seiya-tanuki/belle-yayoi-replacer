# Belle Yayoi Skillpack (Codex)

Deterministic toolchain for Yayoi 25-column import CSV replacement.

## Core behavior

1. Replace only debit account column (column 5).
2. Use only summary column (column 17) for inference.
3. Never use memo column (column 22).
4. Keep everything offline (no network dependency).

## Active skills

1. `$client-register`
2. `$yayoi-replacer`
3. `$client-cache-builder`
4. `$lexicon-extract`
5. `$lexicon-apply`
6. `$migrate-line-layout`

## Active inputs

Canonical line layout (Phase 1):
1. `clients/<CLIENT_ID>/lines/receipt/inputs/kari_shiwake/`
2. `clients/<CLIENT_ID>/lines/receipt/inputs/ledger_ref/`
3. Legacy receipt fallback (deprecated): `clients/<CLIENT_ID>/inputs/*`

## Lexicon pending workflow

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
