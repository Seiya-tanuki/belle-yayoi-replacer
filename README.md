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

## Active inputs

Under `clients/<CLIENT_ID>/inputs/`:
1. `kari_shiwake/`
2. `ledger_ref/`

## Lexicon pending workflow

1. `$yayoi-replacer` updates `client_cache` from `ledger_ref`.
2. `$yayoi-replacer` then auto-grows `lexicon/pending/label_queue.csv` from `ledger_ref`.
3. `$lexicon-extract` can run the same autogrow manually.
4. `$lexicon-apply` applies only `action=ADD` rows.

All queue/state mutation is protected by:
1. `lexicon/pending/locks/label_queue.lock`

## Specs

See `spec/` for canonical contracts.
