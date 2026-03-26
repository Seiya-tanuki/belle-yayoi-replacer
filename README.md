# Belle Yayoi Skillpack (Codex)

Deterministic toolchain for line-aware Yayoi 25-column import CSV replacement.
Shared canonical lexicon lives at `lexicon/lexicon.json`.
The current taxonomy is the reconstructed 69-category operational/posting taxonomy used for deterministic routing.

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

Repository baseline tracks only placeholders under `lexicon/receipt/pending/`.
Queue/state/log files are generated at runtime from empty state.

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

## Developer Validation

Run the canonical repo-owned validation command from the repository root:

```bash
python tools/run_tests.py
```

This is the standard unittest entrypoint for fresh checkouts and ZIP-extracted copies.
It sets up import resolution from the script location, so no manual `PYTHONPATH=.` setup is required.

Optional legacy direct command from the repository root only:

```bash
python -m unittest discover -s tests -v
```

## Local UI

`Belle ローカルUI` は、日常運用で使う最低限の操作をブラウザから順に進めるためのローカル向け UI です。
対象はクライアント選択、新規作成、line 選択、入力ファイル配置、事前確認、実行、成果物 ZIP 作成です。

起動手順:

```bash
python -m pip install -r requirements-ui.txt
python start_local_ui.py
```

この UI は daily operation 向けの薄い操作盤で、既存 CLI / skill の置き換えではありません。
辞書編集、config 編集、review CSV 編集、複雑な保守操作は対象外です。
