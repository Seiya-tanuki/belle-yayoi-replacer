# Belle Yayoi Skillpack (Codex)

Deterministic toolchain for line-aware Yayoi 25-column import CSV replacement.
Shared canonical lexicon lives at `lexicon/lexicon.json`.
The current taxonomy is the reconstructed 69-category operational/posting taxonomy used for deterministic routing.

## Implemented lines

1. `receipt`
2. `bank_statement`
3. `credit_card_statement`

## Core behavior

1. `receipt`: replace debit account and debit-side tax division; inference uses summary column (column 17) only; memo column (column 22) is not used.
2. `bank_statement`: replace only the fields defined by `spec/BANK_REPLACER_SPEC.md`; bank keeps its own tax-division replacement logic, then shared tax postprocess may fill tax amount.
3. `credit_card_statement`: replace placeholder-side account, placeholder-side tax division, rewrite the payable side to cache `canonical_payable` when safe, and fill payable-side subaccount per `spec/CREDIT_CARD_REPLACER_SPEC.md`; the placeholder side may be `debit` or `credit`.
4. Keep everything offline (no network dependency).

## Receipt active config

1. Active runtime config path: `clients/<CLIENT_ID>/lines/receipt/config/receipt_line_config.json`
2. Tracked TEMPLATE provisioning baseline: `clients/TEMPLATE/lines/receipt/config/receipt_line_config.json`

## Defaults / overrides contract

1. `receipt` and `credit_card_statement` defaults/overrides use the live row shape:
   - `target_account`
   - `target_tax_division`
2. `receipt` interprets that contract on the debit side.
3. `credit_card_statement` interprets that contract on the placeholder side.
4. `bank_statement` does not use `category_overrides.json`.

## Shared tax postprocess config

Shared client config path:
1. `clients/<CLIENT_ID>/config/yayoi_tax_config.json`
2. This shared config controls runtime tax amount auto-fill.
3. The shared tax postprocess runs after line-specific tax-division replacement on `receipt`, `bank_statement`, and `credit_card_statement`.

Current default behavior:
1. Missing config resolves to disabled / no-op.
2. New client bootstrap requires an explicit bookkeeping mode choice and writes `clients/<CLIENT_ID>/config/yayoi_tax_config.json` during registration.
3. `tax_excluded` writes `enabled: true`, `bookkeeping_mode: tax_excluded`, `rounding_mode: floor`.
4. `tax_included` writes `enabled: false`, `bookkeeping_mode: tax_included`, `rounding_mode: floor`.
5. `receipt` / `credit_card_statement` bootstrap seeds `category_overrides.json` from the defaults variant that matches the selected bookkeeping mode.

Current v1 runtime scope:
1. `bookkeeping_mode = tax_excluded`
2. tax amount cell is blank
3. tax division is parseable as `inner`
4. `rounding_mode = floor`

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

## Canonical line inputs (current)

1. `receipt`: `clients/<CLIENT_ID>/lines/receipt/inputs/kari_shiwake/` and `clients/<CLIENT_ID>/lines/receipt/inputs/ledger_ref/`
2. `bank_statement`: `clients/<CLIENT_ID>/lines/bank_statement/inputs/kari_shiwake/`, `clients/<CLIENT_ID>/lines/bank_statement/inputs/training/ocr_kari_shiwake/`, and `clients/<CLIENT_ID>/lines/bank_statement/inputs/training/reference_yayoi/`
3. `credit_card_statement`: `clients/<CLIENT_ID>/lines/credit_card_statement/inputs/kari_shiwake/` and `clients/<CLIENT_ID>/lines/credit_card_statement/inputs/ledger_ref/`

## Receipt lexicon pending workflow

Repository baseline tracks only placeholders under `lexicon/receipt/pending/`.
Queue/state/log files are generated at runtime from empty state.

1. `$yayoi-replacer` updates `client_cache` from `ledger_ref`.
2. `$yayoi-replacer` then auto-grows `lexicon/receipt/pending/label_queue.csv` from `ledger_ref`.
3. `$lexicon-extract` can run the same autogrow manually.
4. `$lexicon-apply` applies only `action=ADD` rows.

All queue/state mutation is protected by:
1. `lexicon/receipt/pending/locks/label_queue.lock`

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
