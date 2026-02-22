---
name: yayoi-replacer
description: Deterministic multi-line replacer for Yayoi 25-col CSV. Always use PLAN/dry-run then explicit user approval.
---

# yayoi-replacer

Deterministic replacement skill for Yayoi import CSVs.

## Preconditions
1. `--client <CLIENT_ID>` is required.
2. `--line` choices: `receipt`, `bank_statement`, `credit_card_statement`, `all` (default: `all`).
3. Work under a single client folder.
4. Receipt preferred line layout:
   - `clients/<CLIENT_ID>/lines/receipt/`
5. Receipt legacy fallback (deprecated, auto-detected if line layout missing):
   - `clients/<CLIENT_ID>/`
6. Bank line is line-scoped only:
   - `clients/<CLIENT_ID>/lines/bank_statement/`
7. `bank_statement` training is optional:
   - when both inboxes are empty (`inputs/training/ocr_kari_shiwake/` = 0 and `inputs/training/reference_yayoi/` = 0), learning is skipped as a normal no-op
   - if training is provided, per run it must be exactly one pair: OCR `*.csv` = 1 and reference (`*.csv` or `*.txt`) = 1
   - one-side-only (`1/0`, `0/1`) or multiple files on either side (`2+`) is fail-closed
8. `bank_statement` pair-set idempotency:
   - `pair_set_sha256 = sha256("ocr=<ocr_sha>|ref=<ref_sha>")`
   - if already applied in `client_cache.applied_training_sets`, learning is skipped, but both inbox files are still ingested for cleanup (duplicate records are kept)
9. `bank_statement` fail-closed gates for new learning:
   - one-side-new manifest mismatch (`ocr_sha_known != ref_sha_known`)
   - both OCR/reference SHAs are known in manifests but the pair-set is not applied in cache
   - `pairs_unique_used == 0` (no ingest/cache write side effects; inbox files remain)
10. `credit_card_statement` is currently unimplemented.

## Operator protocol (mandatory)
この手順は Codex/operator 実行時の最上位ランブックであり、必ずこの順序で実施すること。

### Step 1: クライアント指定（推測禁止）
1. ユーザーがクライアントを明示していない場合は、必ず次をそのまま返す:
   - 「置換を行うクライアントを指定してください。」
2. `CLIENT_ID` を推測・補完してはならない。

### Step 2: 事前確認（`--dry-run` を常に実行）
1. 必ず次のコマンドを実行する:
```bash
python .agents/skills/yayoi-replacer/scripts/run_yayoi_replacer.py --client "<CLIENT_ID>" --line all --dry-run
```
2. 実行後は line ごとに次のラベルで要約する:
   - `置換対象なし` : `skip`（kari_shiwakeが0件）
   - `置換可能` : `ready`（必要ファイルの確認がOK）
   - `必須ファイル不足` : `fail`（不足内容を明示）
3. その後、必ず次の文言をそのまま表示する:
   - 「実行前の確認結果です。この内容で実行しますか？実行する場合は"実行を許可"と入力してください。」
4. ユーザーがファイル追加・差し替え後に再確認を求めた場合は、必ず Step 2 を再実行する。自動で本実行へ進んではならない。
5. `--dry-run` に `--yes` を付け足してはならない（dry-run は `--yes` 不要）。

### Step 3: 実行（承認トークン受領後のみ）
1. ユーザー入力が **完全一致で** 「実行を許可」の場合のみ、本実行へ進んでよい。
2. 実行コマンドは次を用いる:
```bash
python .agents/skills/yayoi-replacer/scripts/run_yayoi_replacer.py --client "<CLIENT_ID>" --line all --yes
```
3. 禁止事項:
   - ユーザーが「実行を許可」と入力する前に `--yes` を付けて実行してはならない。
   - Step 2 を省略してはならない。

### 必須ファイルメモ（診断結果優先）
1. `receipt` line: 対象は `inputs/kari_shiwake/` 配下。実行時アセット不足は PLAN の `fail` 内容をそのまま提示する。
2. `bank_statement` line: training は任意（`ocr_kari_shiwake=0` かつ `reference_yayoi=0` は no-op）。training 実施時は `inputs/training/ocr_kari_shiwake/` にCSV1件 + `inputs/training/reference_yayoi/` にCSV/TXT1件のみ許可（片側のみ/複数は fail-closed）。
3. `credit_card_statement` line: 未実装のため `--line all` では skip。

### Examples (dialog)
1. User: 「yayoi-replacerを実行して」
   - Operator: 「置換を行うクライアントを指定してください。」
2. User: 「CLIENT_ID は acme」
   - Operator: Step 2 の `--dry-run` を実行して結果要約を提示し、次を表示:
   - 「実行前の確認結果です。この内容で実行しますか？実行する場合は"実行を許可"と入力してください。」
3. User: 「実行を許可」
   - Operator: Step 3 の `--yes` コマンドで実行する。

## PLAN semantics (always printed)
1. The skill always performs preflight planning and prints:
   - `[PLAN] client=<CLIENT_ID> line=<...>`
   - one line per selected line with `RUN` / `SKIP` / `FAIL`
2. `SKIP` only when target input count in `inputs/kari_shiwake/` is 0.
3. `FAIL` when:
   - target input count is 2 or more
   - required runtime/config is missing
   - structural invariants are invalid
   - bank training input contract or learning safety gates are violated (`0/0` is allowed, otherwise `1/1` only; one-side-new mismatch / manifests-known-but-not-applied / `pairs_unique_used == 0`)
4. `credit_card_statement` behavior:
   - in `--line all`: `SKIP (unimplemented)`
   - explicit `--line credit_card_statement`: exit 2 with clear unimplemented error

## Confirmation gate
1. If PLAN has any `FAIL`, execution is blocked and exits 1.
2. `--dry-run` prints PLAN and exits:
   - 0 when no `FAIL`
   - 1 when `FAIL` exists
3. If there are `RUN` lines:
   - `--yes`: proceed without prompt
   - interactive TTY: prompt `Proceed with RUN lines? [y/N]`
   - non-interactive without `--yes`: exit 2 with guidance
4. If all selected lines are `SKIP`, exits 0 with `[OK] nothing to do`.

## Runtime behavior (line execution)
1. The skill entrypoint is a dispatcher only.
2. Line runners are separated under `belle/line_runners/`:
   - `receipt.py`
   - `bank_statement.py`
   - `credit_card_statement.py`
3. Receipt and bank execution logic remain unchanged in behavior; only orchestration is refactored.

## Canonical specs
1. `spec/REPLACER_SPEC.md`
2. `spec/CATEGORY_OVERRIDES_SPEC.md`
3. `spec/CLIENT_CACHE_SPEC.md`
4. `spec/LEXICON_PENDING_SPEC.md`
5. `spec/BANK_REPLACER_SPEC.md`

## Execution examples
```bash
python .agents/skills/yayoi-replacer/scripts/run_yayoi_replacer.py --client <CLIENT_ID>
```

```bash
python .agents/skills/yayoi-replacer/scripts/run_yayoi_replacer.py --client <CLIENT_ID> --line receipt --yes
```

```bash
python .agents/skills/yayoi-replacer/scripts/run_yayoi_replacer.py --client <CLIENT_ID> --line all --dry-run
```

```bash
python .agents/skills/yayoi-replacer/scripts/run_yayoi_replacer.py --client "<CLIENT_ID>" --line all --dry-run
```

```bash
python .agents/skills/yayoi-replacer/scripts/run_yayoi_replacer.py --client "<CLIENT_ID>" --line all --yes
```
