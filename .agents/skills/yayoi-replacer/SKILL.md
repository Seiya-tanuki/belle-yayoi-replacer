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
10. `bank_statement` target should satisfy Contract A assumptions (one target CSV should represent one passbook/account context).
11. `bank_statement` bank-side subaccount fill is file-level:
   - inference source: row votes from `cache.bank_account_subaccount_stats`
   - when inference is `OK`, the SAME inferred bank-side subaccount is applied to all required-fill rows in the file (no partial fill)
   - thresholds: `thresholds.file_level_bank_sub_inference.min_votes` (default `3`) and `thresholds.file_level_bank_sub_inference.min_p_majority` (default `0.9`)
   - if required fill exists and inference is not `OK`, runner strict-stops with exit `2` after writing artifacts (`bank_sub_fill_required_failed == true`)
12. `credit_card_statement` is implemented/runnable.
13. `credit_card_statement` target must satisfy Contract A (one statement per target file).

## Shared tax postprocess config
1. Shared config path:
   - `clients/<CLIENT_ID>/config/yayoi_tax_config.json`
2. Runtime tax amount auto-fill is controlled by this shared config.
3. Missing shared config defaults to disabled / no-op.
4. Current v1 auto-fill scope is intentionally narrow:
   - `bookkeeping_mode = tax_excluded`
   - tax amount cell is blank
   - tax division is parseable as `inner`
   - `rounding_mode = floor`

## Operator protocol (mandatory)
Codex/operator は以下の手順を固定で実施すること。

### Step 1: クライアント確認（実行前）
1. ユーザーに `CLIENT_ID` を確認し、なければ次をそのまま提示する:
   - 「対象を教えるクライアントIDを指定してください。」
2. `CLIENT_ID` を確認・復唱してから次へ進む。

### Step 2: 事前確認（`--dry-run` を先に実施）
1. 必ず次のコマンドを実行する。
```bash
python .agents/skills/yayoi-replacer/scripts/run_yayoi_replacer.py --client "<CLIENT_ID>" --line all --dry-run
```
2. PLAN で line ごとに次を確認する。
   - `SKIP`: 対象入力 0 件
   - `RUN`: 実行可能（契約OK）
   - `FAIL`: 契約違反または必須資材不足
3. PLAN の結果をユーザーに提示する。
4. FAIL がある場合は原因を案内し、Step 2 を再実施する（FAIL 解消まで本実行しない）。
5. `--dry-run` に `--yes` は付けない（dry-run は `--yes` 不要）。

### Step 3: 実行（ユーザー承認後のみ）
1. ユーザーが「実行して」と明示した場合のみ実行する。
2. 実行コマンドは次を使う。
```bash
python .agents/skills/yayoi-replacer/scripts/run_yayoi_replacer.py --client "<CLIENT_ID>" --line all --yes
```
3. 運用ルール:
   - ユーザーが「実行して」と言う前に `--yes` 実行しない。
   - Step 2 を省略しない。

### 必須ファイルメモ（診断結果優先）
1. `receipt` line: 対象は `inputs/kari_shiwake/` 配下。実行時アセット不足は PLAN の `fail` 内容をそのまま提示する。
2. `bank_statement` line: training は任意（`ocr_kari_shiwake=0` かつ `reference_yayoi=0` は no-op）。training 実施時は `inputs/training/ocr_kari_shiwake/` にCSV1件 + `inputs/training/reference_yayoi/` にCSV/TXT1件のみ許可（片側のみ/複数は fail-closed）。
3. `credit_card_statement` line: 対象は `clients/<CLIENT_ID>/lines/credit_card_statement/inputs/kari_shiwake/`。件数は `0 => SKIP`, `1 => RUN`, `2+ => FAIL`（plan-time）。
4. `credit_card_statement` runtime strict-stop: `payable_sub_fill_required_failed == true` の場合、成果物を書き出した後に exit `2`（`SystemExit(2)`、run_dir保持）。

### Examples (dialog)
1. User: 「yayoi-replacerを実行して」
   - Operator: 「対象のクライアントIDを指定してください。」
2. User: 「CLIENT_ID は acme」
   - Operator: Step 2 の `--dry-run` を実行して PLAN 結果を提示。
3. User: 「実行して」
   - Operator: Step 3 の `--yes` で実行。

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
   - in `--line all`: 他ラインと同様に input 契約で `RUN` / `SKIP` / `FAIL` を判定（未実装扱いの特別SKIPはしない）
   - PLAN は file-level card inference の確信度不足を事前確定できないため、実行時に strict-stop（exit `2`）が起こりうる
   - strict-stop 条件: `payable_sub_fill_required_failed == true`（成果物は保持）

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
5. During execution, strict-stop returns exit 2 after writing artifacts for:
   - `bank_statement` when `bank_sub_fill_required_failed == true`
   - `credit_card_statement` when `payable_sub_fill_required_failed == true`

## Runtime behavior (line execution)
1. The skill entrypoint is a dispatcher only.
2. Line runners are separated under `belle/line_runners/`:
   - `receipt.py`
   - `bank_statement.py`
   - `credit_card_statement.py`
3. `bank_statement` runner enforces file-level bank-side subaccount inference behavior:
   - one inferred value per target CSV (no hybrid/partial bank-side fill)
   - strict-stop via `SystemExit(2)` when `bank_sub_fill_required_failed == true` after artifact write
   - thresholds path: `thresholds.file_level_bank_sub_inference.min_votes` / `min_p_majority`
4. `credit_card_statement` runner enforces Contract A assumptions and strict-stop behavior.

## Canonical specs
1. `spec/REPLACER_SPEC.md`
2. `spec/CATEGORY_OVERRIDES_SPEC.md`
3. `spec/CLIENT_CACHE_SPEC.md`
4. `spec/LEXICON_PENDING_SPEC.md`
5. `spec/BANK_REPLACER_SPEC.md`
6. `spec/CREDIT_CARD_LINE_INPUTS_SPEC.md`
7. `spec/CREDIT_CARD_CLIENT_CACHE_SPEC.md`
8. `spec/CREDIT_CARD_REPLACER_SPEC.md`

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
