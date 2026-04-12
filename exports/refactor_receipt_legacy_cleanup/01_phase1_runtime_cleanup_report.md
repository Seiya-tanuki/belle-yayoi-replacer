# 01 Phase 1 Runtime Cleanup Report

## 結論
Phase 1 の完了条件は満たしました。`receipt` の runtime / precheck / maintenance entrypoint から legacy client-root layout fallback を外し、実装済み line flow の active runtime で `line_id=None` を layout 意味として使う経路を止めました。shared client-root semantics は shared helper / shared config / client registration 側に残しています。

## 事前準備の証跡
- clean check: `git status --short` は空出力でした。
- 作成ブランチ: `refactor/receipt-legacy-cleanup`
- remote branch 確認:
  - `git ls-remote --heads origin refactor/receipt-legacy-cleanup`
  - `492ce86782fcbca6bd4b5a214b75e5dc9ac1c9ec	refs/heads/refactor/receipt-legacy-cleanup`
- 作成タグ: `checkpoint/refactor-receipt-legacy-cleanup-start-20260412`
- remote tag 確認:
  - `git ls-remote --tags origin checkpoint/refactor-receipt-legacy-cleanup-start-20260412`
  - `db344b72405bf01109ca6ac11764a4f92958cd54	refs/tags/checkpoint/refactor-receipt-legacy-cleanup-start-20260412`
- レポート出力先: `exports/refactor_receipt_legacy_cleanup/`

## ベースライン
- 変更前 full suite:
  - `python tools/run_tests.py`
  - 結果: `Ran 368 tests in 25.775s`
  - 結果: `OK`

## 実装内容
### 変更ファイル
- `belle/line_runners/common.py`
  - `resolve_client_layout()` から receipt 専用 legacy fallback を削除しました。
- `belle/line_runners/receipt.py`
  - receipt plan を line-only に固定し、runtime が `client_layout_line_id=None` を受けないようにしました。
- `.agents/skills/yayoi-replacer/scripts/run_yayoi_replacer.py`
  - receipt 実行時の layout marker を `"receipt"` のみに限定しました。
- `.agents/skills/client-cache-builder/scripts/build_client_cache.py`
  - receipt 用の duplicate legacy fallback を削除し、auto-detect / `--client` 解決を line-aware のみにしました。
- `.agents/skills/lexicon-extract/scripts/run_lexicon_extract.py`
  - receipt 用の duplicate legacy fallback を削除し、auto-detect / `--client` 解決を line-aware のみにしました。
- `tests/test_paths_line_aware_provisioning.py`
  - `line_id=None` の残存意味を「legacy receipt runtime」ではなく「shared root provisioning」として明示しました。
- `tests/test_input_discovery_hardening.py`
  - client-cache-builder / lexicon-extract の receipt auto-detect が legacy root layout を拾わないことを追加検証しました。
- `tests/test_yayoi_replacer_plan_confirm.py`
  - yayoi-replacer precheck が legacy receipt root layout を FAIL 扱いにすることを追加検証しました。
- `tests/test_kari_shiwake_ingest.py`
  - receipt runtime ingest 前提を line-aware layout に更新しました。
- `tests/test_lexicon_autogrow.py`
  - line-aware runtime 前提に合わせて fail-closed integration test の fixture を更新しました。

### runtime cleanup の中身
- `belle/line_runners/common.py:28`
  - `resolve_client_layout()` は line dir が存在する場合のみ成功し、`clients/<CLIENT_ID>/` への receipt fallback をしなくなりました。
- `belle/line_runners/receipt.py:43`
  - `plan_receipt()` の details は常に `layout="line"` を設定します。
- `belle/line_runners/receipt.py:121`
  - `run_receipt()` は `client_layout_line_id: str` を受け、`receipt` 以外は `invalid receipt layout marker` で止めます。
- `.agents/skills/yayoi-replacer/scripts/run_yayoi_replacer.py:180`
  - receipt 実行時は `raw_layout == "receipt"` 以外を reject します。
- `.agents/skills/client-cache-builder/scripts/build_client_cache.py:62`
  - `_resolve_client_layout()` / `find_client_id_auto()` は `tuple[str, Path]` / `tuple[str, str]` に変わり、receipt でも Optional layout を返しません。
- `.agents/skills/lexicon-extract/scripts/run_lexicon_extract.py:36`
  - `_resolve_client_layout()` / `find_client_id_auto()` は `tuple[str, Path]` / `tuple[str, str, Path]` に変わり、receipt でも Optional layout を返しません。

## 要件別の証跡
### 1. receipt legacy runtime fallback を除去した証拠
- `belle/line_runners/common.py:28` に receipt 専用 fallback が残っていません。
- `tests/test_yayoi_replacer_plan_confirm.py:232`
  - legacy root のみを作った fixture で `run_yayoi_replacer.py --line receipt --dry-run` が
  - `receipt: FAIL (client dir not found: .../clients/C1/lines/receipt)`
  - になることを検証しています。
- `tests/test_input_discovery_hardening.py:323`
  - client-cache-builder の receipt auto-detect は legacy root の ledger_ref を候補にしません。
- `tests/test_input_discovery_hardening.py:361`
  - lexicon-extract の receipt auto-detect も legacy root の ledger_ref を候補にしません。

### 2. active receipt runtime が `client_layout_line_id is None` を受けない証拠
- `belle/line_runners/receipt.py:121`
  - `run_receipt(..., client_layout_line_id: str, ...)`
- `belle/line_runners/receipt.py:130`
  - `invalid receipt layout marker` guard を追加済みです。
- `.agents/skills/yayoi-replacer/scripts/run_yayoi_replacer.py:180-189`
  - receipt 実行前に `"receipt"` 以外の layout marker を reject します。

### 3. active runtime warning / legacy detection path が消えた証拠
- in-scope files から `legacy client layout detected` を除去しました。
- 検索結果:
  - `rg -n "legacy client layout detected|client_layout_line_id is None|if client_layout_line_id is None|line_id=None" belle .agents tests`
  - 残存 warning は `.agents/skills/collect-outputs/scripts/collect_outputs.py:304` のみです。
- この残存箇所は user 指示の non-goal にある collect-outputs であり、Phase 2 対象なので今回は未変更です。
- `tests/test_yayoi_replacer_plan_confirm.py:258`
  - receipt legacy root failure 時に `legacy client layout detected` が stdout に出ないことを検証しています。

### 4. implemented line flows が runtime layout 意味として `line_id=None` に依存しない証拠
- receipt:
  - `belle/line_runners/common.py:28`
  - `belle/line_runners/receipt.py:121`
  - `.agents/skills/yayoi-replacer/scripts/run_yayoi_replacer.py:180`
- maintenance entrypoints:
  - `.agents/skills/client-cache-builder/scripts/build_client_cache.py:62,69,126-130`
  - `.agents/skills/lexicon-extract/scripts/run_lexicon_extract.py:36,43,91-93`
- これらの in-scope entrypoint は line-aware path が無いと `client dir not found: .../lines/<line_id>` で止まります。

### 5. shared client-root semantics を壊していない証拠
- `belle/paths.py:22`
  - `get_client_root(..., line_id=None)` は shared client root 解決 helper として残っています。
- `belle/paths.py:53,57,61`
  - client registration audit helper は shared client-root 配下を使い続けます。
- `belle/paths.py:221`
  - `ensure_client_system_dirs(..., line_id=None)` も shared root provisioning 用 API として残しています。
- `tests/test_paths_line_aware_provisioning.py:43`
  - shared root provisioning が root-scoped dirs を作ることを確認しています。
- 同じテストファイル内の `test_client_registration_audit_helpers_resolve_shared_client_root_paths` も通過しています。

### 6. UI stdout marker format を保持した証拠
- marker を出す実コード行は変更していません。
  - `.agents/skills/yayoi-replacer/scripts/run_yayoi_replacer.py:46`
  - `belle/line_runners/receipt.py:290-292`
  - `belle/line_runners/bank_statement.py:333-341`
  - `belle/line_runners/credit_card_statement.py:307-310`
- `tests/test_local_ui_replacer_service`
  - PLAN / run marker 解析が通過しています。
- `tests/test_yayoi_replacer_plan_confirm`
  - `[PLAN] ...` 出力を含む契約が通過しています。

## 残存する `line_id=None` の intentional usage
- `belle/paths.py`
  - shared client-root helper 群です。client registration や shared config / shared artifacts 用の API で、implemented line runtime layout を意味しません。
- `belle/lexicon_manager.py:617`
  - `line_id is None` のとき legacy global pending queue (`lexicon/pending`) を選ぶ契約です。
  - user 指示の non-goal にある「legacy pending path cleanup」は今回の対象外です。
- `belle/build_client_cache.py:247`
  - low-level cache builder API の optional parameter は残っていますが、Phase 1 で cleanup 対象にした active runtime / precheck / maintenance entrypoint からはもう implemented line layout として使っていません。

## テスト
### required targeted tests
- `python -m unittest tests.test_paths_line_aware_provisioning`
  - `Ran 5 tests`
  - `OK`
- `python -m unittest tests.test_input_discovery_hardening`
  - `Ran 10 tests`
  - `OK`
- `python -m unittest tests.test_yayoi_replacer_plan_confirm`
  - `Ran 7 tests`
  - `OK`
- `python -m unittest tests.test_local_ui_replacer_service`
  - `Ran 13 tests`
  - `OK`
- `python -m unittest tests.test_kari_shiwake_ingest`
  - `Ran 10 tests`
  - `OK`
- `python -m unittest tests.test_bookkeeping_mode_rollout_acceptance`
  - `Ran 2 tests`
  - `OK`
- `python -m unittest tests.test_tax_postprocess_runtime_wiring`
  - `Ran 5 tests`
  - `OK`

### full suite
- 変更後 full suite:
  - `python tools/run_tests.py`
  - 結果: `Ran 371 tests in 22.942s`
  - 結果: `OK`

## 意図的な挙動変更
- `clients/<CLIENT_ID>/lines/receipt/` が無い receipt client は、`clients/<CLIENT_ID>/` に legacy layout が残っていても runtime / precheck / client-cache-builder / lexicon-extract で自動救済されなくなりました。
- failure は line-aware path 前提の `client dir not found: .../lines/receipt` として出ます。

## DoD 判定
- baseline full suite 採取: 完了
- receipt legacy runtime fallback 削除: 完了
- active receipt runtime の `line_id=None` layout 意味削除: 完了
- in-scope entry script の duplicate fallback 削除: 完了
- shared client-root semantics 維持: 完了
- UI stdout marker 契約維持: 完了
- required targeted tests: 全通過
- final full suite: 全通過
- completion report 作成: 完了
