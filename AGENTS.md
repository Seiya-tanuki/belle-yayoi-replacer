# AGENTS.md (Belle / Yayoi suite)

## 0) ユーザーとの対話言語
1. **チャットでの応答（ユーザーとのやり取り）は全て日本語**で行うこと。
2. ただし、出力成果物（JSON/CSV等のファイル）内部の言語は、安定動作を優先して英語を基本としてよい。
   1. 例外：辞書キーワード・勘定科目名・弥生CSVの内容は日本語のまま扱う。

## 1) 運用前提（最重要）
1. このリポジトリは **Codex Agent Skills** によって動作する。
2. **必ず `$skill` を明示呼び出し**して作業を開始すること（暗黙起動は禁止）。
   1. skills は `.agents/skills/*` に配置されている。
3. スキルは責務ごとに分離されている（混線防止）：
   1. `$yayoi-replacer` : 仮仕訳CSVの借方勘定科目（5列目）だけを置換
   2. `$client-cache-builder` : ledger_ref を取り込み client_cache キャッシュを増分更新
   3. `$lexicon-extract` : ledger_train から未登録語を抽出し label_queue.csv を育成
   4. `$lexicon-apply` : label_queue.csv の ADD 行だけを lexicon.json に反映

## 2) データ配置（client単位で取り違え防止）
1. すべての入力/出力は `clients/<CLIENT_ID>/` 配下に閉じる。
2. 入力の用途別ディレクトリ（固定）：
   1. `inputs/kari_shiwake/` : 置換対象「仮仕訳CSV」
   2. `inputs/ledger_ref/`   : 参照用「過去の実仕訳CSV」（client_cache作成・T番号統計）
   3. `inputs/ledger_train/` : 学習用「過去の実仕訳CSV」（辞書learned育成）
3. 生成物：
   1. `artifacts/client_cache.json`（append-onlyキャッシュ）
   2. `artifacts/ledger_ref_ingested.json`（ledger_ref取込マニフェスト）
   3. `artifacts/ledger_train_ingested.json`（ledger_train取込マニフェスト）
   4. `outputs/*`
   5. `artifacts/reports/*`

## 3) 重要な安全制約（弥生インポートを壊さない）
1. 弥生会計インポートCSVは **25列固定**で扱う。
2. 置換エージェント（$yayoi-replacer）が変更してよいのは **借方勘定科目（5列目）のみ**。
3. それ以外の列（摘要/税区分/メモ等）は **一切変更しない**（バイト同一を目標）。
4. 解析・推定に使用してよいのは **摘要（17列目）のみ**。
   1. 仕訳メモ（22列目）は推定に使用しない（禁止）。
5. `##DUMMY_OCR_UNREADABLE##` はダミー行として扱い、置換は行わずレビュー優先度を上げる。

## 4) ネットワークアクセス
1. このプロジェクトは **外部Webアクセスなし**で完結する前提。
2. `lexicon/lexicon.json` と `clients/<CLIENT_ID>/artifacts/*` のローカル情報だけで決定論的に処理する。

## 5) 正本ファイルとキャッシュ
1. 共通辞書（単一正本）：`lexicon/lexicon.json`（core + learned）
2. 未登録語キュー（ユーザーが時々編集する）：`lexicon/pending/label_queue.csv`
3. デフォルト科目：`defaults/category_defaults.json`
4. client差分キャッシュ：`clients/<CLIENT_ID>/artifacts/client_cache.json`
   1. client_cache は **再生成ではなく増分更新（append-only）**を基本とする。
   2. ledger_ref の取込状況は `ledger_ref_ingested.json` と `client_cache.applied_ledger_ref_sha256` で管理する。

## 6) 仕様は spec/ を唯一の正本とする
1. スキーマ・更新ルール・置換順序などの厳密仕様は `spec/` 配下を参照する。
2. skill本文（SKILL.md）は薄く保ち、詳細は `spec/` を参照すること。

