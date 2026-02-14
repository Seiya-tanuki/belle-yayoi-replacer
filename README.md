# Belle Yayoi Skillpack (Codex)

このリポジトリは、弥生会計インポート用の **25列CSV（仮仕訳）**のうち、
**借方勘定科目（5列目）だけ**を推定置換するための Codex Skills セットです。

- 置換の根拠：T番号 / (T番号×カテゴリ) / 取引先キー / カテゴリ（lexicon） / デフォルト科目（category_defaults）
- 最終確認は人間が行う前提（税理士業務）

## 使い方（基本）
1. `$client-register` を明示呼び出しして `clients/TEMPLATE/` から `clients/<CLIENT_ID>/` を作る
2. `clients/<CLIENT_ID>/inputs/...` にファイルを置く
3. Codex で **必ず `$skill` を明示呼び出し**して実行する（暗黙起動は禁止）

### 主なskills
1. `$client-register`
   1. 顧客名を厳格検証し、`clients/TEMPLATE/` を `clients/<CLIENT_ID>/` にコピー
   2. Windows/WSL で安全なディレクトリ名に正規化（NFKC / space→underscore）
2. `$yayoi-replacer`
   1. 仮仕訳CSVの借方勘定科目（5列目）のみ置換
   2. 実行前に client_cache キャッシュを自動で増分更新（ledger_ref を取り込み）
3. `$client-cache-builder`
   1. ledger_ref の取り込みと client_cache キャッシュの増分更新だけを行う
4. `$lexicon-extract`
   1. ledger_train から未登録語を抽出し `lexicon/pending/label_queue.csv` を育成
5. `$lexicon-apply`
   1. `label_queue.csv` の action=ADD 行だけを `lexicon/lexicon.json` に反映

厳密仕様は `spec/` を参照してください。

