---
name: collect-outputs
description: クライアント横断で run 成果物（置換CSV・レビューCSV・manifest）を収集し、exports/collect に単一ZIPを出力します。明示呼び出し専用。
---

# collect-outputs

クライアント横断で run 出力を収集し、コピー専用で 1 つの ZIP にまとめます。

## 既定動作
- JST 当日分の run を全クライアントから収集対象にします。
- 実行時に日本語の対話入力で任意フィルタを指定できます。
  - クライアントID（カンマ区切り）
  - 日付（`YYYY-MM-DD`, JST）
  - 時間帯（`HH:MM-HH:MM`, JST）
- 収集前にプレビューを表示し、`この内容で収集ZIPを作成しますか？ (y/N)` で確認します。
  - `N`（既定）なら ZIP は作成しません。

## 出力
- `exports/collect/collect_<JST_DATE>_<UTC_TS>_<SHA8>.zip`
- `exports/collect/LATEST.txt`

## 注意
- コピーのみを行い、ソースファイルの移動・削除はしません。
- 置換CSV（`*_replaced_*.csv`）が1件もない run は不完全扱いでスキップします。

## 実行
```bash
python .agents/skills/collect-outputs/scripts/collect_outputs.py
```

## 自動実行用オプション
```bash
python .agents/skills/collect-outputs/scripts/collect_outputs.py --date YYYY-MM-DD --client A,B --time HH:MM-HH:MM --yes
```
