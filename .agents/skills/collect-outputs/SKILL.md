---
name: collect-outputs
description: クライアント横断で run 成果物（置換CSV・レビューCSV・manifest）を収集し、exports/collect に単一ZIPを出力します。既定は --line all で、line別に名前空間化して同梱します。
---

# collect-outputs

クライアント横断で run 成果物を収集し、1つの ZIP にまとめます。

## 対象ファイル
1. `*_replaced_*.csv`
2. `*_review_report.csv`
3. `run_manifest.json` と `*_manifest.json`

## line 指定
1. `--line` の選択肢: `receipt`, `bank_statement`, `credit_card_statement`, `all`
2. 既定値は `--line all`
3. `--line all` のとき:
   1. 固定順 `receipt -> bank_statement -> credit_card_statement` で収集します。
   2. 各 line ごとに eligible run が 0 件なら、その line はスキップします（失敗しません）。
   3. 全 line 合計で eligible run が 0 件なら、`no runs found` で失敗します（exit code 1）。
4. ルート探索:
   1. 全 implemented line で line-scoped root のみを対象にします。
   2. legacy client root 配下の `outputs/runs/` は収集対象外です。

## ZIP 構造
1. `--line all` では line_id ごとに名前空間化して格納します。
2. 例:
   1. `receipt/csv/...`
   2. `bank_statement/reports/...`
   3. `credit_card_statement/manifests/...`
3. `MANIFEST.json` には line別件数、line別 run_id 一覧、スキップされた line 一覧を含みます。

## 出力
1. `exports/collect/collect_<JST_DATE>_<UTC_TS>_<SHA8>.zip`
2. `exports/collect/LATEST.txt`

## 実行例
```bash
python .agents/skills/collect-outputs/scripts/collect_outputs.py --yes
python .agents/skills/collect-outputs/scripts/collect_outputs.py --line all --date YYYY-MM-DD --client A,B --time HH:MM-HH:MM --yes
python .agents/skills/collect-outputs/scripts/collect_outputs.py --line receipt --date YYYY-MM-DD --client A --yes
```
