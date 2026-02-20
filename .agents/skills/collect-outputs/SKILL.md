---
name: collect-outputs
description: クライアント横断で run 成果物（置換CSV・レビューCSV・manifest）を収集し、exports/collect に単一ZIPを出力します。receipt は legacy 互換探索、bank_statement は line スコープのみです。
---

# collect-outputs

クライアント横断で run 成果物を収集し、1つの ZIP にまとめます。

## 対象
1. `*_replaced_*.csv`
2. `*_review_report.csv`
3. `run_manifest.json` と `*_manifest.json`

## line 対応
1. `--line receipt` が既定です。
2. receipt では以下を両方探索します:
   1. `clients/<CLIENT_ID>/lines/receipt/outputs/runs/`
   2. `clients/<CLIENT_ID>/outputs/runs/` (legacy)
3. `bank_statement` は line スコープのみ探索します:
   1. `clients/<CLIENT_ID>/lines/bank_statement/outputs/runs/`
4. `credit_card_statement` は未実装のため fail-closed です。

## 出力
1. `exports/collect/collect_<JST_DATE>_<UTC_TS>_<SHA8>.zip`
2. `exports/collect/LATEST.txt`

## 実行
```bash
python .agents/skills/collect-outputs/scripts/collect_outputs.py --line receipt
```

## 非対話オプション
```bash
python .agents/skills/collect-outputs/scripts/collect_outputs.py --line receipt --date YYYY-MM-DD --client A,B --time HH:MM-HH:MM --yes
```
