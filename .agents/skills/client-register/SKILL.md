---
name: client-register
description: Register a new client directory by copying clients/TEMPLATE to clients/<CLIENT_ID> with strict Windows-safe name validation. Explicit invocation only.
---

# client-register

新しい顧客ディレクトリを安全に作成するスキルです。

## 前提
1. 必ず `$client-register` を明示呼び出しして使う。
2. 実行はローカルファイル環境で行う（ネットワーク不要）。

## 実行内容
1. 入力名を検証し、Windows で安全な `CLIENT_ID` に正規化する。
2. `clients/TEMPLATE/` を `clients/<CLIENT_ID>/` にコピーする。
3. `clients/<CLIENT_ID>/config/category_overrides.json` を full-expanded で初期生成する。
4. 次の投入先を案内する:
   1. `inputs/kari_shiwake/`
   2. `inputs/ledger_ref/`
   3. `inputs/ledger_train/`

## Template contract (must preserve)
1. `clients/TEMPLATE/config/` exists.
2. `clients/TEMPLATE/outputs/runs/` exists.
3. `clients/TEMPLATE/artifacts/cache/` exists.
4. `clients/TEMPLATE/artifacts/ingest/` exists.
5. `clients/TEMPLATE/artifacts/telemetry/` exists.
6. Use `.gitkeep` files as needed to keep empty directories in git.

## Execution
```bash
python3 .agents/skills/client-register/register_client.py
```
