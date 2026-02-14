---
name: client-register
description: Register a new client directory by copying clients/TEMPLATE to clients/<CLIENT_ID> with strict Windows-safe name validation. Explicit invocation only.
---

# client-register

新しい顧客ディレクトリを安全に作成するスキルです。

## 実行ルール
1. **明示呼び出しのみ**: 必ず `$client-register` を指定して開始する。
2. 外部ネットワークは使わず、ローカルファイル操作のみで完結する。

## フロー
1. 処理内容を日本語で案内する。
2. `登録したい顧客名（ディレクトリ名に使います）` を1行で受け取る。
3. 入力名を厳格に検証する（Windows/WSL 安全な名前）。
4. 無効な場合は理由を表示し、別名で再実行するよう案内して終了する（何も作成しない）。
5. 有効な場合は正規化後のディレクトリ名を表示する。
6. `clients/TEMPLATE/` を `clients/<CANONICAL_NAME>/` へコピーする。
7. 完了後に次の作業（inputs 配置先と次に使うスキル）を表示する。

## 実行コマンド
```bash
python3 .agents/skills/client-register/register_client.py
```

## Template contract (must preserve)
1. `clients/TEMPLATE/outputs/runs/` exists.
2. `clients/TEMPLATE/artifacts/cache/` exists.
3. `clients/TEMPLATE/artifacts/ingest/` exists.
4. `clients/TEMPLATE/artifacts/telemetry/` exists.
5. Use `.gitkeep` files as needed to keep empty directories in git.
