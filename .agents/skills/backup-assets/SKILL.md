---
name: backup-assets
description: Backup field assets (clients and lexicon/<line_id>/pending) into exports/backups with MANIFEST.json. Explicit invocation only.
---

# backup-assets

Creates a fixed-scope asset backup zip from runtime field assets.

## Scope (fixed)
- `receipt`: `clients/**` + `lexicon/receipt/pending/**`
- `bank_statement` (and future `credit_card_statement` safety path): `clients/**` only
- `MANIFEST.json` at zip root

## Output
- `exports/backups/assets_<UTC_TS>_<SHA8>.zip`
- `exports/backups/LATEST.txt`

## Notes
- This skill is explicit invocation only.
- `receipt` のみ `label_queue` lock を取得して `lexicon/receipt/pending/` を読み取る。
- `bank_statement` は lock を取得せず、lexicon pending を扱わない。

## Execution
```bash
python .agents/skills/backup-assets/scripts/backup_assets.py --line receipt
```

```bash
python .agents/skills/backup-assets/scripts/backup_assets.py --line bank_statement
```
