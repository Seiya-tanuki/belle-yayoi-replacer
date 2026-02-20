---
name: restore-assets
description: Restore field assets (clients and lexicon/<line_id>/pending) from a backup ZIP with validation and safety gates. Explicit invocation only.
---

# restore-assets

Restores fixed-scope runtime field assets from a backup zip.

## Scope (fixed)
- `receipt`: restores `clients/**` and `lexicon/receipt/pending/**`
- `bank_statement` (and future `credit_card_statement` safety path): restores `clients/**` only
- Never restores tracked code directories/files

## Arguments
- `--zip <path>`: backup ZIP path (required)
- `--line <line_id>`: default `receipt` (current implementation supports `receipt` / `bank_statement`)
- `--force`: required when destination assets already contain data

## Safety gates
- Validates zip structure and `MANIFEST.json` schema/hash integrity before apply.
- Creates pre-restore safety snapshot under `exports/backups/` before overwrite.
- `bank_statement` は lexicon pending を要求せず、clients スコープのみで検証・復元する。

## Execution
```bash
python .agents/skills/restore-assets/scripts/restore_assets.py --zip <path_to_backup_zip> --line receipt [--force]
```

```bash
python .agents/skills/restore-assets/scripts/restore_assets.py --zip <path_to_backup_zip> --line bank_statement [--force]
```
