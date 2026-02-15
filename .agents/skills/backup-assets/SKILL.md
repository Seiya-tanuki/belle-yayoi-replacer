---
name: backup-assets
description: Backup field assets (clients and lexicon/pending) into exports/backups with MANIFEST.json. Explicit invocation only.
---

# backup-assets

Creates a fixed-scope asset backup zip from runtime field assets.

## Scope (fixed)
- `clients/**`
- `lexicon/pending/**`
- `MANIFEST.json` at zip root

## Output
- `exports/backups/assets_<UTC_TS>_<SHA8>.zip`
- `exports/backups/LATEST.txt`

## Notes
- This skill is explicit invocation only.
- Acquires the global `label_queue` lock before reading `lexicon/pending/`.

## Execution
```bash
python .agents/skills/backup-assets/scripts/backup_assets.py
```
