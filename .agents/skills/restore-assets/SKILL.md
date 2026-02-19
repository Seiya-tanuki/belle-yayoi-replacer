---
name: restore-assets
description: Restore field assets (clients and lexicon/<line_id>/pending) from a backup ZIP with validation and safety gates. Explicit invocation only.
---

# restore-assets

Restores fixed-scope runtime field assets from a backup zip.

## Scope (fixed)
- Restores only `clients/**` and `lexicon/receipt/pending/**`
- Never restores tracked code directories/files

## Arguments
- `--zip <path>`: backup ZIP path (required)
- `--line <line_id>`: default `receipt` (Phase 1 supports receipt only)
- `--force`: required when destination assets already contain data

## Safety gates
- Validates zip structure and `MANIFEST.json` schema/hash integrity before apply.
- Creates pre-restore safety snapshot under `exports/backups/` before overwrite.

## Execution
```bash
python .agents/skills/restore-assets/scripts/restore_assets.py --zip <path_to_backup_zip> --line receipt [--force]
```
