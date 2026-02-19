---
name: migrate-line-layout
description: Safely migrate legacy receipt client layout and legacy lexicon/pending into line-scoped paths with dry-run by default.
---

# migrate-line-layout

Migrates deprecated receipt-era runtime layout into canonical line-scoped paths.

## Scope
1. Client layout:
   1. `clients/<CLIENT_ID>/{config,inputs,outputs,artifacts}`
   2. `clients/<CLIENT_ID>/lines/receipt/{config,inputs,outputs,artifacts}`
2. Legacy pending:
   1. `lexicon/pending/*`
   2. `lexicon/receipt/pending/*`

## Safety
1. Dry-run is default (`--dry-run true`).
2. Real changes require both `--apply` and `--dry-run false`.
3. Fail-closed on overwrite risk (no merge/override mode in Phase 2).
4. `label_queue.lock` is never migrated.
5. `--line` supports `receipt` only in Phase 2.

## Execution examples
```bash
python .agents/skills/migrate-line-layout/scripts/migrate_line_layout.py --client ALL --dry-run true --line receipt
```

```bash
python .agents/skills/migrate-line-layout/scripts/migrate_line_layout.py --client C001 --mode copy --apply --dry-run false --line receipt
```

```bash
python .agents/skills/migrate-line-layout/scripts/migrate_line_layout.py --migrate-pending --dry-run true --line receipt
```
