---
name: lexicon-apply
description: Apply user-labeled ADD rows from lexicon/pending/label_queue.csv into lexicon/lexicon.json and remove them from the queue. Explicit invocation only.
---

# lexicon-apply

Applies `action=ADD` rows from the pending label queue into the canonical lexicon.

## Inputs
- `lexicon/pending/label_queue.csv` (user edits: set `user_category_key` and `action=ADD`)

## Outputs
- `lexicon/lexicon.json` (appends learned term_rows; rebuilds buckets)
- `lexicon/pending/label_queue.csv` (removes applied rows)
- `lexicon/pending/label_queue_state.json` (removes applied keys)
- `lexicon/pending/applied_log.jsonl` (append-only audit)
- `lexicon/pending/locks/label_queue.lock` (same global lock used by extraction)
- `lexicon/pending/apply_run_<TS>.json`

## Execution
```bash
python3 .agents/skills/lexicon-apply/scripts/run_lexicon_apply.py
```
