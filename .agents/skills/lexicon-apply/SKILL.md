---
name: lexicon-apply
description: Apply user-labeled ADD rows from lexicon/receipt/pending/label_queue.csv into lexicon/lexicon.json and remove them from the queue. Receipt-only. Explicit invocation only.
---

# lexicon-apply

Applies `action=ADD` rows from the pending label queue into the canonical lexicon.

## Line support
1. Receipt-only skill.
2. If `--line != receipt`, the script exits with code `2` and an explicit receipt-only error.

## Inputs
- `lexicon/receipt/pending/label_queue.csv` (user edits: set `user_category_key` and `action=ADD`)

## Outputs
- `lexicon/lexicon.json` (appends learned term_rows; rebuilds buckets)
- `lexicon/receipt/pending/label_queue.csv` (removes applied rows)
- `lexicon/receipt/pending/label_queue_state.json` (removes applied keys)
- `lexicon/receipt/pending/applied_log.jsonl` (append-only audit)
- `lexicon/receipt/pending/locks/label_queue.lock` (same global lock used by extraction)
- `lexicon/receipt/pending/apply_run_<TS>.json`

## Execution
```bash
python .agents/skills/lexicon-apply/scripts/run_lexicon_apply.py --line receipt
```
