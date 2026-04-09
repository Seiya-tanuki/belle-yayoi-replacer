---
name: export-lexicon-review-pack
description: Export a fixed Lexicon Steward GPTs review pack zip from repository root data and specs. Acquires global label_queue lock before reading lexicon/receipt/pending files. Receipt-only. Explicit invocation only.
---

# export-lexicon-review-pack

Exports a fixed read-only snapshot zip for Lexicon Steward GPTs.

## Line support
1. Receipt-only skill.
2. If `--line != receipt`, the script exits with code `2` and an explicit receipt-only error.

## Output
- `exports/gpts_lexicon_review/lexicon_review_pack_<UTC_TS>_<SHA8>.zip`
- `exports/gpts_lexicon_review/LATEST.txt`

## Notes
- File selection is fixed. No interactive selection.
- Receipt exports include the tracked defaults assets returned by `tracked_category_defaults_relpaths("receipt")`, so the pack contains both `category_defaults_tax_excluded.json` and `category_defaults_tax_included.json`.
- If required files are missing or lock acquisition fails, exit non-zero without creating a zip.

## Execution
```bash
python .agents/skills/export-lexicon-review-pack/scripts/export_pack.py --line receipt
```
