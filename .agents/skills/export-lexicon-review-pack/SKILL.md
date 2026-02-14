---
name: export-lexicon-review-pack
description: Export a fixed Lexicon Steward GPTs review pack zip from repository root data and specs. Acquires global label_queue lock before reading lexicon/pending files. Explicit invocation only.
---

# export-lexicon-review-pack

Exports a fixed read-only snapshot zip for Lexicon Steward GPTs.

## Output
- `exports/gpts_lexicon_review/lexicon_review_pack_<UTC_TS>_<SHA8>.zip`
- `exports/gpts_lexicon_review/LATEST.txt`

## Notes
- File selection is fixed. No interactive selection.
- If required files are missing or lock acquisition fails, exit non-zero without creating a zip.

## Execution
```bash
python3 .agents/skills/export-lexicon-review-pack/scripts/export_pack.py
```
