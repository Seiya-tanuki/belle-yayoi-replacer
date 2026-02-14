---
name: lexicon-extract
description: Extract unknown terms from clients/<CLIENT_ID>/inputs/ledger_train and update lexicon/pending/label_queue.csv (cumulative). Explicit invocation only.
---

# lexicon-extract

Updates the global pending label queue (`lexicon/pending/label_queue.csv`) by scanning per-client training ledgers.

## Inputs
- `clients/<CLIENT_ID>/inputs/ledger_train/*.csv` or `*.txt` (append-only batches)

## Outputs
- `lexicon/pending/label_queue.csv` (cumulative; do NOT delete manually)
- `lexicon/pending/label_queue_state.json` (internal state; do NOT edit)
- `clients/<CLIENT_ID>/artifacts/ledger_train_ingested.json` (ingest manifest)
- `clients/<CLIENT_ID>/artifacts/reports/lexicon_extract_run_<TS>.json`

## Execution
```bash
python3 .agents/skills/lexicon-extract/scripts/run_lexicon_extract.py --client <CLIENT_ID>
```

