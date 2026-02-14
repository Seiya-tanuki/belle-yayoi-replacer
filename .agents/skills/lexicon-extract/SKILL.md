---
name: lexicon-extract
description: Extract unknown terms from clients/<CLIENT_ID>/inputs/ledger_ref and grow lexicon/pending/label_queue.csv (cumulative). Explicit invocation only.
---

# lexicon-extract

Updates the global pending label queue (`lexicon/pending/label_queue.csv`) by scanning per-client reference ledgers.

## Inputs
- `clients/<CLIENT_ID>/inputs/ledger_ref/*.csv` or `*.txt` (append-only batches)

## Outputs
- `lexicon/pending/label_queue.csv` (cumulative; do NOT delete manually)
- `lexicon/pending/label_queue_state.json` (internal state; do NOT edit)
- `lexicon/pending/locks/label_queue.lock` (global lock file for queue/state mutation)
- `clients/<CLIENT_ID>/artifacts/ingest/ledger_ref_ingested.json` (ingest manifest with processed markers)
- `clients/<CLIENT_ID>/artifacts/telemetry/lexicon_autogrow_latest.json` (latest internal run summary)

## Artifact policy
- `artifacts/*` is system-managed. Do not edit manually.

## Execution
```bash
python3 .agents/skills/lexicon-extract/scripts/run_lexicon_extract.py --client <CLIENT_ID>
```
