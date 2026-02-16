---
name: lexicon-extract
description: Extract unknown terms from ledger_ref and grow lexicon/pending/label_queue.csv. Explicit invocation only.
---

# lexicon-extract

Updates `lexicon/pending/label_queue.csv` by scanning per-client ledger_ref history.

## Inputs
1. Put new `ledger_ref` CSV/TXT files in `clients/<CLIENT_ID>/inputs/ledger_ref/`.

## Outputs
1. `lexicon/pending/label_queue.csv`
2. `lexicon/pending/label_queue_state.json`
3. `lexicon/pending/locks/label_queue.lock`
4. `clients/<CLIENT_ID>/artifacts/ingest/ledger_ref_ingested.json`
5. `clients/<CLIENT_ID>/artifacts/telemetry/lexicon_autogrow_latest.json`

## Ingest behavior
1. `inputs/ledger_ref/` is an ingest inbox.
2. Successful ingest moves files into `clients/<CLIENT_ID>/artifacts/ingest/ledger_ref/`.
3. Autogrow reads stored file paths from `ledger_ref_ingested.json` entries.

## Execution
```bash
python .agents/skills/lexicon-extract/scripts/run_lexicon_extract.py --client <CLIENT_ID>
```
