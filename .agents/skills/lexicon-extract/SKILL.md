---
name: lexicon-extract
description: Extract unknown terms from ledger_ref and grow lexicon/receipt/pending/label_queue.csv. Receipt-only. Explicit invocation only.
---

# lexicon-extract

Updates `lexicon/receipt/pending/label_queue.csv` by scanning per-client ledger_ref history.

## Line support
1. Receipt-only skill.
2. If `--line != receipt`, the script exits with code `2` and an explicit receipt-only error.

## Inputs
1. Receipt line layout:
   - `clients/<CLIENT_ID>/lines/receipt/inputs/ledger_ref/`

## Outputs
1. `lexicon/receipt/pending/label_queue.csv`
2. `lexicon/receipt/pending/label_queue_state.json`
3. `lexicon/receipt/pending/locks/label_queue.lock`
4. `clients/<CLIENT_ID>/lines/receipt/artifacts/ingest/ledger_ref_ingested.json`
5. `clients/<CLIENT_ID>/lines/receipt/artifacts/telemetry/lexicon_autogrow_latest.json`

## Ingest behavior
1. `inputs/ledger_ref/` is an ingest inbox.
2. Successful ingest moves files into `clients/<CLIENT_ID>/lines/receipt/artifacts/ingest/ledger_ref/`.
3. Autogrow reads stored file paths from `ledger_ref_ingested.json` entries.

## Execution
```bash
python .agents/skills/lexicon-extract/scripts/run_lexicon_extract.py --client <CLIENT_ID> --line receipt
```
