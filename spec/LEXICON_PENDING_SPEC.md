# LEXICON_PENDING_SPEC (label_queue)

## Purpose

`lexicon/<line_id>/pending/label_queue.csv` is the per-line queue of unknown terms that were observed in client ledgers but are not yet covered by `lexicon/lexicon.json`.

This queue supports:
1. Cumulative unknown-term collection (append-only counts)
2. Human labeling (`action=ADD`)
3. Deterministic application into `lexicon/lexicon.json`

## Files (receipt in Phase 1)

1. `lexicon/receipt/pending/label_queue.csv`
2. `lexicon/receipt/pending/label_queue_state.json`
3. `lexicon/receipt/pending/applied_log.jsonl`
4. `lexicon/receipt/pending/locks/label_queue.lock`

`label_queue.lock` must be shared by:
1. integrated autogrow in `$yayoi-replacer`
2. manual `$lexicon-extract`
3. `$lexicon-apply`

## Queue CSV schema (stable)

Columns:
1. `norm_key` (string): normalize_n0(term), unique key
2. `raw_example` (string): example original string
3. `example_summary` (string): example summary
4. `count_total` (int): cumulative occurrence count
5. `clients_seen` (int): number of distinct clients
6. `first_seen_at` (ISO-8601 UTC)
7. `last_seen_at` (ISO-8601 UTC)
8. `suggested_category_key` (string)
9. `user_category_key` (string): required for `action=ADD`
10. `action` (string): `HOLD` or `ADD`
11. `notes` (string)

## Active workflow

1. `$yayoi-replacer`
   1. Updates `client_cache` from `clients/<CLIENT_ID>/.../inputs/ledger_ref/*`
   2. Auto-runs strict lexicon candidate extraction from the same `ledger_ref` source
   3. Writes/updates:
      1. `lexicon/receipt/pending/label_queue.csv`
      2. `lexicon/receipt/pending/label_queue_state.json`
      3. `clients/<CLIENT_ID>/.../artifacts/telemetry/lexicon_autogrow_latest.json`
      4. `clients/<CLIENT_ID>/.../artifacts/ingest/ledger_ref_ingested.json` markers
2. `$lexicon-extract` (manual)
   1. Runs the same `ledger_ref`-based autogrow logic on demand
3. `$lexicon-apply`
   1. Applies only `action=ADD` rows into `lexicon/lexicon.json`
   2. Removes applied rows from queue/state

## Strict autogrow filter contract

Source:
1. Use summary column (17th column) only.
2. Memo column (22nd column) must never be used.

Include condition:
1. Process only rows whose summary has no lexicon category hit at summary level.

Exclude:
1. Dummy summary: `##DUMMY_OCR_UNREADABLE##`
2. `T\d{13}` tokens
3. Long numeric IDs (`>= 6` digits)
4. Date-like tokens
5. Phone-like tokens
6. Very short tokens (less than 3 chars after normalization)
7. Broad verbs configured by implementation
8. Terms already known by lexicon matching

## Idempotency contract

Per `clients/<CLIENT_ID>/.../artifacts/ingest/ledger_ref_ingested.json` entry:
1. `processed_to_label_queue_at` is set only after queue/state write succeeds.
2. Optional metadata:
   1. `processed_to_label_queue_run_id`
   2. `processed_to_label_queue_version`

Only entries missing `processed_to_label_queue_at` may be processed.

## Fail-closed contract

For integrated autogrow in `$yayoi-replacer`:
1. Lock timeout / write error / manifest corruption must fail the run.
2. `outputs/runs/<RUN_ID>/` must not be created when autogrow fails.
3. `outputs/LATEST.txt` must not be updated when autogrow fails.

## Legacy compatibility (receipt only, deprecated)

1. Legacy client layout may still be used for client-side ingest/cache paths.
2. Pending queue path is line-scoped (`lexicon/receipt/pending/*`) in Phase 1.
