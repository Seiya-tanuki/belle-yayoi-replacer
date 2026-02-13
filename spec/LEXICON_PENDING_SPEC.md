# LEXICON_PENDING_SPEC (label_queue)

## Purpose
`lexicon/pending/label_queue.csv` is the **single global queue** of unknown (unlabeled) terms that were observed
in clients' historical ledgers but are not yet covered by `lexicon/lexicon.json`.

This queue is designed for:
1. long-tail collection (cumulative counts)
2. periodic human labeling (assign a category)
3. deterministic application into lexicon.json (append-only learned term_rows)

## Files
1. `lexicon/pending/label_queue.csv`
   1. user edits: set `user_category_key` and `action=ADD`
   2. everything else should be treated as read-only
2. `lexicon/pending/label_queue_state.json`
   1. internal state (client sets per norm_key)
   2. do not edit manually
3. `lexicon/pending/applied_log.jsonl`
   1. append-only audit of applied rows

## Queue CSV schema (stable)
Columns:
1. `norm_key` (string): normalize_n0(term). This is the unique key.
2. `raw_example` (string): example original string (human hint).
3. `example_summary` (string): example 摘要 for context.
4. `count_total` (int): cumulative occurrence count (across runs and clients).
5. `clients_seen` (int): number of distinct clients that observed this key.
6. `first_seen_at` (ISO-8601 UTC)
7. `last_seen_at` (ISO-8601 UTC)
8. `suggested_category_key` (string): optional best-effort hint (do not trust blindly).
9. `user_category_key` (string): REQUIRED for action=ADD.
10. `action` (string): HOLD | ADD
11. `notes` (string): freeform

## Workflow (two skills)
1. `$lexicon-extract`
   1. ingests `clients/<CLIENT_ID>/inputs/ledger_train/*.csv` (append-only batches)
   2. updates label_queue.csv cumulatively
2. `$lexicon-apply`
   1. reads label_queue.csv
   2. applies only action=ADD rows into lexicon.json as learned term_rows
   3. removes applied rows from queue

## Safety rules
1. NEVER fetch external data. Everything must work offline.
2. Application into lexicon.json is append-only:
   1. terms are added as learned rows (weight < core weight).
   2. existing core rows must not be removed by automated steps.


