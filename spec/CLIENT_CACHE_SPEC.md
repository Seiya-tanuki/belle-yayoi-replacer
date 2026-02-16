# CLIENT_CACHE_SPEC (belle.client_cache.v1)

## Purpose
`clients/<CLIENT_ID>/artifacts/cache/client_cache.json` is a per-client **append-only cache** learned from
historical finalized journals (`ledger_ref`).

It provides empirical debit-account distributions keyed by:
1. **T-number** (`T\d{13}`) extracted from summary (17th column)
2. **T-number x category** (category inferred from lexicon using summary)
3. **vendor_key** extracted from summary (splitters + legal-form stripping)
4. **category** inferred from lexicon using summary
5. **global** debit-account distribution (all rows)

The replacer uses client_cache only when evidence is strong enough (gated by thresholds); otherwise it falls back to
`defaults/category_defaults.json`.

## Append-only cache semantics (critical)
1. client_cache is **not** rebuilt from scratch during normal operation.
2. Updates are applied by ingesting new ledger_ref batches and incrementing counts.
3. Previously observed evidence must not disappear in an update.

## Ingestion and deduplication
Each client has an ingest manifest:
- `clients/<CLIENT_ID>/artifacts/ingest/ledger_ref_ingested.json`

Behavior:
1. New source files are placed in `clients/<CLIENT_ID>/inputs/ledger_ref/` (inbox).
2. All `*.csv` and `*.txt` files in that inbox are hashed (sha256).
3. New content is moved+renamed to:
   - `clients/<CLIENT_ID>/artifacts/ingest/ledger_ref/INGESTED_<UTC_TS>_<SHA8>.csv`
4. Duplicate content is moved+renamed to:
   - `clients/<CLIENT_ID>/artifacts/ingest/ledger_ref/IGNORED_DUPLICATE_<UTC_TS>_<SHA8>.csv`
5. Manifest `ingested[sha256]` records `stored_name` and `stored_relpath` (relative to `clients/<CLIENT_ID>/`).
6. `client_cache` tracks already-applied batches via `client_cache.applied_ledger_ref_sha256`.

After successful ingest, `inputs/ledger_ref/` should be empty (except placeholders such as `.gitkeep`).

## Schema (high level)
Top-level keys:
1. `schema`: `belle.client_cache.v1`
2. `version`
3. `client_id`
4. `created_at`, `updated_at`
5. `append_only`: bool
6. `applied_ledger_ref_sha256`: `{ sha256 -> { applied_at, stored_name, stored_relpath, rows_total, rows_used } }`
7. `decision_thresholds`: copy of thresholds for audit
8. `stats`: distribution maps

### stats maps
Each stats entry stores:
- `sample_total`
- `top_account`
- `top_count`
- `p_majority`
- `debit_account_counts` (kept to support explainability/audit)

Maps:
1. `t_numbers`: `{ "T123...": StatsEntry }`
2. `t_numbers_by_category`: `{ "T123...": { "<CATEGORY_KEY>": StatsEntry } }`
3. `vendor_keys`: `{ "<VENDOR_KEY>": StatsEntry }`
4. `categories`: `{ "<CATEGORY_KEY>": StatsEntry }`
5. `global`: StatsEntry

## Important invariants
1. Only summary (17th column) and debit account (5th column) are used.
2. Memo (22nd column) MUST NOT be used.
3. Dummy summaries (`##DUMMY_OCR_UNREADABLE##`) are excluded from stats.
