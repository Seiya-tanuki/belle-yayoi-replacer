# CLIENT_CACHE_SPEC (belle.client_cache.v1)

## Purpose
`clients/<CLIENT_ID>/artifacts/client_cache.json` is a per-client **append-only cache** learned from
historical finalized journals (`inputs/ledger_ref/`).

It provides empirical debit-account distributions keyed by:
1. **T-number** (`T\d{13}`) extracted from 摘要 (17th column)
2. **T-number × category** (category inferred from lexicon using 摘要)
3. **vendor_key** extracted from 摘要 (splitters + legal-form stripping)
4. **category** inferred from lexicon using 摘要
5. **global** debit-account distribution (all rows)

The replacer uses client_cache only when evidence is strong enough (gated by thresholds); otherwise it falls back to
`defaults/category_defaults.json`.

## Append-only cache semantics (critical)
1. client_cache is **not** rebuilt from scratch during normal operation.
2. Updates are applied by ingesting new ledger_ref batches and incrementing counts.
3. Previously observed evidence must not disappear in an update.

This supports stable replacement coverage across time.

## Ingestion and deduplication
Each client has an ingest manifest:
- `clients/<CLIENT_ID>/artifacts/ledger_ref_ingested.json`

Behavior:
1. All `inputs/ledger_ref/*.csv` files are hashed (sha256).
2. New content is renamed in-place to: `INGESTED_<UTC_TS>_<SHA8>.csv`
3. Duplicate content is renamed to: `IGNORED_DUPLICATE_<UTC_TS>_<SHA8>.csv` and ignored.
4. client_cache tracks which sha256 batches have already been applied via:
   - `client_cache.applied_ledger_ref_sha256`

## Schema (high level)
Top-level keys:
1. `schema`: `belle.client_cache.v1`
2. `version`
3. `client_id`
4. `created_at`, `updated_at`
5. `append_only`: bool
6. `applied_ledger_ref_sha256`: `{ sha256 -> { applied_at, stored_name, rows_total, rows_used } }`
7. `decision_thresholds`: copy of thresholds for audit
8. `stats`: distribution maps

### stats maps
Each stats entry stores:
- `sample_total`
- `top_account`
- `top_count`
- `p_majority`
- `debit_account_counts` (kept to support future explainability / audits)

Maps:
1. `t_numbers`: `{ "T123...": StatsEntry }`
2. `t_numbers_by_category`: `{ "T123...": { "<CATEGORY_KEY>": StatsEntry } }`
3. `vendor_keys`: `{ "<VENDOR_KEY>": StatsEntry }`
4. `categories`: `{ "<CATEGORY_KEY>": StatsEntry }`
5. `global`: StatsEntry

## Important invariants
1. Only 摘要 (17th column) and 借方勘定科目 (5th column) are used.
2. 仕訳メモ (22th column) MUST NOT be used.
3. Dummy summaries (`##DUMMY_OCR_UNREADABLE##`) are excluded from stats.


