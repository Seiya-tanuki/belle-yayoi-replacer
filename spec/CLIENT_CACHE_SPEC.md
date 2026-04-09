# CLIENT_CACHE_SPEC (receipt line client_cache, belle.client_cache.v2)

## Scope

This spec applies to receipt line cache only (`line_id=receipt`).
For bank-statement and credit-card line cache behavior, see:
1. `spec/BANK_CLIENT_CACHE_SPEC.md`
2. `spec/CREDIT_CARD_CLIENT_CACHE_SPEC.md`

## Purpose

`clients/<CLIENT_ID>/lines/<line_id>/artifacts/cache/client_cache.json` is a per-client, per-line append-only cache learned from historical finalized journals (`ledger_ref`).
In this spec, learning source is `ledger_ref` only.
Repository baseline does not track this file; runtime creates it from client state when needed and
then extends it append-only.

It provides empirical debit-account distributions keyed by:
1. **T-number** (`T\d{13}`) extracted from summary (17th column)
2. **T-number x category** (category inferred from lexicon using summary)
3. **vendor_key** extracted from summary (splitters + legal-form stripping)
4. **category** inferred from lexicon using summary
5. **global** debit-account distribution (all rows)

It also provides receipt debit-side tax-division distributions conditioned on the chosen debit account:
1. `t_number + category + target_account`
2. `t_number + target_account`
3. `vendor_key + target_account`
4. `category + target_account`
5. `global + target_account`

The replacer uses client_cache only when evidence is strong enough (gated by thresholds); otherwise it falls back to `defaults/<line_id>/category_defaults.json`.

## Append-only cache semantics (critical)

1. client_cache is **not** rebuilt from scratch during normal operation.
2. Updates are applied by ingesting new ledger_ref batches and incrementing counts.
3. Previously observed evidence must not disappear in an update.

## Ingestion and deduplication

Each client+line has an ingest manifest:
1. `clients/<CLIENT_ID>/lines/<line_id>/artifacts/ingest/ledger_ref_ingested.json`

Behavior:
1. New source files are placed in `clients/<CLIENT_ID>/lines/<line_id>/inputs/ledger_ref/`.
2. All `*.csv` and `*.txt` files in that inbox are hashed (sha256).
3. New content is moved+renamed to:
   1. `.../artifacts/ingest/ledger_ref/INGESTED_<UTC_TS>_<SHA8>.csv`
4. Duplicate content is moved+renamed to:
   1. `.../artifacts/ingest/ledger_ref/IGNORED_DUPLICATE_<UTC_TS>_<SHA8>.csv`
5. Manifest `ingested[sha256]` records `stored_name` and `stored_relpath` (relative to effective client root).
6. `client_cache` tracks already-applied batches via `client_cache.applied_ledger_ref_sha256`.

No bank-training sources (`training/ocr_kari_shiwake`, `training/reference_yayoi`) are part of this receipt spec.

After successful ingest, `inputs/ledger_ref/` should be empty (except placeholders such as `.gitkeep`).

## Schema (high level)

Top-level keys:
1. `schema`: `belle.client_cache.v2`
2. `version`
3. `client_id`
4. `created_at`, `updated_at`
5. `append_only`: bool
6. `applied_ledger_ref_sha256`: `{ sha256 -> { applied_at, stored_name, stored_relpath, rows_total, rows_used } }`
7. `decision_thresholds`: copy of thresholds for audit
8. `stats`: debit-account distribution maps
9. `tax_stats`: debit-side tax-division distribution maps conditioned on debit account

### stats maps

Each stats entry stores:
1. `sample_total`
2. `top_account`
3. `top_count`
4. `p_majority`
5. `debit_account_counts` (kept to support explainability/audit)

Maps:
1. `t_numbers`: `{ "T123...": StatsEntry }`
2. `t_numbers_by_category`: `{ "T123...": { "<CATEGORY_KEY>": StatsEntry } }`
3. `vendor_keys`: `{ "<VENDOR_KEY>": StatsEntry }`
4. `categories`: `{ "<CATEGORY_KEY>": StatsEntry }`
5. `global`: StatsEntry

### tax_stats maps

Each tax stats entry stores:
1. `sample_total`
2. `top_tax_division`
3. `top_count`
4. `p_majority`
5. `tax_division_counts`

Maps:
1. `t_numbers_by_category_and_account`: `{ "T123...": { "<CATEGORY_KEY>": { "<DEBIT_ACCOUNT>": TaxStatsEntry } } }`
2. `t_numbers_by_account`: `{ "T123...": { "<DEBIT_ACCOUNT>": TaxStatsEntry } }`
3. `vendor_keys_by_account`: `{ "<VENDOR_KEY>": { "<DEBIT_ACCOUNT>": TaxStatsEntry } }`
4. `categories_by_account`: `{ "<CATEGORY_KEY>": { "<DEBIT_ACCOUNT>": TaxStatsEntry } }`
5. `global_by_account`: `{ "<DEBIT_ACCOUNT>": TaxStatsEntry }`

## Important invariants

1. Account learning uses only summary (17th column) and debit account (5th column).
2. Tax learning uses only summary (17th column), debit account (5th column), and debit tax division (8th column).
3. Tax learning is skipped when summary is blank/dummy, debit account is blank, or debit tax division is blank.
4. Memo (22nd column) MUST NOT be used.
5. Dummy summaries (`##DUMMY_OCR_UNREADABLE##`) are excluded from stats.

## Compatibility note

This phase does not provide backward-compatibility or migration support for older receipt client_cache schema versions.

## Legacy compatibility (receipt only, deprecated)

1. If `clients/<CLIENT_ID>/lines/receipt/` is absent, receipt scripts may use:
   1. `clients/<CLIENT_ID>/artifacts/cache/client_cache.json`
   2. `clients/<CLIENT_ID>/artifacts/ingest/*`
2. Non-receipt lines must never use legacy fallback.
