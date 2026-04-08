# LEXICON_SPEC (belle.lexicon.v1)

## Purpose
`lexicon/lexicon.json` is the **single canonical dictionary** used to map free-text
(Yayoi summary text) to a **category**. The current taxonomy is the reconstructed
69-category operational/posting taxonomy. Category labels are routing buckets for deterministic
replacement, not a strict ontology. Categories are later mapped to debit accounts via:
1) per-client+line `clients/<CLIENT_ID>/lines/<line_id>/artifacts/cache/client_cache.json` (learned from historical journals), and
2) global `defaults/<line_id>/category_defaults.json` (fallback).

This lexicon must be usable **offline** (no network).

## Line usage contract (Phase D)
1. `receipt` uses lexicon category inference as primary routing.
2. `credit_card_statement` uses lexicon for category fallback only (secondary to merchant-key routing).
3. `bank_statement` does not use lexicon category routing.
4. `label_queue.csv` workflow is receipt-only and is specified in `spec/LEXICON_PENDING_SPEC.md`.

## Core design choice
The category label (e.g. `CONVENIENCE_STORE`) is **not** used to "generate" vendor names.
Instead, lexicon contains an explicit **term table** (`term_rows`) that maps
normalized terms -> category IDs.

## Schema (high level)

Top-level keys (stable):
- `schema`: string (e.g. `belle.lexicon.v1`)
- `version`: string
- `created_at`: ISO-8601
- `normalization`: definitions for `n0` and `n1`
- `matching_contract`: matching strategy contract
- `categories`: array of category objects
- `term_rows`: flat array of term rows
- `term_buckets_prefix2`: optional speed index
- `learned`: metadata for learned-term tracking (optional)

### Category object
Current asset fields:
- `id`: int (stable numeric ID)
- `key`: short string key (stable across versions if possible)
- `label`: upper snake-case label (display / reporting)
- `label_ja`: Japanese display label
- `kind`: one of `merchant|platform|payment|government|utility|...` (for analytics only)
- `precision_hint`: float (0..1)
- `deprecated`: bool
- `negative_terms`: dict with keys `n0` and `n1` (array of needles) used as negative filters
- `default_rule`: supporting default mapping metadata carried in the canonical asset
- `source_ref`: supporting provenance / reconstruction metadata

**Important:** categories do NOT embed their own keyword lists.
Keywords live in `term_rows`.

Runtime-essential matching/routing fields are `id`, `key`, `label`, `negative_terms`, and
`precision_hint`. `label_ja`, `kind`, `deprecated`, `default_rule`, and `source_ref` are
supporting metadata used for display, defaults generation, audit, or authoring support.

### `default_rule` supporting metadata
`default_rule` mirrors the shared target-side fallback contract used by
`defaults/<line_id>/category_defaults.json`:
- `target_account`: non-empty string
- `target_tax_division`: string, may be empty
- `confidence`: float
- `priority`: `"HIGH"|"MED"|"LOW"`
- `reason_code`: string

Phase A updates this metadata shape only. Lexicon matching behavior, category IDs/keys, and term rows remain unchanged.

### term_rows (explicit keyword table)
Each row is:
`[field, needle, category_id, weight, type]`

- `field`: `"n0"` or `"n1"`
- `needle`: normalized string in the corresponding field space
- `category_id`: int (must exist in categories)
- `weight`: float (1.0 for core, <1.0 for learned by convention)
- `type`: `"S"` = substring match

## Normalization
Two normalization fields are supported:

### n0 (aggressive)
- Unicode NFKC
- Uppercase Latin letters
- Drop characters in Unicode categories: `Z*`, `P*`, `S*`, `C*`
Result: produces a compact alnum-ish string (keeps Japanese letters).

### n1 (conservative)
- Unicode NFKC
- Uppercase Latin letters
- Collapse whitespace into a single space
- Drop control chars
- Trim

## Matching strategy (deterministic)
- For each input summary, compute `n0` and `n1`.
- Check each term row of the same field: **substring** (`needle in normalized_text`).
- Apply category negative filter: if any negative needle for that category appears, ignore the category.
- Score per category:
  `score = max_over_matched_terms(weight * (len(needle)/12.0))`
- Pick the highest-scoring category (ties broken by longer needle, then higher `precision_hint`).
- Record match quality:
  - `none`: no term matched
  - `ambiguous`: top-2 scores ratio <= 1.05
  - `clear`: otherwise

## Learned terms
Learned terms are appended into `term_rows` with a **weight < 1.0**
(e.g. `0.85`) so downstream components can treat them as lower-confidence signals.
User labeling is required for many learned terms.

## External references
Any URLs / external reference sources MUST NOT be required for runtime.
They are stored separately in `spec/LEXICON_SOURCES.md` (documentation only).
Those references are supplementary seed/sanity-check support only; category assignment source of
truth is always the local `lexicon/lexicon.json`.
