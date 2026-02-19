---
name: yayoi-replacer
description: Replace ONLY debit account (col 5) in Yayoi 25-col CSV using lexicon + client_cache + defaults. Explicit invocation only.
---

# yayoi-replacer

Deterministic debit-account replacement for Yayoi import CSVs.

## Preconditions
1. `--client <CLIENT_ID>` is required.
2. `--line <line_id>` is available (default: `receipt`).
3. Work under a single client folder.
4. Preferred line layout:
   - `clients/<CLIENT_ID>/lines/receipt/`
5. Receipt legacy fallback (deprecated, auto-detected if line layout missing):
   - `clients/<CLIENT_ID>/`
6. Put exactly one target CSV in `.../inputs/kari_shiwake/`.
7. Put historical reference CSV/TXT files in `.../inputs/ledger_ref/`.

## Runtime behavior (important)
1. `ledger_ref` ingest treats `inputs/ledger_ref/` as an inbox.
2. On successful ingest, files are moved to:
   - `.../artifacts/ingest/ledger_ref/INGESTED_<UTC_TS>_<SHA8>.csv`
3. Duplicate sha files are moved to:
   - `.../artifacts/ingest/ledger_ref/IGNORED_DUPLICATE_<UTC_TS>_<SHA8>.csv`
4. `inputs/ledger_ref/` is expected to be empty after successful ingest (except placeholders like `.gitkeep`).
5. Downstream processing reads ingested file paths from `artifacts/ingest/ledger_ref_ingested.json`.

## What this skill does
1. Loads `lexicon/receipt/lexicon.json`.
2. Loads defaults + per-client overrides and builds effective defaults.
3. Updates `client_cache` (append-only).
4. Auto-grows pending lexicon candidates from unprocessed ingested ledger_ref entries.
5. Ingests the single kari_shiwake input to `artifacts/ingest/kari_shiwake/`.
6. Replaces only column 5 and writes outputs to `.../outputs/runs/<RUN_ID>/`.
7. `receipt` only in Phase 1 (`bank_statement` and `credit_card_statement` are fail-closed).

## Canonical specs
1. `spec/REPLACER_SPEC.md`
2. `spec/CATEGORY_OVERRIDES_SPEC.md`
3. `spec/CLIENT_CACHE_SPEC.md`
4. `spec/LEXICON_PENDING_SPEC.md`

## Execution
```bash
python .agents/skills/yayoi-replacer/scripts/run_yayoi_replacer.py --client <CLIENT_ID> --line receipt
```
