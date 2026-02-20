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
4. Receipt preferred line layout:
   - `clients/<CLIENT_ID>/lines/receipt/`
5. Receipt legacy fallback (deprecated, auto-detected if line layout missing):
   - `clients/<CLIENT_ID>/`
6. For `receipt`: put exactly one target CSV in `.../inputs/kari_shiwake/`.
7. For `receipt`: put historical reference CSV/TXT files in `.../inputs/ledger_ref/`.
8. For `bank_statement`: use line-scoped path only (no legacy fallback):
   - `clients/<CLIENT_ID>/lines/bank_statement/`
9. For `bank_statement`: place exactly one target CSV in:
   - `clients/<CLIENT_ID>/lines/bank_statement/inputs/kari_shiwake/`
10. For `bank_statement` training/cache update:
   - OCR training: `.../inputs/training/ocr_kari_shiwake/`
   - Teacher reference: `.../inputs/training/reference_yayoi/`
11. `bank_statement` v0 teacher rule:
   - exactly one ingested teacher reference file is required at run time
   - one canonical file under `inputs/training/reference_yayoi/` is recommended

## Runtime behavior (important)
1. For `receipt`, `ledger_ref` ingest treats `inputs/ledger_ref/` as an inbox.
2. On successful ingest, files are moved to:
   - `.../artifacts/ingest/ledger_ref/INGESTED_<UTC_TS>_<SHA8>.csv`
3. Duplicate sha files are moved to:
   - `.../artifacts/ingest/ledger_ref/IGNORED_DUPLICATE_<UTC_TS>_<SHA8>.csv`
4. `inputs/ledger_ref/` is expected to be empty after successful ingest (except placeholders like `.gitkeep`).
5. Downstream processing reads ingested file paths from `artifacts/ingest/ledger_ref_ingested.json`.

## What this skill does
1. For `receipt`, loads `lexicon/receipt/lexicon.json`.
2. For `receipt`, loads defaults + per-client overrides and builds effective defaults.
3. For `receipt`, updates `client_cache` (append-only) and auto-grows pending lexicon candidates from ingested `ledger_ref`.
4. Ingests the single kari_shiwake input to `artifacts/ingest/kari_shiwake/`.
5. Replaces only column 5 and writes outputs to `.../outputs/runs/<RUN_ID>/`.
6. For `bank_statement`, does not load receipt lexicon/defaults; it updates bank cache and runs the bank replacer using training + cache assets.
7. Writes line-scoped run artifacts and updates `outputs/LATEST.txt` under the selected line.
8. `credit_card_statement` remains fail-closed.

## Canonical specs
1. `spec/REPLACER_SPEC.md`
2. `spec/CATEGORY_OVERRIDES_SPEC.md`
3. `spec/CLIENT_CACHE_SPEC.md`
4. `spec/LEXICON_PENDING_SPEC.md`

## Execution
```bash
python .agents/skills/yayoi-replacer/scripts/run_yayoi_replacer.py --client <CLIENT_ID> --line receipt
```

```bash
python .agents/skills/yayoi-replacer/scripts/run_yayoi_replacer.py --client <CLIENT_ID> --line bank_statement
```
