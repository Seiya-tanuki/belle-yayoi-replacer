---
name: yayoi-replacer
description: Replace ONLY the debit account (借方勘定科目, col 5) in Yayoi 25-col import CSV using lexicon + client_cache + effective defaults overlay. Explicit invocation only.
---

# yayoi-replacer

Deterministic debit-account replacement for Yayoi import CSVs.

## Preconditions
1. Work under a single client folder: `clients/<CLIENT_ID>/`
2. Put input CSV(s) in: `clients/<CLIENT_ID>/inputs/kari_shiwake/`
3. (Recommended) Put historical finalized CSV(s) in: `clients/<CLIENT_ID>/inputs/ledger_ref/` (append-only batches)

## What this skill does
1. Loads `lexicon/lexicon.json` (offline dictionary).
2. Loads global defaults: `defaults/category_defaults.json`.
3. Loads per-client overrides: `clients/<CLIENT_ID>/config/category_overrides.json`.
4. Builds effective defaults by overlay merge (override only `debit_account`).
5. **Updates client_cache cache**:
   1. Ingests `inputs/ledger_ref/*.csv` (sha256 + in-place rename)
   2. Applies only not-yet-applied batches into `artifacts/cache/client_cache.json` (append-only growth)
6. Replaces **only** column 5 (借方勘定科目). No other columns are modified.
7. Writes:
   1. creates `RUN_ID` and run folder: `clients/<CLIENT_ID>/outputs/runs/<RUN_ID>/`
   2. writes replaced CSV + per-file manifest JSON + review report CSV into that run folder
   3. writes batch run manifest JSON as `run_manifest.json` into that run folder
   4. updates `clients/<CLIENT_ID>/outputs/LATEST.txt` with the latest `RUN_ID`
8. Artifacts are system-managed only:
   1. `clients/<CLIENT_ID>/artifacts/cache/*`
   2. `clients/<CLIENT_ID>/artifacts/ingest/*`
   3. `clients/<CLIENT_ID>/artifacts/telemetry/*`
   4. users should not manually edit artifacts files

## User-editable defaults
1. Edit only `clients/<CLIENT_ID>/config/category_overrides.json`.
2. Edit only `overrides.<category_key>.debit_account` values.
3. Do not edit key structure/schema.

## Canonical specs (read-only)
1. `spec/REPLACER_SPEC.md`
2. `spec/CATEGORY_OVERRIDES_SPEC.md`
3. `spec/CLIENT_CACHE_SPEC.md`
4. `spec/LEXICON_SPEC.md`
5. `spec/CATEGORY_DEFAULTS_SPEC.md`

## Execution
```bash
python3 .agents/skills/yayoi-replacer/scripts/run_yayoi_replacer.py --client <CLIENT_ID>
```
