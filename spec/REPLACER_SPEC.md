# REPLACER_SPEC (belle.replacer.v2)

## Goal
Replace ONLY the debit account field (借方勘定科目, 5th column) in Yayoi 25-column import CSV.
Never change any other field. Preserve formatting as much as possible (byte-identical pass-through).

## Input/Output contract
1. CSV has **exactly 25 columns** per line.
2. Default encoding is CP932 (Shift-JIS family). The replacer must preserve the original encoding.
3. Line ending must remain CRLF when present in the input.
4. `clients/<CLIENT_ID>/inputs/kari_shiwake/` is fail-closed:
   1. 0 files -> error and exit non-zero before creating `outputs/runs/<RUN_ID>/`
   2. 2+ files -> error and exit non-zero before creating `outputs/runs/<RUN_ID>/`
   3. exactly 1 file -> ingest first, then replacement
5. Kari-shiwake ingest (pre-run):
   1. compute sha256
   2. move+rename to `clients/<CLIENT_ID>/artifacts/ingest/kari_shiwake/INGESTED_<UTC_TS>_<SHA8>.csv`
   3. append/update `clients/<CLIENT_ID>/artifacts/ingest/kari_shiwake_ingested.json`
      (`schema: belle.kari_shiwake_ingest.v1`, dedupe by sha256, duplicate is renamed to `IGNORED_DUPLICATE_<UTC_TS>_<SHA8>.csv`)
   4. replacer reads the ingested file path as the actual input

## Allowed inference fields
1. Only 摘要 (17th column) may be used for inference.
2. Debit account (5th) is read-only for 'before' value and replaced in output.
3. Memo (22nd) must not be used.

## Defaults overlay (runtime)
1. Load global defaults from `defaults/category_defaults.json`.
2. Load per-client overrides from `clients/<CLIENT_ID>/config/category_overrides.json`.
3. Build `effective_defaults = merge(global_defaults, client_overrides)`:
   1. Override only `debit_account` per `category_key`.
   2. Keep global `confidence`, `priority`, and `reason_code` unchanged.
4. If overrides file is missing, generate a full-expanded file and continue.
5. If overrides file exists but is invalid (JSON/schema/keys/value), fail-closed and exit non-zero before creating `outputs/runs/<RUN_ID>/`.

## Deterministic decision order (strong -> weak)
For each row, compute suggestion in this order:

1. Dummy row:
   1. if summary == '##DUMMY_OCR_UNREADABLE##' (exact match)
   2. keep original debit account, mark priority HIGH, confidence 0.0

2. T-number × category route (client evidence, gated):
   1. extract `T\d{13}` from summary
   2. match summary to a lexicon category
   3. if client_cache has stats for this (T, category) and meets thresholds -> use top_account

3. T-number route (client evidence, gated):
   1. extract `T\d{13}` from summary
   2. if client_cache has stats for this T-number and meets thresholds -> use top_account

4. vendor_key route (client evidence, gated):
   1. extract vendor_key from summary (splitters + legal-form stripping)
   2. if client_cache has stats and meets thresholds -> use top_account

5. category route (client evidence, gated):
   1. match summary to lexicon category
   2. if client_cache has category stats and meets thresholds -> use top_account

6. category default route:
   1. if a category matched but client evidence is weak/missing -> use effective_defaults[category_key]

7. global fallback:
   1. use effective_defaults.global_fallback

## Notes on accuracy vs coverage
The system is optimized for high replacement coverage. It is acceptable to output low-confidence
suggestions as long as:
1. the output remains import-safe
2. low-confidence suggestions are clearly marked for review (review_report.csv)

## Outputs
1. Create one run directory per execution: `clients/<CLIENT_ID>/outputs/runs/<RUN_ID>/`
2. Replaced CSV(s) under that run directory
3. Per-file manifest JSON(s) under that run directory
4. Review report CSV(s) under that run directory
5. Batch run manifest as `run_manifest.json` under that run directory
6. Update `clients/<CLIENT_ID>/outputs/LATEST.txt` with the latest `RUN_ID`
7. `run_manifest.json` must include:
   1. `inputs.kari_shiwake.original_name`
   2. `inputs.kari_shiwake.stored_name`
   3. `inputs.kari_shiwake.sha256`


