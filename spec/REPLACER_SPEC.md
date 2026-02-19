# REPLACER_SPEC (belle.replacer.v2)

## Goal

Replace ONLY the debit account field (5th column) in Yayoi 25-column import CSV.
Never change any other field. Preserve formatting behavior and CSV contract.

## Line implementation status (Phase 1)

1. `receipt`: implemented
2. `bank_statement`: unimplemented (must fail-closed)
3. `credit_card_statement`: unimplemented (must fail-closed)

## Input/Output contract

1. CSV has exactly 25 columns per line.
2. Default encoding is CP932 (Shift-JIS family). The replacer must preserve the original encoding.
3. Line ending must remain CRLF behavior-compatible with current parser/writer.
4. Canonical input path:
   1. `clients/<CLIENT_ID>/lines/<line_id>/inputs/kari_shiwake/`
5. Fail-closed input count:
   1. 0 files -> error and exit non-zero before creating `outputs/runs/<RUN_ID>/`
   2. 2+ files -> error and exit non-zero before creating `outputs/runs/<RUN_ID>/`
   3. exactly 1 file -> ingest first, then replacement
6. Kari-shiwake ingest (pre-run):
   1. compute sha256
   2. move+rename to `.../artifacts/ingest/kari_shiwake/INGESTED_<UTC_TS>_<SHA8>.csv`
   3. append/update `.../artifacts/ingest/kari_shiwake_ingested.json`
      (`schema: belle.kari_shiwake_ingest.v1`, dedupe by sha256, duplicate renamed to `IGNORED_DUPLICATE_<UTC_TS>_<SHA8>.csv`)
   4. replacer reads the ingested file path as the actual input

## Allowed inference fields

1. Only summary (17th column) may be used for inference.
2. Debit account (5th) is read-only for before value and replaced in output.
3. Memo (22nd) must not be used.

## Defaults overlay (runtime)

1. Load global defaults from `defaults/<line_id>/category_defaults.json`.
2. Load per-client overrides from `clients/<CLIENT_ID>/lines/<line_id>/config/category_overrides.json`.
3. Build `effective_defaults = merge(global_defaults, client_overrides)`:
   1. Override only `debit_account` per `category_key`.
   2. Keep global `confidence`, `priority`, and `reason_code` unchanged.
4. If overrides file is missing, generate a full-expanded file and continue.
5. If overrides file exists but is invalid, fail-closed and exit non-zero before creating `outputs/runs/<RUN_ID>/`.

## Deterministic decision order (strong -> weak)

For each row, compute suggestion in this order:

1. Dummy row:
   1. if summary == `##DUMMY_OCR_UNREADABLE##` (exact match)
   2. keep original debit account, mark priority HIGH, confidence 0.0
2. T-number × category route (client evidence, gated)
3. T-number route (client evidence, gated)
4. vendor_key route (client evidence, gated)
5. category route (client evidence, gated)
6. category default route
7. global fallback

## Outputs

1. Create one run directory per execution: `clients/<CLIENT_ID>/lines/<line_id>/outputs/runs/<RUN_ID>/`
2. Replaced CSV(s) under that run directory
3. Per-file manifest JSON(s) under that run directory
4. Review report CSV(s) under that run directory
5. Batch run manifest as `run_manifest.json` under that run directory
6. Update `clients/<CLIENT_ID>/lines/<line_id>/outputs/LATEST.txt` with latest `RUN_ID`
7. `run_manifest.json` must include:
   1. `inputs.kari_shiwake.original_name`
   2. `inputs.kari_shiwake.stored_name`
   3. `inputs.kari_shiwake.sha256`

## Legacy compatibility (receipt only, deprecated)

1. If `clients/<CLIENT_ID>/lines/receipt/` is absent, receipt scripts may use legacy paths:
   1. `clients/<CLIENT_ID>/inputs/*`
   2. `clients/<CLIENT_ID>/outputs/*`
   3. `clients/<CLIENT_ID>/artifacts/*`
2. Non-receipt lines must never use legacy fallback.
