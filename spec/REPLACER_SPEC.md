# REPLACER_SPEC (belle.replacer.v2)

## Goal
Replace ONLY the debit account field (借方勘定科目, 5th column) in Yayoi 25-column import CSV.
Never change any other field. Preserve formatting as much as possible (byte-identical pass-through).

## Input/Output contract
1. CSV has **exactly 25 columns** per line.
2. Default encoding is CP932 (Shift-JIS family). The replacer must preserve the original encoding.
3. Line ending must remain CRLF when present in the input.

## Allowed inference fields
1. Only 摘要 (17th column) may be used for inference.
2. Debit account (5th) is read-only for 'before' value and replaced in output.
3. Memo (22nd) must not be used.

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
   1. if a category matched but client evidence is weak/missing -> use defaults[category_key]

7. global fallback:
   1. use defaults.global_fallback

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


