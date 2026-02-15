# AGENTS.md (Belle / Yayoi suite)

## 0) Chat Language
1. All chat responses to the user must be in Japanese.
2. Output artifact file contents (JSON/CSV, etc.) may use English for stability.
   1. Exceptions: lexicon keywords, account names, and Yayoi CSV values remain Japanese when required.

## 1) Operating Assumptions (Most Important)
1. This repository is designed around Codex Agent Skills.
2. Explicit skill invocation policy:
   1. When the user wants to run a system function, the user explicitly invokes the corresponding skill with `$skill`.
   2. Do not implicitly invoke skills on your own.
   3. Only run a skill when the user explicitly requests it.
3. Available Skills (split by responsibility):
   1. `$client-register`: clone `clients/TEMPLATE/` into `clients/<CLIENT_ID>/` using a safe client name.
   2. `$yayoi-replacer`: replace only debit account (column 5) in draft journal CSVs.
   3. `$client-cache-builder`: ingest `ledger_ref` and incrementally update `client_cache`.
   4. `$lexicon-extract`: extract unknown terms from finalized ledger data and grow `label_queue.csv`.
   5. `$lexicon-apply`: apply only `ADD` rows from `label_queue.csv` to `lexicon.json`.
   6. `$export-lexicon-review-pack`: acquire the global label_queue lock and export a fixed review ZIP + MANIFEST for Lexicon Steward GPTs under `exports/gpts_lexicon_review/`.
   7. `$backup-assets`: backup field assets (`clients/` + `lexicon/pending/`) into `exports/backups/` with MANIFEST.
   8. `$restore-assets`: restore field assets (`clients/` + `lexicon/pending/`) from a backup ZIP with force/safety gates.
   9. `$system-diagnose`: run comprehensive environment/system readiness diagnostics and export a Markdown report under `exports/system_diagnose/`.
4. Current runtime behavior:
   1. The pipeline is ledger_ref-only.
   2. `$yayoi-replacer` includes client_cache incremental update and lexicon candidate autogrow from `ledger_ref` before replacement.

## 2) Data Placement (Per Client, No Mix-ups)
1. All per-client inputs/outputs live under `clients/<CLIENT_ID>/`.
2. Inputs:
   1. `inputs/kari_shiwake/`: target draft Yayoi CSV(s) for replacement.
   2. `inputs/ledger_ref/`: historical finalized CSV(s) used for cache and statistics.
3. User-facing outputs:
   1. `outputs/runs/<RUN_ID>/*`: artifacts for one execution.
   2. `outputs/LATEST.txt`: latest `RUN_ID`.
4. System-managed artifacts:
   1. `artifacts/cache/client_cache.json`: append-only client cache.
   2. `artifacts/ingest/ledger_ref_ingested.json`: ledger_ref ingest manifest.
   3. `artifacts/telemetry/*`: internal logs/metrics.
5. Principles:
   1. Users should be able to fetch one full run from `outputs/runs/<RUN_ID>/`.
   2. `artifacts/*` is system-owned; users should not manually edit it.

## 3) Critical Safety Constraints (Do Not Break Yayoi Import)
1. Yayoi import CSV must be treated as fixed 25 columns.
2. `$yayoi-replacer` may change only debit account (column 5).
3. All other columns (summary/tax/memo/etc.) must remain unchanged (byte-identical target).
4. Inference may use only summary (column 17).
5. Memo (column 22) must not be used for inference.
6. `##DUMMY_OCR_UNREADABLE##` must be treated as a dummy row: do not replace it and raise review priority.
7. Yayoi CSV read/write must remain cp932-compatible.

## 4) Network Access
1. This project is intended to run without external web access.
2. Decisions must be deterministic from local data (`lexicon/lexicon.json`, `clients/<CLIENT_ID>/artifacts/*`, and local inputs).

## 5) Source Files and Cache
1. Shared lexicon source of truth: `lexicon/lexicon.json` (core + learned).
2. Pending queue (occasionally edited by users): `lexicon/pending/label_queue.csv`.
3. Default account mappings: `defaults/category_defaults.json`.
4. Client delta cache: `clients/<CLIENT_ID>/artifacts/cache/client_cache.json`.
   1. `client_cache` is append-only incremental state (no destructive rebuild by default).
   2. ledger_ref ingest/application state is tracked by `artifacts/ingest/ledger_ref_ingested.json` and `client_cache.applied_ledger_ref_sha256`.

## 6) spec/ Is the Source of Truth
1. For schemas, update rules, and replacement order, always follow `spec/`.
2. Keep skill docs (`SKILL.md`) concise and defer strict behavior to `spec/`.

## 7) BOM Troubleshooting (Minimal)
1. If Skills are skipped or `SKILL.md` is reported invalid, check UTF-8 BOM:
   1. `python tools/bom_guard.py --check`
2. Remove BOM if needed:
   1. `python tools/bom_guard.py --fix`
3. After clone, enable pre-commit hook:
   1. `git config core.hooksPath .githooks`
