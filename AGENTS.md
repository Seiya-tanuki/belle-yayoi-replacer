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
   2. `$yayoi-replacer`: run line-aware replacement (`receipt`: debit account + debit-side tax division, with shared tax postprocess tax-amount fill when configured/applicable; `bank_statement`: see BANK_REPLACER_SPEC; `credit_card_statement`: see CREDIT_CARD_REPLACER_SPEC).
   3. `$client-cache-builder`: ingest line-specific learning inputs and incrementally update `client_cache`.
   4. `$lexicon-extract`: extract unknown terms from finalized ledger data and grow `label_queue.csv`.
   5. `$lexicon-apply`: apply only `ADD` rows from `label_queue.csv` to `lexicon.json`.
   6. `$export-lexicon-review-pack`: acquire the global label_queue lock and export a fixed review ZIP + MANIFEST for Lexicon Steward GPTs under `exports/gpts_lexicon_review/`.
   7. `$backup-assets`: backup field assets (`clients/` + `lexicon/<line_id>/pending/`) into `exports/backups/` with MANIFEST.
   8. `$restore-assets`: restore field assets (`clients/` + `lexicon/<line_id>/pending/`) from a backup ZIP with force/safety gates.
   9. `$system-diagnose`: run comprehensive environment/system readiness diagnostics and export a Markdown report under `exports/system_diagnose/`.
   10. `$collect-outputs`: クライアント横断で run 成果物（置換CSV・レビューCSV・manifest）を収集し、`exports/collect/` に単一ZIPを出力。
   11. `$migrate-line-layout`: receipt 旧レイアウト資産を line-aware 配置へ安全に移行する。
4. Current runtime behavior:
   1. `receipt`, `bank_statement`, and `credit_card_statement` are implemented/runnable via explicit skill invocation.
   2. `receipt` flow is `ledger_ref`-based (`inputs/ledger_ref` + `artifacts/ingest/ledger_ref*`).
   3. `bank_statement` flow uses training + target only (`inputs/training/*` + `inputs/kari_shiwake`) and bank cache artifacts.
   4. `bank_statement` must not use `inputs/ledger_ref/**` or `artifacts/ingest/ledger_ref/**`.
   5. `credit_card_statement` requires Contract A (one statement per target file) and may strict-stop with exit `2` after writing artifacts when `payable_sub_fill_required_failed == true`.

## 2) Data Placement (Per Client, No Mix-ups)
1. Canonical per-client path is `clients/<CLIENT_ID>/lines/<line_id>/`.
2. Receipt legacy compatibility (deprecated): `clients/<CLIENT_ID>/...` is still accepted when `--line receipt`.
3. Inputs:
   1. `inputs/kari_shiwake/`: target draft Yayoi CSV(s) for replacement (receipt/bank_statement/credit_card_statement).
   2. `inputs/ledger_ref/`: historical finalized CSV(s) used by `receipt` and `credit_card_statement` cache/statistics flow.
   3. `inputs/training/ocr_kari_shiwake/`: bank_statement training OCR inputs.
   4. `inputs/training/reference_yayoi/`: bank_statement training teacher reference inputs.
4. User-facing outputs:
   1. `outputs/runs/<RUN_ID>/*`: artifacts for one execution.
   2. `outputs/LATEST.txt`: latest `RUN_ID`.
5. System-managed artifacts:
   1. `artifacts/cache/client_cache.json`: append-only client cache.
   2. `artifacts/ingest/ledger_ref_ingested.json`: receipt ledger_ref ingest manifest.
   3. `artifacts/ingest/training_ocr_ingested.json`: bank_statement training OCR ingest manifest.
   4. `artifacts/ingest/training_reference_ingested.json`: bank_statement training reference ingest manifest.
   5. `artifacts/telemetry/*`: optional internal logs/metrics (allowed, non-blocking).
6. Principles:
   1. Users should be able to fetch one full run from `outputs/runs/<RUN_ID>/`.
   2. `artifacts/*` is system-owned; users should not manually edit it.

## 3) Critical Safety Constraints (Do Not Break Yayoi Import)
1. Yayoi import CSV must be treated as fixed 25 columns.
2. For `receipt`, `$yayoi-replacer` may change:
   1. debit account (column 5)
   2. debit-side tax division (column 8)
   3. shared tax postprocess may subsequently fill tax amount fields when configured/applicable
3. For `bank_statement`, replacement target fields are defined by `spec/BANK_REPLACER_SPEC.md` and must stay within that contract.
4. All non-target columns must remain unchanged.
5. Inference-source constraints are line-aware:
   1. `receipt`: inference uses summary (column 17) only; memo (column 22) is not an inference source.
   2. `credit_card_statement`: inference uses summary (column 17) only; memo (column 22) is not an inference source.
   3. `bank_statement`: memo (column 22) may be used only for the `SIGN` fallback described by `spec/BANK_REPLACER_SPEC.md`.
7. `##DUMMY_OCR_UNREADABLE##` must be treated as a dummy row: do not replace it and raise review priority.
8. Yayoi CSV read/write must remain cp932-compatible.

## 4) Network Access
1. This project is intended to run without external web access.
2. Decisions must be deterministic from local data (`lexicon/lexicon.json`, `clients/<CLIENT_ID>/.../artifacts/*`, and local inputs).

## 5) Source Files and Cache
1. Shared lexicon source of truth: `lexicon/lexicon.json` (core + learned).
2. Lexicon line usage:
   1. `receipt`: primary category inference source.
   2. `credit_card_statement`: category fallback only (secondary to merchant-key routing).
   3. `bank_statement`: lexicon category routing is not wired.
3. Pending queue (receipt-only, occasionally edited by users): `lexicon/receipt/pending/label_queue.csv`.
4. Default account mappings:
   1. `defaults/receipt/category_defaults_tax_excluded.json`
   2. `defaults/receipt/category_defaults_tax_included.json`
   3. `defaults/credit_card_statement/category_defaults_tax_excluded.json`
   4. `defaults/credit_card_statement/category_defaults_tax_included.json`
5. category_overrides (best-effort): `clients/<CLIENT_ID>/lines/<line_id>/config/category_overrides.json` for `receipt` and `credit_card_statement`.
6. Client delta cache: `clients/<CLIENT_ID>/lines/<line_id>/artifacts/cache/client_cache.json`.
   1. `client_cache` is append-only incremental state (no destructive rebuild by default).
   2. receipt ledger_ref ingest/application state is tracked by `artifacts/ingest/ledger_ref_ingested.json` and `client_cache.applied_ledger_ref_sha256`.
   3. bank_statement ingest/application state is tracked by `artifacts/ingest/training_ocr_ingested.json`, `artifacts/ingest/training_reference_ingested.json`, and bank cache metadata.

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

## 8) Developer Validation
1. Canonical validation command from the repository root:
   1. `python tools/run_tests.py`
2. This repo-owned entrypoint is the default for humans and Codex.
   1. It must be preferred over shell-specific `PYTHONPATH` setup.
3. Optional legacy direct command from the repository root only:
   1. `python -m unittest discover -s tests -v`
