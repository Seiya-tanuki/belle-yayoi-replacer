# FILE_LAYOUT (repo contract)

This repository is designed for deterministic, shell-driven operation via Codex Agent Skills.
All runtime data must be isolated by client and line.

## Canonical line IDs (current)

1. `receipt` (implemented)
2. `bank_statement` (implemented)
3. `credit_card_statement` (implemented)

## Client directory layout (canonical)

```text
clients/<CLIENT_ID>/
  config/
    yayoi_tax_config.json             # live shared tax postprocess config for all implemented lines
  artifacts/
    client_registration/              # shared new-client registration audit (onboarding provenance only)
      runs/
        <RUN_ID>/
          run_manifest.json
      LATEST.txt
  lines/
    <line_id>/
      config/
        category_overrides.json        # receipt + credit_card_statement only; shared override rows use target_account / target_tax_division
        bank_line_config.json          # bank_statement only
        credit_card_line_config.json   # credit_card_statement only; target_payable_placeholder_names + teacher_extraction.canonical_payable_thresholds live here
      inputs/
        kari_shiwake/                  # target draft CSV for the selected line (all implemented lines)
        ledger_ref/                    # receipt + credit_card_statement only
        training/                      # bank_statement only
          ocr_kari_shiwake/
          reference_yayoi/
      outputs/
        runs/
          <RUN_ID>/
            *_replaced_<RUN_ID>.csv
            *_review_report.csv
            *_manifest.json
            run_manifest.json
        LATEST.txt
      artifacts/
        cache/
          client_cache.json
        derived/                       # credit_card_statement only; managed derived teacher artifacts
          cc_teacher_manifest.json
          cc_teacher/
            <RAW_SHA256>__cc_teacher.csv
        ingest/
          ledger_ref/                  # receipt + credit_card_statement only
          kari_shiwake/                # all implemented lines
          training_ocr/                # bank_statement only
          training_reference/          # bank_statement only
          ledger_ref_ingested.json     # receipt + credit_card_statement only
          kari_shiwake_ingested.json   # all implemented lines
          training_ocr_ingested.json   # bank_statement only
          training_reference_ingested.json
        telemetry/                     # optional runtime logs; non-blocking if absent
          lexicon_autogrow_latest.json # receipt only
          *.json
```

## bank_statement additions (implemented)

`bank_statement` is implemented and uses line-scoped paths only (no legacy fallback).
The following paths are used by the bank cache builder/replacer flow:

```text
clients/<CLIENT_ID>/lines/bank_statement/
  inputs/
    kari_shiwake/
    training/
      ocr_kari_shiwake/
      reference_yayoi/
  outputs/
    runs/<RUN_ID>/
    LATEST.txt
  artifacts/
    ingest/
      training_ocr/
      training_reference/
      training_ocr_ingested.json
      training_reference_ingested.json
    cache/
      client_cache.json                # schema differs: belle.bank_client_cache.v0
    telemetry/                         # optional; allowed and non-blocking
```

Related specs:
1. `spec/BANK_LINE_INPUTS_SPEC.md`
2. `spec/BANK_CLIENT_CACHE_SPEC.md`
3. `spec/BANK_REPLACER_SPEC.md`

## Line-specific source policy

1. `receipt` uses `inputs/ledger_ref/` and `artifacts/ingest/ledger_ref*/` for incremental ingest/cache updates.
   1. Lexicon category routing is enabled (primary).
   2. Pending queue/autogrow path is `lexicon/receipt/pending/`.
2. `bank_statement` MUST NOT use any `ledger_ref` path; it uses only:
   1. `inputs/training/ocr_kari_shiwake/`
   2. `inputs/training/reference_yayoi/`
   3. `inputs/kari_shiwake/`
   4. `artifacts/ingest/training_ocr/` + `training_ocr_ingested.json`
   5. `artifacts/ingest/training_reference/` + `training_reference_ingested.json`
   6. Lexicon category routing is not wired.
3. `credit_card_statement` uses line-scoped inputs:
   1. `inputs/kari_shiwake/` (target; `0 => SKIP`, `1 => RUN`, `2+ => FAIL`)
   2. `inputs/ledger_ref/` (append-only raw historical teacher input)
   3. `artifacts/derived/cc_teacher/<RAW_SHA256>__cc_teacher.csv` stores derived teacher rows extracted from each raw `ledger_ref` source.
   4. `artifacts/derived/cc_teacher_manifest.json` stores deterministic raw-to-derived provenance plus cache-application state.
   4. Contract A is required (one statement per target file).
   5. Runtime may strict-stop with exit `2` after artifacts are written when `payable_sub_fill_required_failed == true` or `canonical_payable_required_failed == true`.
   6. Lexicon category routing is fallback-only (secondary to merchant-key routing).
   7. Per-client overrides path is `clients/<CLIENT_ID>/lines/credit_card_statement/config/category_overrides.json`.

## bank_statement forbidden paths (explicit)

The following paths are forbidden for `line_id=bank_statement` and must not be used as data sources:
1. `clients/<CLIENT_ID>/lines/bank_statement/inputs/ledger_ref/**`
2. `clients/<CLIENT_ID>/lines/bank_statement/artifacts/ingest/ledger_ref/**`

## Shared assets (tracked)

1. `lexicon/lexicon.json`
2. `defaults/receipt/category_defaults_tax_excluded.json`
3. `defaults/receipt/category_defaults_tax_included.json`
4. `defaults/credit_card_statement/category_defaults_tax_excluded.json`
5. `defaults/credit_card_statement/category_defaults_tax_included.json`
6. `rulesets/receipt/replacer_config_v1_15.json`
7. `rulesets/credit_card_statement/teacher_extraction_rules_v1.json`
8. `clients/TEMPLATE/config/yayoi_tax_config.json`
9. `lexicon/receipt/pending/.gitkeep`
10. `lexicon/receipt/pending/locks/.gitkeep`

## Shared client config

1. `clients/<CLIENT_ID>/config/yayoi_tax_config.json` is the shared client config path for Yayoi tax postprocess.
2. The shared tax postprocess is wired into `receipt`, `bank_statement`, and `credit_card_statement`.
3. `clients/TEMPLATE/config/yayoi_tax_config.json` is tracked as the staged template baseline and is validated as part of template integrity.
4. New-client registration rewrites the staged client copy to match the operator-selected `bookkeeping_mode`.
5. The tracked template currently sets `enabled: true` and `bookkeeping_mode: tax_excluded`, but this is not the final contract for newly registered clients.
6. The shared target-side override contract for `receipt` and `credit_card_statement` is `target_account` / `target_tax_division`.
7. `receipt` / `credit_card_statement` tracked defaults are dual assets selected by `bookkeeping_mode`.
8. `clients/<CLIENT_ID>/artifacts/client_registration/` is the shared audit location for successful new-client registration runs.
9. `clients/<CLIENT_ID>/artifacts/client_registration/runs/<RUN_ID>/run_manifest.json` stores onboarding provenance only; it is not a runtime replacer artifact.
10. `clients/<CLIENT_ID>/artifacts/client_registration/LATEST.txt` points to the latest successful registration audit run.

## Runtime-managed assets (ignored)

1. `clients/**` except `clients/TEMPLATE/**`
2. `lexicon/receipt/pending/**` except placeholders:
   1. `lexicon/receipt/pending/.gitkeep`
   2. `lexicon/receipt/pending/locks/.gitkeep`
3. `exports/**`

## Legacy compatibility (receipt only, deprecated)

1. Receipt scripts may read/write legacy client layout:
   1. `clients/<CLIENT_ID>/config/`
   2. `clients/<CLIENT_ID>/inputs/`
   3. `clients/<CLIENT_ID>/outputs/`
   4. `clients/<CLIENT_ID>/artifacts/`
2. Non-receipt lines must never fall back to legacy layout.
3. Shared assets do not use legacy global paths in the current rollout.
4. Phase 2 provides an explicit safe migration utility:
   1. `python .agents/skills/migrate-line-layout/scripts/migrate_line_layout.py --client <ID|ALL> --dry-run true --line receipt`
   2. Real migration requires `--apply --dry-run false`
5. Legacy shared pending path `lexicon/pending/` may be migrated to `lexicon/receipt/pending/` via the same utility.

## Ingest marker extension (`ledger_ref_ingested.json`)

Each `ingested[sha256]` entry may include:
1. `stored_name` and `stored_relpath` (relative path from effective client root)
2. `processed_to_label_queue_at` (ISO-8601 UTC)
3. `processed_to_label_queue_run_id` (optional)
4. `processed_to_label_queue_version` (optional)

These fields are append-only markers used to guarantee idempotent label queue growth.
