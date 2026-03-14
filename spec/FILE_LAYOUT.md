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
  config/                              # optional future shared config (can be empty)
  lines/
    <line_id>/
      config/
        category_overrides.json        # receipt + credit_card_statement only
        bank_line_config.json          # bank_statement only
        credit_card_line_config.json   # credit_card_statement only
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
   2. `inputs/ledger_ref/` (append-only historical teacher input)
   3. Contract A is required (one statement per target file).
   4. Runtime may strict-stop with exit `2` after artifacts are written when `payable_sub_fill_required_failed == true`.
   5. Lexicon category routing is fallback-only (secondary to merchant-key routing).
   6. Per-client overrides path is `clients/<CLIENT_ID>/lines/credit_card_statement/config/category_overrides.json`.

## bank_statement forbidden paths (explicit)

The following paths are forbidden for `line_id=bank_statement` and must not be used as data sources:
1. `clients/<CLIENT_ID>/lines/bank_statement/inputs/ledger_ref/**`
2. `clients/<CLIENT_ID>/lines/bank_statement/artifacts/ingest/ledger_ref/**`

## Shared assets (tracked)

1. `lexicon/lexicon.json`
2. `defaults/receipt/category_defaults.json`
3. `defaults/credit_card_statement/category_defaults.json`
4. `rulesets/receipt/replacer_config_v1_15.json`
5. `lexicon/receipt/pending/.gitkeep`
6. `lexicon/receipt/pending/locks/.gitkeep`

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
3. Shared assets do not use legacy global paths in Phase 1.
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
