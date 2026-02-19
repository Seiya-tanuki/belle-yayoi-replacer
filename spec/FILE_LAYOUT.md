# FILE_LAYOUT (repo contract)

This repository is designed for deterministic, shell-driven operation via Codex Agent Skills.
All runtime data must be isolated by client and line.

## Canonical line IDs (Phase 1)

1. `receipt` (implemented)
2. `bank_statement` (UNIMPLEMENTED in Phase 1; fail-closed)
3. `credit_card_statement` (UNIMPLEMENTED in Phase 1; fail-closed)

## Client directory layout (canonical)

```text
clients/<CLIENT_ID>/
  config/                              # optional future shared config (can be empty)
  lines/
    <line_id>/
      config/
        category_overrides.json        # per-client+line editable full-expanded overrides
      inputs/
        kari_shiwake/                  # target draft Yayoi CSV (receipt line only in Phase 1)
        ledger_ref/                    # historical finalized CSV/TXT inbox
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
          ledger_ref/
          kari_shiwake/
          ledger_ref_ingested.json
          kari_shiwake_ingested.json
        telemetry/
          lexicon_autogrow_latest.json
          *.json
```

## Shared assets (line-scoped, tracked)

1. `lexicon/<line_id>/lexicon.json`
2. `defaults/<line_id>/category_defaults.json`
3. `rulesets/<line_id>/replacer_config_v1_15.json`
4. `lexicon/<line_id>/pending/.gitkeep`
5. `lexicon/<line_id>/pending/locks/.gitkeep`

## Runtime-managed assets (ignored)

1. `clients/**` except `clients/TEMPLATE/**`
2. `lexicon/*/pending/**` except placeholders:
   1. `lexicon/*/pending/.gitkeep`
   2. `lexicon/*/pending/locks/.gitkeep`
3. `exports/**`

## Legacy compatibility (receipt only, deprecated)

1. Receipt scripts may read/write legacy client layout:
   1. `clients/<CLIENT_ID>/config/`
   2. `clients/<CLIENT_ID>/inputs/`
   3. `clients/<CLIENT_ID>/outputs/`
   4. `clients/<CLIENT_ID>/artifacts/`
2. Non-receipt lines must never fall back to legacy layout.
3. Shared assets do not use legacy global paths in Phase 1.

## Ingest marker extension (`ledger_ref_ingested.json`)

Each `ingested[sha256]` entry may include:
1. `stored_name` and `stored_relpath` (relative path from effective client root)
2. `processed_to_label_queue_at` (ISO-8601 UTC)
3. `processed_to_label_queue_run_id` (optional)
4. `processed_to_label_queue_version` (optional)

These fields are append-only markers used to guarantee idempotent label queue growth.
