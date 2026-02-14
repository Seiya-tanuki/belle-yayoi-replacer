# FILE_LAYOUT (repo contract)

This repository is designed for deterministic, shell-driven operation via Codex Agent Skills.
All client data is isolated under `clients/<CLIENT_ID>/` to prevent cross-client mixups.

## Client directory layout

```text
clients/<CLIENT_ID>/
  config/
    category_overrides.json         # per-client editable full-expanded overrides
  inputs/
    kari_shiwake/                   # Yayoi 25-col CSV to be replaced (target)
    ledger_ref/                     # Historical finalized journal CSVs (append-only batches)
  outputs/
    runs/
      <RUN_ID>/                     # e.g. 20260214T093015Z_7F3A
        *_replaced_<RUN_ID>.csv     # replaced Yayoi import CSV(s)
        *_review_report.csv         # row-level review report(s)
        *_manifest.json             # per-file machine manifest(s)
        run_manifest.json           # batch-level machine manifest
    LATEST.txt                      # one line: latest RUN_ID
  artifacts/
    cache/
      client_cache.json             # append-only cache (system-managed)
    ingest/
      ledger_ref_ingested.json      # sha256 ingest manifest for ledger_ref (system-managed)
    telemetry/
      lexicon_autogrow_latest.json  # latest autogrow summary (system-managed)
      *.json                        # other internal metrics/logs (system-managed)
```

## Input types (user-provided)

1. **kari_shiwake CSV**: the file to process with `$yayoi-replacer`
2. **ledger_ref CSV**: append-only batches used by `$client-cache-builder`, `$yayoi-replacer`, and `$lexicon-extract`

## Output vs artifacts policy

1. Users collect deliverables from `clients/<CLIENT_ID>/outputs/runs/<RUN_ID>/`.
2. `clients/<CLIENT_ID>/artifacts/*` is system-managed and should not be edited manually.

## Global / shared files

1. `lexicon/lexicon.json`: single canonical category+terms dictionary (core + learned)
2. `lexicon/pending/label_queue.csv`: pending unknown-term queue for user labeling
3. `lexicon/pending/label_queue_state.json`: queue internal state (system-managed)
4. `lexicon/pending/locks/label_queue.lock`: global lock for queue/state mutation
5. `defaults/category_defaults.json`: global default debit-account mapping per category
6. `clients/<CLIENT_ID>/config/category_overrides.json`: per-client editable debit-account overlay
7. `rulesets/`: versioned deterministic configuration snapshots

## Ingest marker extension (`ledger_ref_ingested.json`)

Each `ingested[sha256]` entry may include:
1. `processed_to_label_queue_at` (ISO-8601 UTC)
2. `processed_to_label_queue_run_id` (optional)
3. `processed_to_label_queue_version` (optional)

These fields are append-only markers used to guarantee idempotent label queue growth.
