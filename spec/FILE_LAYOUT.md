# FILE_LAYOUT (repo contract)

This repository is designed for deterministic, shell-driven operation via Codex Agent Skills.
All client data is isolated under `clients/<CLIENT_ID>/` to prevent cross-client mixups.

## Client directory layout

```text
clients/<CLIENT_ID>/
  config/
    category_overrides.json         # per-client editable full-expanded overrides
  inputs/
    kari_shiwake/                   # Yayoi 25-col CSV to be replaced (run-time requires exactly 1 file)
    ledger_ref/                     # ledger_ref ingest inbox (drop new finalized journal CSV/TXT here)
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
      ledger_ref/                   # ingested ledger_ref snapshots (system-managed)
      kari_shiwake/                 # ingested kari_shiwake snapshots (system-managed)
      kari_shiwake_ingested.json    # sha256 ingest manifest for kari_shiwake (system-managed)
      ledger_ref_ingested.json      # sha256 ingest manifest for ledger_ref (system-managed)
    telemetry/
      lexicon_autogrow_latest.json  # latest autogrow summary (system-managed)
      *.json                        # other internal metrics/logs (system-managed)
```

## Input types (user-provided)

1. **kari_shiwake CSV**: the file to process with `$yayoi-replacer`
   1. Place exactly one file in `inputs/kari_shiwake/`.
   2. At run-time it is moved+renamed to `artifacts/ingest/kari_shiwake/INGESTED_<UTC_TS>_<SHA8>.csv`.
   3. `outputs/runs/<RUN_ID>/run_manifest.json` stores `inputs.kari_shiwake.{original_name,stored_name,sha256}`.
2. **ledger_ref CSV/TXT**: append-only historical finalized batches used by `$client-cache-builder`, `$yayoi-replacer`, and `$lexicon-extract`
   1. Put new files in `inputs/ledger_ref/`.
   2. On successful ingest, each file is moved+renamed to `artifacts/ingest/ledger_ref/INGESTED_<UTC_TS>_<SHA8>.csv`.
   3. Duplicate content is moved to `artifacts/ingest/ledger_ref/IGNORED_DUPLICATE_<UTC_TS>_<SHA8>.csv`.
   4. `inputs/ledger_ref/` should be empty after successful ingest (except placeholders such as `.gitkeep`).

## Output vs artifacts policy

1. Users collect deliverables from `clients/<CLIENT_ID>/outputs/runs/<RUN_ID>/`.
2. `clients/<CLIENT_ID>/artifacts/*` is system-managed and should not be edited manually.

## Asset separation (tracked vs field assets)

1. Tracked code/spec files remain git-managed (e.g. `belle/`, `spec/`, `.agents/`, `defaults/`, `tools/`, `lexicon/lexicon.json`).
2. Field assets are runtime-managed and untracked/ignored:
   1. `clients/**` except `clients/TEMPLATE/**` (template scaffold stays tracked, including placeholder files)
   2. `lexicon/pending/**` except placeholders:
      - `lexicon/pending/.gitkeep`
      - `lexicon/pending/locks/.gitkeep`
   3. `exports/**` (including `exports/backups/`)
3. Backup/restore skills target only field assets and must never overwrite tracked code/spec files.

## Global / shared files

1. `lexicon/lexicon.json`: single canonical category+terms dictionary (core + learned)
2. `lexicon/pending/label_queue.csv`: pending unknown-term queue for user labeling
3. `lexicon/pending/label_queue_state.json`: queue internal state (system-managed)
4. `lexicon/pending/locks/label_queue.lock`: global lock for queue/state mutation
5. `defaults/category_defaults.json`: global default debit-account mapping per category
6. `clients/<CLIENT_ID>/config/category_overrides.json`: per-client editable debit-account overlay
7. `rulesets/`: versioned deterministic configuration snapshots
8. `exports/backups/`: asset backup ZIPs and restore safety snapshots (runtime-managed)

## Ingest marker extension (`ledger_ref_ingested.json`)

Each `ingested[sha256]` entry may include:
1. `stored_name` and `stored_relpath` (relative path from `clients/<CLIENT_ID>/`)
2. `processed_to_label_queue_at` (ISO-8601 UTC)
3. `processed_to_label_queue_run_id` (optional)
4. `processed_to_label_queue_version` (optional)

These fields are append-only markers used to guarantee idempotent label queue growth.
