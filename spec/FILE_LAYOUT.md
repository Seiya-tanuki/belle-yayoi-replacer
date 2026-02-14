# File layout contract (repo)

This repository is designed for deterministic, shell-driven operation via Codex Agent Skills.
All client data is isolated under `clients/<CLIENT_ID>/` to prevent cross-client file mixups.

## Client directory layout

```
clients/<CLIENT_ID>/
  config/
    category_overrides.json # per-client editable full-expanded overrides
  inputs/
    kari_shiwake/      # Yayoi 25-col CSV to be replaced (target)
    ledger_ref/        # Historical finalized journal CSVs (append-only batches; T-number cache source)
    ledger_train/      # Historical journal CSVs (append-only batches; lexicon long-tail source)
  outputs/
    runs/
      <RUN_ID>/                 # e.g. 20260214T093015Z_7F3A
        *_replaced_<RUN_ID>.csv # replaced Yayoi import CSV(s)
        *_review_report.csv     # row-level review report(s)
        *_manifest.json         # per-file machine manifest(s)
        run_manifest.json       # batch-level machine manifest
        run.log                 # optional
    LATEST.txt                  # one line: latest RUN_ID
  artifacts/
    cache/
      client_cache.json         # append-only cache (system-managed)
    ingest/
      ledger_ref_ingested.json  # sha256 ingest manifest for ledger_ref (system-managed)
      ledger_train_ingested.json # sha256 ingest manifest for ledger_train (system-managed)
    telemetry/
      *.json                    # internal metrics/logs (system-managed)
```

### Input types (user-provided)

1. **kari_shiwake CSV**: the file to process with `$yayoi-replacer`
2. **ledger_ref CSV**: append-only batches used by `$client-cache-builder` and `$yayoi-replacer` (cache update)
3. **ledger_train CSV**: append-only batches used by `$lexicon-extract` (unknown-term collection)

## Output vs artifacts policy

1. Users collect deliverables from `clients/<CLIENT_ID>/outputs/runs/<RUN_ID>/`.
2. `clients/<CLIENT_ID>/artifacts/*` is system-managed and should not be edited manually.

## Global / shared files

1. `lexicon/lexicon.json` : single canonical category+terms dictionary (core + learned)
2. `lexicon/pending/label_queue.csv` : pending unknown-term queue for user labeling
3. `defaults/category_defaults.json` : global default debit-account mapping per category (shared baseline)
4. `clients/<CLIENT_ID>/config/category_overrides.json` : per-client editable debit-account overlay (full-expanded)
5. `rulesets/` : versioned deterministic configuration snapshots (parameter defaults)


