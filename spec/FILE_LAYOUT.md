# File layout contract (repo)

This repository is designed for deterministic, shell-driven operation via Codex Agent Skills.
All client data is isolated under `clients/<CLIENT_ID>/` to prevent cross-client file mixups.

## Client directory layout

```
clients/<CLIENT_ID>/
  inputs/
    kari_shiwake/      # Yayoi 25-col CSV to be replaced (target)
    ledger_ref/        # Historical finalized journal CSVs (append-only batches; T-number cache source)
    ledger_train/      # Historical journal CSVs (append-only batches; lexicon long-tail source)
  artifacts/
    client_cache.json            # Append-only cache (generated; grows over time)
    ledger_ref_ingested.json   # sha256 ingest manifest for ledger_ref (generated)
    ledger_train_ingested.json # sha256 ingest manifest for ledger_train (generated)
    reports/                   # run manifests + review reports (generated)
  outputs/
    *.csv              # Replaced Yayoi import CSV(s) (generated)
```

### Input types (user-provided)

1. **kari_shiwake CSV**: the file to process with `$yayoi-replacer`
2. **ledger_ref CSV**: append-only batches used by `$client-cache-builder` and `$yayoi-replacer` (cache update)
3. **ledger_train CSV**: append-only batches used by `$lexicon-extract` (unknown-term collection)

## Global / shared files

1. `lexicon/lexicon.json` : single canonical category+terms dictionary (core + learned)
2. `lexicon/pending/label_queue.csv` : pending unknown-term queue for user labeling
3. `defaults/category_defaults.json` : default debit-account mapping per category (used when client_cache lacks evidence)
4. `rulesets/` : versioned deterministic configuration snapshots (parameter defaults)


