# TEMPLATE client folder

Copy this folder to create a new client:

- clients/<CLIENT_ID>/

Use line-scoped directories:

- clients/<CLIENT_ID>/lines/receipt/
- clients/<CLIENT_ID>/lines/bank_statement/
- clients/<CLIENT_ID>/lines/credit_card_statement/ (UNIMPLEMENTED in Phase 1)

For receipt line:

- Put input CSVs under:
  - lines/receipt/inputs/kari_shiwake/
  - lines/receipt/inputs/ledger_ref/
- Run outputs are written under:
  - lines/receipt/outputs/runs/<RUN_ID>/
  - lines/receipt/outputs/LATEST.txt
- System-managed files are under:
  - lines/receipt/artifacts/cache/
  - lines/receipt/artifacts/ingest/
  - lines/receipt/artifacts/telemetry/

For bank_statement line:

- Training is optional:
  - 0/0 no-op: both inboxes empty (`lines/bank_statement/inputs/training/ocr_kari_shiwake/` and `lines/bank_statement/inputs/training/reference_yayoi/`)
  - If training is used, provide exactly one pair per run: OCR CSV 1 + reference CSV/TXT 1
  - One-side-only or multiple files on either side fail-closed

`clients/<CLIENT_ID>/config/` is reserved for future shared config.

See spec/FILE_LAYOUT.md.


