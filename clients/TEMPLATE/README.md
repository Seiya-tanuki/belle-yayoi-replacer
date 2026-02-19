# TEMPLATE client folder

Copy this folder to create a new client:

- clients/<CLIENT_ID>/

Use line-scoped directories:

- clients/<CLIENT_ID>/lines/receipt/
- clients/<CLIENT_ID>/lines/bank_statement/ (UNIMPLEMENTED in Phase 1)
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

`clients/<CLIENT_ID>/config/` is reserved for future shared config.

See spec/FILE_LAYOUT.md.


