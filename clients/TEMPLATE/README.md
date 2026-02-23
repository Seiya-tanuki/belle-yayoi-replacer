# TEMPLATE client folder

Copy this folder to create a new client:

- clients/<CLIENT_ID>/

Use line-scoped directories:

- clients/<CLIENT_ID>/lines/receipt/
- clients/<CLIENT_ID>/lines/bank_statement/
- clients/<CLIENT_ID>/lines/credit_card_statement/ (implemented; Contract A required)

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

For credit_card_statement line:

- Contract A: one target file must represent exactly one statement for one card
- Target input count in `lines/credit_card_statement/inputs/kari_shiwake/`:
  - 0 => SKIP
  - 1 => RUN
  - 2+ => FAIL
- Runtime strict-stop may exit `2` after artifacts are written when `payable_sub_fill_required_failed == true`

`clients/<CLIENT_ID>/config/` is reserved for future shared config.

See spec/FILE_LAYOUT.md.


