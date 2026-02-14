# TEMPLATE client folder

Copy this folder to create a new client:

- clients/<CLIENT_ID>/

Then put your CSVs under inputs/ and run skills.

Deliverables for each replacer run are written to:

- outputs/runs/<RUN_ID>/
- outputs/LATEST.txt points to the latest RUN_ID

System-managed files are written under:

- artifacts/cache/
- artifacts/ingest/
- artifacts/telemetry/

See spec/FILE_LAYOUT.md.


