# Belle Local UI Manual

## Purpose

`Belle ローカルUI` is a local NiceGUI wrapper for daily operation.
It is intentionally narrow and delegates actual replacement and collection work to existing scripts.

## Entry Points

- `python start_local_ui.py`
- `python start_local_ui.py --host 127.0.0.1 --port 8080 --no-browser`

## Scope

- Select or create a client
- Choose bookkeeping mode when creating a new client
- Optionally attach one teacher CSV / TXT during new-client creation
- Select document lines for the current session
- Stage input files into canonical client paths
- Run precheck and execution through `yayoi-replacer`
- Collect current-session outputs through `collect-outputs`

## Non-goals

- Editing lexicon or pending queues
- Editing runtime config files
- Editing `target_tax_division` inside `category_overrides.json`
- Editing receipt line config in `clients/<CLIENT_ID>/lines/receipt/config/receipt_line_config.json`
- Editing credit-card tax-threshold config in `clients/<CLIENT_ID>/lines/credit_card_statement/config/credit_card_line_config.json`
- Editing line-level tax-threshold config of any kind
- Editing `clients/<CLIENT_ID>/config/yayoi_tax_config.json`
- Editing review CSV outputs
- Replacing CLI workflows for maintenance tasks

## Code Layout

- `belle/local_ui/app.py`: app bootstrap and route registration
- `belle/local_ui/state.py`: in-memory wizard state
- `belle/local_ui/services/clients.py`: client list/create
- `belle/local_ui/services/uploads.py`: slot path resolution and staging
- `belle/local_ui/services/replacer.py`: precheck/run subprocess wrappers and parsers
- `belle/local_ui/services/collect.py`: collect subprocess wrapper and manifest comparison
- `belle/local_ui/pages/*.py`: page builders only

## Notes

- The UI assumes local execution on `127.0.0.1`.
- NiceGUI import is shimmed for current Python 3.14 behavior via `belle/local_ui/nicegui_compat.py`.
- New client creation requires an explicit bookkeeping-mode selection: `税抜経理` or `税込経理`.
- That creation-time selection is passed to `client-register`, which writes `clients/<CLIENT_ID>/config/yayoi_tax_config.json` and seeds `receipt` / `credit_card_statement` `category_overrides.json` from the corresponding defaults variant.
- New client creation can optionally attach one teacher CSV / TXT for registration-time category override bootstrap.
- The local UI stages that optional file under repo-root `.tmp/local_ui/client_register_bootstrap/<SESSION_TOKEN>/<ORIGINAL_BASENAME>`.
- Repo-root `.tmp` is reused when it already exists and is created with `mkdir(..., exist_ok=True)` when it does not.
- The new-client page preview is intentionally minimal and non-technical: it shows only the Japanese category label and the replacement account that would be auto-set.
- Final bootstrap application, audit manifest writing, and registration-time validation still belong to the Phase 1 registration backend (`client-register`); the local UI only stages the optional file, shows the minimal preview, and passes the path through.
- The local UI does not later edit `target_tax_division`, `clients/<CLIENT_ID>/config/yayoi_tax_config.json`, or any line-level tax-threshold config; operators manage those assets outside the UI after creation.
- `exports/` remains outside Git tracking; operational progress files live there by bundle design.
