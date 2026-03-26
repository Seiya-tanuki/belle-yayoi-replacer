# Belle Local UI Manual

## Purpose

`Belle ローカルUI` is a local NiceGUI wrapper for daily operation.
It is intentionally narrow and delegates actual replacement and collection work to existing scripts.

## Entry Points

- `python start_local_ui.py`
- `python start_local_ui.py --host 127.0.0.1 --port 8080 --no-browser`

## Scope

- Select or create a client
- Select document lines for the current session
- Stage input files into canonical client paths
- Run precheck and execution through `yayoi-replacer`
- Collect current-session outputs through `collect-outputs`

## Non-goals

- Editing lexicon or pending queues
- Editing runtime config files
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
- `exports/` remains outside Git tracking; operational progress files live there by bundle design.
