---
name: client-register
description: Register a new client directory by copying clients/TEMPLATE to clients/<CLIENT_ID> with strict Windows-safe name validation. Explicit invocation only.
---

# client-register

Creates a new client workspace from the template.

## Preconditions
1. Explicitly invoke `$client-register`.
2. Run in repo root.

## What this skill does
1. Validates user input and canonicalizes to a Windows-safe `CLIENT_ID`.
2. Copies `clients/TEMPLATE/` to `clients/<CLIENT_ID>/`.
3. Generates full-expanded `clients/<CLIENT_ID>/config/category_overrides.json`.
4. Prepares input directories:
   1. `inputs/kari_shiwake/`
   2. `inputs/ledger_ref/`

## Template contract (must preserve)
1. `clients/TEMPLATE/config/` exists.
2. `clients/TEMPLATE/outputs/runs/` exists.
3. `clients/TEMPLATE/artifacts/cache/` exists.
4. `clients/TEMPLATE/artifacts/ingest/` exists.
5. `clients/TEMPLATE/artifacts/telemetry/` exists.
6. Use `.gitkeep` files as needed to keep empty directories in git.

## Execution
```bash
python3 .agents/skills/client-register/register_client.py
```
