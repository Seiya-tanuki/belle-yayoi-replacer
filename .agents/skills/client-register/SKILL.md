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
3. `--line receipt`:
   1. Generates full-expanded `clients/<CLIENT_ID>/lines/receipt/config/category_overrides.json`.
   2. Requires and prepares receipt input directories including `inputs/ledger_ref/`.
4. `--line bank_statement`:
   1. Requires Phase 4 bank template layout (`inputs/training/*`, `inputs/kari_shiwake`, `artifacts/ingest/*`).
   2. Ensures `clients/<CLIENT_ID>/lines/bank_statement/config/bank_line_config.json` exists.
   3. Does not require or initialize receipt-only assets (`inputs/ledger_ref`, `category_overrides`, `lexicon/defaults/rulesets`).
5. `--line credit_card_statement`:
   1. Registration is allowed (copy template only), even while replacer implementation is pending.

## Template contract (must preserve)
1. `clients/TEMPLATE/lines/receipt/config/` exists.
2. `clients/TEMPLATE/lines/receipt/outputs/runs/` exists.
3. `clients/TEMPLATE/lines/receipt/artifacts/cache/` exists.
4. `clients/TEMPLATE/lines/receipt/artifacts/ingest/` exists.
5. `clients/TEMPLATE/lines/receipt/artifacts/telemetry/` exists.
6. `clients/TEMPLATE/config/` may remain as optional shared config root.
7. Use `.gitkeep` files as needed to keep empty directories in git.

## Execution
```bash
python .agents/skills/client-register/register_client.py --line receipt
python .agents/skills/client-register/register_client.py --line bank_statement
```
