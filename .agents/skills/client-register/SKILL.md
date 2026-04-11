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
2. Requires an explicit bookkeeping mode choice for each new client:
   1. non-interactive: `--bookkeeping-mode tax_excluded|tax_included` is required
   2. interactive: operator must explicitly choose `税抜き` or `税込み`
3. Copies `clients/TEMPLATE/` to `clients/<CLIENT_ID>/`.
4. Writes the staged shared tax postprocess config at:
   1. `clients/<CLIENT_ID>/config/yayoi_tax_config.json`
   2. `tax_excluded` writes `enabled: true`, `bookkeeping_mode: tax_excluded`, `rounding_mode: floor`
   3. `tax_included` writes `enabled: false`, `bookkeeping_mode: tax_included`, `rounding_mode: floor`
   4. `schema` / `version` are preserved from the staged template file; registration fails closed if the staged file is missing or invalid.
5. Provisions lines based on `--line`:
   1. default `--line all`: `receipt`, `bank_statement`, `credit_card_statement`
   2. line-aware `--line <line_id>`: provisions only the selected line directory under `clients/<CLIENT_ID>/lines/`
6. Runs line initialization hooks for selected lines:
   1. Initializes `receipt` `config/category_overrides.json` from the selected bookkeeping-mode defaults variant.
   2. Initializes `credit_card_statement` `config/category_overrides.json` from the selected bookkeeping-mode defaults variant.
   3. Ensures `bank_statement` `config/bank_line_config.json` exists.
7. Optionally bootstraps newly generated `category_overrides.json` from one teacher Yayoi CSV/TXT via `--category-override-teacher-path`:
   1. scope is registration-time only
   2. scope is only `receipt` and `credit_card_statement`
   3. only `target_account` may be rewritten
   4. `target_tax_division` is never rewritten by this bootstrap
   5. `--line bank_statement` with this argument is unsupported and fails closed
8. Writes a shared client-registration audit run under:
   1. `clients/<CLIENT_ID>/artifacts/client_registration/runs/<RUN_ID>/run_manifest.json`
   2. `clients/<CLIENT_ID>/artifacts/client_registration/LATEST.txt`
9. `credit_card_statement` line is provisioned for runnable flow (Contract A and strict-stop are runtime-enforced).

## Execution
1. All lines (default):
```bash
python .agents/skills/client-register/register_client.py --bookkeeping-mode tax_excluded
```
2. Single line:
```bash
python .agents/skills/client-register/register_client.py --line credit_card_statement --bookkeeping-mode tax_included
```
3. With category override bootstrap teacher:
```bash
python .agents/skills/client-register/register_client.py --bookkeeping-mode tax_excluded --category-override-teacher-path path/to/teacher.csv
```

## Notes
1. Category overrides are generated from shared `lexicon/lexicon.json` category keys and line defaults:
   1. `defaults/receipt/category_defaults_tax_excluded.json`
   2. `defaults/receipt/category_defaults_tax_included.json`
   3. `defaults/credit_card_statement/category_defaults_tax_excluded.json`
   4. `defaults/credit_card_statement/category_defaults_tax_included.json`
2. `receipt` / `credit_card_statement` override generation selects the tracked defaults variant from the operator-selected bookkeeping mode and the staged `clients/<CLIENT_ID>/config/yayoi_tax_config.json` written during registration.
3. Generated `category_overrides.json` rows use the live contract:
   1. `target_account`
   2. `target_tax_division`
4. Line interpretation of those rows:
   1. `receipt`: debit side
   2. `credit_card_statement`: placeholder side (`debit` or `credit`)
5. category_overrides generation is best-effort; missing per-category defaults are filled with `global_fallback`.
6. Generated `category_overrides.json` files are runtime/client assets and are not tracked in the repository baseline.
7. Registration bootstrap teacher input must be exactly one Yayoi import CSV/TXT using the repository's 25-column cp932 contract.
8. Registration bootstrap teacher analysis uses only summary (column 17) and debit account (column 5).
9. `bank_statement` does not use category_overrides.
10. The audit manifest contract is defined by `spec/CLIENT_REGISTRATION_INIT_SPEC.md`.

## Template contract (must preserve)
1. `clients/TEMPLATE/lines/receipt/config/` exists.
2. `clients/TEMPLATE/lines/receipt/outputs/runs/` exists.
3. `clients/TEMPLATE/lines/receipt/artifacts/cache/` exists.
4. `clients/TEMPLATE/lines/receipt/artifacts/ingest/` exists.
5. `clients/TEMPLATE/lines/receipt/artifacts/telemetry/` exists.
6. `clients/TEMPLATE/lines/bank_statement/config/` exists.
7. `clients/TEMPLATE/lines/credit_card_statement/` exists.
8. `clients/TEMPLATE/config/yayoi_tax_config.json` exists as the shared client config baseline.
9. Use `.gitkeep` files as needed to keep empty directories in git.
