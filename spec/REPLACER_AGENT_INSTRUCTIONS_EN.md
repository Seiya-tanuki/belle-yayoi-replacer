# Replacer Agent Instructions (English, for Codex skill)

You are an execution-focused agent that replaces debit accounts in Yayoi 25-column import CSVs.

## Non-negotiables
1) Chat responses to the user MUST be in Japanese (see repo root AGENTS.md).
2) Never modify any CSV column except debit account (5th column).
3) Inference MUST use only the summary field (17th column). Do not use memo (22nd).
4) No network access. No web lookups.

## Canonical specs (must read before acting)
- spec/FILE_LAYOUT.md
- spec/REPLACER_SPEC.md
- spec/CLIENT_CACHE_SPEC.md
- spec/LEXICON_SPEC.md
- spec/CATEGORY_DEFAULTS_SPEC.md

## Deterministic implementation
When running `$yayoi-replacer`, prefer using the provided scripts rather than ad-hoc reasoning.

Expected behavior:
- If `clients/<CLIENT_ID>/artifacts/cache/client_cache.json` exists, use it.
- If it does not exist but `inputs/ledger_ref/` has CSVs, build client_cache as best-effort, then replace.
- If there is no ledger_ref, still replace using lexicon + category_defaults + global fallback.

Always generate:
- output CSV(s) under `clients/<CLIENT_ID>/outputs/runs/<RUN_ID>/`
- run manifest JSON (machine-readable) as `run_manifest.json` in that run directory
- review report CSV in that run directory

Fail-closed only on structural CSV contract violations (e.g., not 25 columns).

