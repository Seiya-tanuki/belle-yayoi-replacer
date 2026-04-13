# Replacer Agent Instructions (English, for Codex skill)

You are an execution-focused agent that performs line-aware deterministic replacement for Yayoi 25-column import CSVs.

## Non-negotiables
1) Chat responses to the user MUST be in Japanese (see repo root AGENTS.md).
2) Never modify any CSV column outside the contract of the selected line:
   - `receipt`: follow `spec/REPLACER_SPEC.md`
   - `bank_statement`: follow `spec/BANK_REPLACER_SPEC.md`
   - `credit_card_statement`: follow `spec/CREDIT_CARD_REPLACER_SPEC.md`
3) Inference inputs are line-specific. Follow the selected line's canonical spec; do not generalize one line's summary/memo rules to another line.
4) Fail closed whenever the selected line's canonical spec requires it.
5) No network access. No web lookups.

## Canonical specs (must read before acting)
- spec/FILE_LAYOUT.md
- spec/REPLACER_SPEC.md
- spec/BANK_REPLACER_SPEC.md
- spec/CREDIT_CARD_REPLACER_SPEC.md
- spec/CLIENT_CACHE_SPEC.md
- spec/LEXICON_SPEC.md
- spec/CATEGORY_DEFAULTS_SPEC.md

## Deterministic implementation
When running `$yayoi-replacer`, prefer using the provided scripts rather than ad-hoc reasoning.

Expected behavior:
- Resolve paths from `clients/<CLIENT_ID>/lines/<line_id>/` per `spec/FILE_LAYOUT.md`.
- Use only the selected line's allowed inputs, caches, and ingest state.
- If line-specific learned evidence is unavailable, still replace using the deterministic fallback chain allowed by the selected line's canonical spec.
- Respect any line-specific strict-stop conditions defined by the selected line's canonical spec.

Always generate:
- output CSV(s) under `clients/<CLIENT_ID>/lines/<line_id>/outputs/runs/<RUN_ID>/`
- run manifest JSON (machine-readable) as `run_manifest.json` in that run directory
- review report CSV in that run directory
- update `clients/<CLIENT_ID>/lines/<line_id>/outputs/LATEST.txt` for the selected line

