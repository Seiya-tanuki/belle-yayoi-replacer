# CLIENT_REGISTRATION_INIT_SPEC

This spec is the source of truth for successful new-client registration initialization artifacts.
It covers only onboarding-time behavior for `.agents/skills/client-register/register_client.py`.
It does not change runtime replacer, cache-builder, or local UI replacement contracts.

## Scope

1. Runs are written only for successful new-client registration.
2. The shared audit location is client-root scoped:
   1. `clients/<CLIENT_ID>/artifacts/client_registration/runs/<RUN_ID>/run_manifest.json`
   2. `clients/<CLIENT_ID>/artifacts/client_registration/LATEST.txt`
3. This audit area is onboarding provenance only.
4. Later user edits to `category_overrides.json` are out of scope for this audit stream.

## Top-level manifest contract

`run_manifest.json` must be UTF-8 JSON with:

1. `schema`: `belle.client_registration_init.run_manifest.v1`
2. `version`: `1.0`
3. `client_id`: registered client id
4. `run_id`: audit run id also written to `LATEST.txt`
5. `created_at`: ISO-8601 UTC timestamp
6. `selected_lines`: selected registration line ids in registration order
7. `bookkeeping_mode`: selected bookkeeping mode
8. `category_override_bootstrap`: object defined below

## category_override_bootstrap

Required fields:

1. `requested`: boolean
2. `status`: stable string
   1. `skipped_no_teacher`: no teacher path was supplied
   2. `applied`: at least one `target_account` change was written
   3. `no_changes`: teacher parsing succeeded but no override row changed
3. `teacher_source_basename`: basename only, never an absolute path
4. `teacher_source_sha256`: SHA-256 of the teacher source bytes, or empty string when not requested
5. `row_count`: total parsed Yayoi rows
6. `clear_rows`: rows where `match_summary(...).quality == "clear"`
7. `ambiguous_rows`: rows where `match_summary(...).quality == "ambiguous"`
8. `none_rows`: rows where `match_summary(...).quality == "none"`
9. `rules_used`: object
   1. `matched_rows_min`: `2`
   2. `strict_plurality`: `true`
   3. `min_p_majority`: `0.40`
   4. `denylist_exact_names`: exact-name denylist array used for v1
10. `per_line`: object keyed by selected override lines only (`receipt`, `credit_card_statement`)

## Bootstrap teacher analysis contract

1. The teacher input is exactly one Yayoi import CSV/TXT with the repository's fixed 25-column cp932 contract.
2. Reading must use `belle.yayoi_csv.read_yayoi_csv()`.
3. Only these columns are used:
   1. summary: `belle.yayoi_columns.COL_SUMMARY`
   2. debit account: `belle.yayoi_columns.COL_DEBIT_ACCOUNT`
4. Category matching must use:
   1. `belle.lexicon.load_lexicon()`
   2. `belle.lexicon.match_summary()`
5. Rows with `category_key is None` are excluded from category aggregation.
6. Ambiguous matches are counted and included in aggregation in v1.
7. Aggregation is debit-account vote counting per matched category.
8. A category is eligible only when all are true:
   1. `matched_rows >= 2`
   2. strict plurality (`top_account_count > second_account_count`)
   3. `p_majority >= 0.40`
   4. top account is not in the exact-name denylist
9. If the top account is denylisted, do not promote the second-ranked account.

## per_line contract

For each selected override line:

1. `applied_count`: number of written override row changes
2. `changes`: ordered array of change items

Each change item must include:

1. `category_key`
2. `category_label`
3. `from_target_account`
4. `to_target_account`

## category_overrides write contract

1. Registration first generates full `category_overrides.json` exactly from the selected bookkeeping-mode defaults variant and current lexicon keys.
2. Bootstrap, when requested, is applied only after generation.
3. Bootstrap may rewrite only `target_account`.
4. Bootstrap must never rewrite `target_tax_division`.
5. The file schema remains `belle.category_overrides.v2`.
6. No provenance, audit metadata, or absolute source paths may be embedded in `category_overrides.json`.
