---
name: yayoi-replacer
description: Deterministic multi-line replacer for Yayoi 25-col CSV. Always use PLAN/dry-run then explicit user approval.
---

# yayoi-replacer

Deterministic replacement skill for Yayoi import CSVs.

## Preconditions
1. `--client <CLIENT_ID>` is required.
2. `--line` choices: `receipt`, `bank_statement`, `credit_card_statement`, `all` (default: `all`).
3. Work under a single client folder.
4. Receipt preferred line layout:
   - `clients/<CLIENT_ID>/lines/receipt/`
5. Receipt legacy fallback (deprecated, auto-detected if line layout missing):
   - `clients/<CLIENT_ID>/`
6. Bank line is line-scoped only:
   - `clients/<CLIENT_ID>/lines/bank_statement/`
7. `bank_statement` training is optional at runtime:
   - if no training files are provided, runtime continues (no learning update)
   - if training files are provided, they must be exactly one pair:
     - OCR: exactly one `*.csv` under `inputs/training/ocr_kari_shiwake/`
     - teacher reference: exactly one `*.csv` or `*.txt` under `inputs/training/reference_yayoi/`
   - one-side-only or multiple files are fail-closed (`FAIL`)
8. `credit_card_statement` is currently unimplemented.

## Operator protocol (mandatory)
縺薙・謇矩・・ Codex/operator 螳溯｡梧凾縺ｮ譛荳贋ｽ阪Λ繝ｳ繝悶ャ繧ｯ縺ｧ縺ゅｊ縲∝ｿ・★縺薙・鬆・ｺ上〒螳滓命縺吶ｋ縺薙→縲・
### Step 1: 繧ｯ繝ｩ繧､繧｢繝ｳ繝域欠螳夲ｼ域耳貂ｬ遖∵ｭ｢・・1. 繝ｦ繝ｼ繧ｶ繝ｼ縺後け繝ｩ繧､繧｢繝ｳ繝医ｒ譏守､ｺ縺励※縺・↑縺・ｴ蜷医・縲∝ｿ・★谺｡繧偵◎縺ｮ縺ｾ縺ｾ霑斐☆:
   - 縲檎ｽｮ謠帙ｒ陦後≧繧ｯ繝ｩ繧､繧｢繝ｳ繝医ｒ謖・ｮ壹＠縺ｦ縺上□縺輔＞縲ゅ・2. `CLIENT_ID` 繧呈耳貂ｬ繝ｻ陬懷ｮ後＠縺ｦ縺ｯ縺ｪ繧峨↑縺・・
### Step 2: 莠句燕遒ｺ隱搾ｼ・--dry-run` 繧貞ｸｸ縺ｫ螳溯｡鯉ｼ・1. 蠢・★谺｡縺ｮ繧ｳ繝槭Φ繝峨ｒ螳溯｡後☆繧・
```bash
python .agents/skills/yayoi-replacer/scripts/run_yayoi_replacer.py --client "<CLIENT_ID>" --line all --dry-run
```
2. 螳溯｡悟ｾ後・ line 縺斐→縺ｫ谺｡縺ｮ繝ｩ繝吶Ν縺ｧ隕∫ｴ・☆繧・
   - `鄂ｮ謠帛ｯｾ雎｡縺ｪ縺輿 : `skip`・・ari_shiwake縺・莉ｶ・・   - `鄂ｮ謠帛庄閭ｽ` : `ready`・亥ｿ・ｦ√ヵ繧｡繧､繝ｫ縺ｮ遒ｺ隱阪′OK・・   - `蠢・医ヵ繧｡繧､繝ｫ荳崎ｶｳ` : `fail`・井ｸ崎ｶｳ蜀・ｮｹ繧呈・遉ｺ・・3. 縺昴・蠕後∝ｿ・★谺｡縺ｮ譁・ｨ繧偵◎縺ｮ縺ｾ縺ｾ陦ｨ遉ｺ縺吶ｋ:
   - 縲悟ｮ溯｡悟燕縺ｮ遒ｺ隱咲ｵ先棡縺ｧ縺吶ゅ％縺ｮ蜀・ｮｹ縺ｧ螳溯｡後＠縺ｾ縺吶°・溷ｮ溯｡後☆繧句ｴ蜷医・"螳溯｡後ｒ險ｱ蜿ｯ"縺ｨ蜈･蜉帙＠縺ｦ縺上□縺輔＞縲ゅ・4. 繝ｦ繝ｼ繧ｶ繝ｼ縺後ヵ繧｡繧､繝ｫ霑ｽ蜉繝ｻ蟾ｮ縺玲崛縺亥ｾ後↓蜀咲｢ｺ隱阪ｒ豎ゅａ縺溷ｴ蜷医・縲∝ｿ・★ Step 2 繧貞・螳溯｡後☆繧九り・蜍輔〒譛ｬ螳溯｡後∈騾ｲ繧薙〒縺ｯ縺ｪ繧峨↑縺・・5. `--dry-run` 縺ｫ `--yes` 繧剃ｻ倥￠雜ｳ縺励※縺ｯ縺ｪ繧峨↑縺・ｼ・ry-run 縺ｯ `--yes` 荳崎ｦ・ｼ峨・
### Step 3: 螳溯｡鯉ｼ域価隱阪ヨ繝ｼ繧ｯ繝ｳ蜿鈴伜ｾ後・縺ｿ・・1. 繝ｦ繝ｼ繧ｶ繝ｼ蜈･蜉帙′ **螳悟・荳閾ｴ縺ｧ** 縲悟ｮ溯｡後ｒ險ｱ蜿ｯ縲阪・蝣ｴ蜷医・縺ｿ縲∵悽螳溯｡後∈騾ｲ繧薙〒繧医＞縲・2. 螳溯｡後さ繝槭Φ繝峨・谺｡繧堤畑縺・ｋ:
```bash
python .agents/skills/yayoi-replacer/scripts/run_yayoi_replacer.py --client "<CLIENT_ID>" --line all --yes
```
3. 遖∵ｭ｢莠矩・
   - 繝ｦ繝ｼ繧ｶ繝ｼ縺後悟ｮ溯｡後ｒ險ｱ蜿ｯ縲阪→蜈･蜉帙☆繧句燕縺ｫ `--yes` 繧剃ｻ倥￠縺ｦ螳溯｡後＠縺ｦ縺ｯ縺ｪ繧峨↑縺・・   - Step 2 繧堤怐逡･縺励※縺ｯ縺ｪ繧峨↑縺・・
### 蠢・医ヵ繧｡繧､繝ｫ繝｡繝｢・郁ｨｺ譁ｭ邨先棡蜆ｪ蜈茨ｼ・1. `receipt` line: 蟇ｾ雎｡縺ｯ `inputs/kari_shiwake/` 驟堺ｸ九ょｮ溯｡梧凾繧｢繧ｻ繝・ヨ荳崎ｶｳ縺ｯ PLAN 縺ｮ `fail` 蜀・ｮｹ繧偵◎縺ｮ縺ｾ縺ｾ謠千､ｺ縺吶ｋ縲・2. `bank_statement` line: training は任意。投入する場合は OCR1 + teacher1 の単一ペアのみ（片側のみ/複数は FAIL）。3. `credit_card_statement` line: 譛ｪ螳溯｣・・縺溘ａ `--line all` 縺ｧ縺ｯ skip縲・
### Examples (dialog)
1. User: 縲軽ayoi-replacer繧貞ｮ溯｡後＠縺ｦ縲・   - Operator: 縲檎ｽｮ謠帙ｒ陦後≧繧ｯ繝ｩ繧､繧｢繝ｳ繝医ｒ謖・ｮ壹＠縺ｦ縺上□縺輔＞縲ゅ・2. User: 縲靴LIENT_ID 縺ｯ acme縲・   - Operator: Step 2 縺ｮ `--dry-run` 繧貞ｮ溯｡後＠縺ｦ邨先棡隕∫ｴ・ｒ謠千､ｺ縺励∵ｬ｡繧定｡ｨ遉ｺ:
   - 縲悟ｮ溯｡悟燕縺ｮ遒ｺ隱咲ｵ先棡縺ｧ縺吶ゅ％縺ｮ蜀・ｮｹ縺ｧ螳溯｡後＠縺ｾ縺吶°・溷ｮ溯｡後☆繧句ｴ蜷医・"螳溯｡後ｒ險ｱ蜿ｯ"縺ｨ蜈･蜉帙＠縺ｦ縺上□縺輔＞縲ゅ・3. User: 縲悟ｮ溯｡後ｒ險ｱ蜿ｯ縲・   - Operator: Step 3 縺ｮ `--yes` 繧ｳ繝槭Φ繝峨〒螳溯｡後☆繧九・
## PLAN semantics (always printed)
1. The skill always performs preflight planning and prints:
   - `[PLAN] client=<CLIENT_ID> line=<...>`
   - one line per selected line with `RUN` / `SKIP` / `FAIL`
2. `SKIP` only when target input count in `inputs/kari_shiwake/` is 0.
3. `FAIL` when:
   - target input count is 2 or more
   - required runtime/config is missing
   - structural invariants are invalid
   - bank training input contract is violated (one-side only or multiple files)
4. `credit_card_statement` behavior:
   - in `--line all`: `SKIP (unimplemented)`
   - explicit `--line credit_card_statement`: exit 2 with clear unimplemented error

## Confirmation gate
1. If PLAN has any `FAIL`, execution is blocked and exits 1.
2. `--dry-run` prints PLAN and exits:
   - 0 when no `FAIL`
   - 1 when `FAIL` exists
3. If there are `RUN` lines:
   - `--yes`: proceed without prompt
   - interactive TTY: prompt `Proceed with RUN lines? [y/N]`
   - non-interactive without `--yes`: exit 2 with guidance
4. If all selected lines are `SKIP`, exits 0 with `[OK] nothing to do`.

## Runtime behavior (line execution)
1. The skill entrypoint is a dispatcher only.
2. Line runners are separated under `belle/line_runners/`:
   - `receipt.py`
   - `bank_statement.py`
   - `credit_card_statement.py`
3. Receipt and bank execution logic remain unchanged in behavior; only orchestration is refactored.

## Canonical specs
1. `spec/REPLACER_SPEC.md`
2. `spec/CATEGORY_OVERRIDES_SPEC.md`
3. `spec/CLIENT_CACHE_SPEC.md`
4. `spec/LEXICON_PENDING_SPEC.md`
5. `spec/BANK_REPLACER_SPEC.md`

## Execution examples
```bash
python .agents/skills/yayoi-replacer/scripts/run_yayoi_replacer.py --client <CLIENT_ID>
```

```bash
python .agents/skills/yayoi-replacer/scripts/run_yayoi_replacer.py --client <CLIENT_ID> --line receipt --yes
```

```bash
python .agents/skills/yayoi-replacer/scripts/run_yayoi_replacer.py --client <CLIENT_ID> --line all --dry-run
```

```bash
python .agents/skills/yayoi-replacer/scripts/run_yayoi_replacer.py --client "<CLIENT_ID>" --line all --dry-run
```

```bash
python .agents/skills/yayoi-replacer/scripts/run_yayoi_replacer.py --client "<CLIENT_ID>" --line all --yes
```

