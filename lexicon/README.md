# lexicon/

Shared canonical category+terms dictionary lives at `lexicon/lexicon.json`.
The current taxonomy is the reconstructed 69-category operational/posting taxonomy.
Category keys are operational routing buckets, not a strict ontology.

Line usage:
1. `receipt`: uses lexicon as primary category inference source.
2. `credit_card_statement`: uses lexicon only as secondary category fallback.
3. `bank_statement`: does not use lexicon category routing.

Pending queue:
1. `label_queue.csv` is receipt-only in Phase D.
2. Path: `lexicon/receipt/pending/`.
3. Repository baseline tracks only placeholders under that directory; queue/state/log files are generated at runtime from empty state.

See:
1. `spec/LEXICON_SPEC.md`
2. `spec/LEXICON_PENDING_SPEC.md`
