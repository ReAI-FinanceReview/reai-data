<!-- Parent: ../AGENTS.md -->
<!-- Generated: 2026-08-10T08:45:46Z | Updated: 2026-08-28T00:00:00Z -->

# src/processing

## Purpose
Bronze → Silver cleansing (issue #14). Normalizes raw Korean review text into `refined_text` in the
order `ReviewCleaner.clean()` applies them (`cleanse.py:147-155`): Unicode normalization, emoji
removal, repeated-character reduction, PII masking, special-character stripping, synonym
canonicalization, profanity tagging, whitespace collapsing. Output is written as Silver Parquet and
the review's `processing_status` advances `RAW → CLEANED`.

## Key Files

| File | Description |
|------|-------------|
| `cleanse.py` | Text primitives (`normalize_unicode`, `remove_emojis`, `reduce_repeated_chars`, `remove_special_chars`, `normalize_whitespace`, `mask_pii`), the `ReviewCleaner.clean(text)` rule engine, Parquet IO (`load_bronze_parquet`, `write_silver_parquet`), and `ReviewCleaningPipeline.run(target_date)` |
| `__init__.py` | Empty package marker — import `src.processing.cleanse` directly |

## Subdirectories
None.

## For AI Agents

### Working In This Directory
- The pipeline is date-scoped: `run(target_date)` processes one Bronze partition. The Airflow task
  passes `{{ ds }}` through `scripts/cleanse_reviews.py --date`.
- Dictionaries are injected as paths (`synonyms_path`, `profanity_path`), not read from a global —
  `scripts/cleanse_reviews.py` supplies `config/dictionaries/*.json`. Keep them parameters so tests
  can pass temp files.
- Text rules are ordered; changing the order changes the output. PII masking runs *before*
  special-character stripping so phone and email separators still exist to match on, and whitespace
  collapsing runs last. Add a regression test with any reordering.
- PII masking is a correctness requirement, not a nicety — masked values must never be reversible
  back into Silver output.
- `run_preprocess` in `src/pipeline/steps.py` is the deprecated predecessor of this module; it warns
  and returns failure. Do not route new work through it.

### Testing Requirements
`tests/test_cleanse.py` builds temporary
synonym/profanity JSON files per test.

```bash
TEST_DATABASE_URL="${TEST_DATABASE_URL:-postgresql://testuser:testpass@localhost:5433/testdb}" \
  PYTHONPATH=. uv run pytest tests/test_cleanse.py -q
```

### Common Patterns
Pure functions for each text rule, composed by `ReviewCleaner.clean()`; the pipeline class owns IO
and DB state so the rule layer stays trivially testable.

## Dependencies

### Internal
`config/dictionaries/synonyms.json`, `config/dictionaries/profanity.json`,
`src/models/review_master_index.py`, `src/schemas/parquet/review_preprocessed.py`,
`src/utils/minio_client.py`, `src/utils/parquet_writer.py`.

### External
`emoji`, `flashtext`, `konlpy`, `nltk`, `pyarrow`, `sqlalchemy` (stdlib `re` / `unicodedata` for the
core rules).

<!-- MANUAL: Notes added below this line are preserved on regeneration -->
