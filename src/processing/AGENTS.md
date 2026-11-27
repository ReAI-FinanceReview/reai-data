<!-- Parent: ../AGENTS.md -->
<!-- Generated: 2026-08-10T08:45:46Z | Updated: 2026-11-27T00:00:00Z -->

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
- A day's Bronze partition can hold one review under several `review_id`s. An App Store crawl
  stopped by the empty-feed cutoff still uploads what it read, its Airflow retry runs before
  `load_reviews`, and the crawler dedups only against `review_master_index`, so the retry uploads
  those reviews again with new uuid7 ids (`BaseCrawler.save_daily_batch`). `BatchLoader` loads each
  `platform_review_id` once. `run` reads every object in the partition (`load_bronze_parquet`), so
  before grouping it calls `_drop_duplicate_bronze_rows`: for each `platform_review_id` with more
  than one `review_id` it keeps the rows whose `review_id` is in `review_master_index` (one query
  for all such ids) and drops the other copies; a group none of whose ids is there was never
  loaded, cannot be marked `CLEANED`, and is dropped whole. The drop is logged at WARNING and
  counted as `duplicates_dropped` in `run`'s result. A `platform_review_id` with a single
  `review_id` is not looked up and goes through as before. Without this step `_update_db_status`
  raises `Conflicting review_id values`, and because Bronze objects are never rewritten, every
  retry of that day's cleanse would fail the same way.
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
synonym/profanity JSON files per test. Its duplicate-drop test writes Bronze objects to the test
MinIO (`minio_bucket`) and commits `review_master_index` rows; `tests/test_crawl_outcome.py` runs the
same path end to end, cleansing the Bronze partition a retried crawl wrote.

```bash
PYTHONPATH=. uv run pytest tests/test_cleanse.py -q
```
Database-backed tests need the Docker test database, and the duplicate-drop test the test MinIO;
start and connect them as described in `tests/AGENTS.md` (Testing Requirements).

### Common Patterns
Pure functions for each text rule, composed by `ReviewCleaner.clean()`; the pipeline class owns IO
and DB state so the rule layer stays trivially testable.

## Dependencies

### Internal
`config/dictionaries/synonyms.json`, `config/dictionaries/profanity.json`,
`src/models/review_master_index.py`, `src/models/review_preprocessed.py` (ORM; Silver Parquet is
written from plain dicts, not `src/schemas/parquet/`), `src/models/enums.py`,
`src/utils/minio_client.py`, `src/utils/logger.py`.

### External
`emoji`, `flashtext`, `konlpy`, `nltk`, `pyarrow`, `sqlalchemy` (stdlib `re` / `unicodedata` for the
core rules).

<!-- MANUAL: hand-maintained; keep this marker as the last line and add notes to their section (root AGENTS.md) -->
