<!-- Parent: ../AGENTS.md -->
<!-- Generated: 2026-08-10T08:45:46Z | Updated: 2026-08-28T00:00:00Z -->

# src/schemas/parquet

## Purpose
Pydantic schemas for the Parquet files that hold Bronze and Silver review data on MinIO/NAS. They
mirror the corresponding `schema_v4.sql` table structures and are the only validation those files
get, since no database constraint applies to object storage.

## Key Files

| File | Description |
|------|-------------|
| `app_review.py` | `AppReviewSchema` (alias `AppReview`) — Bronze raw review. Full field contract: `review_id`, `app_id`, `platform_type`, `platform_review_id`, `reviewer_name`, `review_text`, `rating`, `reviewed_at`, `created_at`, `is_reply`, `reply_comment`. Mirrors `app_reviews` |
| `review_preprocessed.py` | `ReviewPreprocessedSchema` (alias `ReviewPreprocessed`) — Silver cleansed review: `review_id`, `platform_review_id`, `refined_text`. Mirrors `reviews_preprocessed` |
| `base.py` | Shared helpers: `generate_uuid_v7()` (time-sortable IDs for better index locality), `utc_now()`, `to_utc()` |
| `__init__.py` | Layer-annotated re-exports plus a doctest-style usage example |

## Subdirectories
None.

## For AI Agents

### Working In This Directory
- Field changes are a Parquet format change. Existing objects in MinIO were written with the old
  shape, so adding a required field breaks reads of historical partitions — add optional fields with
  defaults, or plan a rewrite.
- Keep these schemas aligned with `sql/schema_v4.sql` and the ORM models in `src/models/review.py` /
  `src/models/review_preprocessed.py`; the three describe the same data in different media.
- Timestamps must be timezone-aware UTC. Use `utc_now()` / `to_utc()` instead of naive
  `datetime.now()`.
- IDs are UUID v7 via `generate_uuid_v7()` so Bronze partitions stay time-ordered — do not swap in
  `uuid4`.
- Schema class and alias are both exported (`AppReviewSchema` / `AppReview`); keep both when
  renaming, since call sites use each.

### Testing Requirements
```bash
PYTHONPATH=. uv run pytest tests/test_parquet_schemas.py tests/test_parquet_writer.py -q
```
`tests/test_crawler_consistency.py` also depends on this shape being identical across stores.

### Common Patterns
Pydantic v2 `BaseModel` with `ConfigDict`, `Field(default_factory=...)` for generated values, and
`@field_validator` for normalization (for example coercing timestamps to UTC).

## Dependencies

### Internal
`src/crawlers/base_crawler.py` (Bronze writes), `src/processing/cleanse.py` (Silver writes),
`src/loaders/batch_loader.py` (reads), `src/utils/parquet_writer.py`.

### External
`pydantic`, `uuid6`, `pyarrow` (at the writer boundary).

<!-- MANUAL: Notes added below this line are preserved on regeneration -->
