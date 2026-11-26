<!-- Parent: ../AGENTS.md -->
<!-- Generated: 2026-08-10T08:45:46Z | Updated: 2026-11-26T00:00:00Z -->

# src/schemas

## Purpose
Pydantic validation layer for Parquet payloads on MinIO/NAS. Only Bronze is actually validated:
the crawlers build `AppReviewSchema` records before writing (`_build_parquet_records` in
`src/crawlers/appstore_crawler.py` and `src/crawlers/playstore_crawler.py`) and the loader re-validates them on read
(`src/loaders/batch_loader.py:127`). Silver is written straight from dicts
(`src/processing/cleanse.py:203`) and no production code uses `ReviewPreprocessedSchema`.

## Key Files

| File | Description |
|------|-------------|
| `__init__.py` | Convenience re-exports of `AppReviewSchema` and `ReviewPreprocessedSchema` |

## Subdirectories

| Directory | Purpose |
|-----------|---------|
| `parquet/` | Bronze/Silver Parquet schemas and shared UUID/timestamp helpers (see `parquet/AGENTS.md`) |

## For AI Agents

### Working In This Directory
- This package validates *file* payloads; database structure is owned by `src/models/` and
  `sql/schema_v4.sql`. A field added to one must be mirrored in the other deliberately.
- Keep the top-level re-exports in sync when adding a schema, since call sites use
  `from src.schemas import ...` as well as the fully qualified path.

### Testing Requirements
```bash
PYTHONPATH=. uv run pytest tests/test_parquet_schemas.py -q
```

### Common Patterns
Pydantic v2 `BaseModel` with `ConfigDict`, `Field`, and `@field_validator`; defaults are produced by
factory helpers (`generate_uuid_v7`, `utc_now`) rather than mutable literals.

## Dependencies

### Internal
Used by `src/crawlers/`, `src/processing/`, `src/loaders/`, and `src/utils/parquet_writer.py`.

### External
`pydantic`, `uuid6`.

<!-- MANUAL: hand-maintained; keep this marker as the last line and add notes to their section (root AGENTS.md) -->
