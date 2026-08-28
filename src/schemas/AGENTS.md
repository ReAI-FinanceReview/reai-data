<!-- Parent: ../AGENTS.md -->
<!-- Generated: 2026-08-10T08:45:46Z | Updated: 2026-08-10T08:45:46Z -->

# src/schemas

## Purpose
Pydantic validation layer for data that leaves the database — primarily Parquet payloads written to
MinIO/NAS. Keeping validation here (rather than in the ORM) means Bronze and Silver files are
schema-checked at write time even though no database constraint protects them.

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

<!-- MANUAL: Notes added below this line are preserved on regeneration -->
