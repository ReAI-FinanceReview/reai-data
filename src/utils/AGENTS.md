<!-- Parent: ../AGENTS.md -->
<!-- Generated: 2026-08-10T08:45:46Z | Updated: 2026-08-28T00:00:00Z -->

# src/utils

## Purpose
Shared infrastructure used by every pipeline stage: logging, database sessions, object storage,
Parquet IO, and environment-aware path resolution. Nothing here knows about reviews — these modules
stay domain-agnostic so stage code can depend on them without coupling.

## Key Files

| File | Description |
|------|-------------|
| `logger.py` | Singleton `Logger` loading `config/logging_config.yml`; creates `logs/{crawler,error,debug}` and falls back to `basicConfig` if the YAML fails. Public entry: `get_logger(name)` |
| `db_connector.py` | `DatabaseConnector(config_path)` — builds the SQLAlchemy engine from `DATABASE_URL`, exposes `get_session()`, `get_autocommit_connection()`, `create_tables(base)` |
| `minio_client.py` | `MinIOClient` (boto3 S3): `list_objects`, `get_parquet`, `put_parquet`, `delete_object`. Works against MinIO or native AWS S3 depending on `MINIO_ENDPOINT` |
| `parquet_writer.py` | `ParquetWriter`: `write_batch`, `write_single`, `append_to_partition`, `list_partitions`, `get_partition_stats`, plus `read_parquet_to_schemas()` for reading back into Pydantic models |
| `path_resolver.py` | `PathResolver` with `${VARIABLE}` substitution over `config/paths.yml`; helpers `get_resolver()`, `resolve_path()`, `get_medallion_paths()` |
| `file_manager.py` | Local filesystem helper (`ensure_directories`, `get_output_path`, `save_reviews`, `backup_file`, `list_files`, `cleanup_old_files`) for the legacy CSV output path |
| `data_processor.py` | `DataProcessor` static methods over plain dicts and DataFrames: `flatten_entry`, `normalize_appstore_review`, `normalize_playstore_review`, `create_unified_dataframe`, `clean_text`, `extract_app_info`. It imports `Review` for typing only and never builds ORM rows |
| `__init__.py` | Package version marker only (`1.0.0`) — import modules directly |

## Subdirectories
None.

## For AI Agents

### Working In This Directory
- These modules must not import from `src/crawlers/`, `src/gold/`, `src/loaders/`, or
  `src/processing/`. Utilities depend downward only (`src/models/`, `src/schemas/`).
- Paths come from `config/paths.yml` through `PathResolver`, driven by `PARQUET_BASE_PATH`. Do not
  hardcode `data/`, `/mnt/nas/...`, or bucket prefixes at call sites.
- `MINIO_ENDPOINT` selects the backend: `host:9000` (no scheme) targets MinIO, unset/empty targets
  native AWS S3 with IAM credentials. `MINIO_USE_SSL` must be `true` in production.
- `logger.py` reads `config/logging_config.yml`, not `config/logging.yml`; the latter is an
  unreferenced duplicate.
- The logger is a module-level singleton instantiated at import time and creates log directories as
  a side effect — keep that in mind when writing tests that assert on the filesystem.
- `file_manager.py` and `data_processor.py` serve the older CSV/DataFrame path; new work should go
  through `parquet_writer.py` + `minio_client.py`.

### Testing Requirements
```bash
PYTHONPATH=. uv run pytest tests/test_path_resolver.py tests/test_parquet_writer.py tests/test_minio_client.py -q
```
`tests/conftest.py` provides `temp_dir`, `temp_parquet_dir`, and `temp_bronze_dir` so tests write to
throwaway directories instead of the configured medallion paths.

### Common Patterns
- `get_logger(__name__)` at module scope in every consumer.
- Sessions are opened by the caller and closed in `finally`; `DatabaseConnector` does not manage
  transaction scope for you.
- Environment reads use `os.getenv(NAME, default)` with the default documented in `.env.example`.

## Dependencies

### Internal
`config/logging_config.yml`, `config/paths.yml`, `src/schemas/parquet/`, `src/models/review.py`.

### External
`sqlalchemy`, `psycopg2-binary`, `boto3`, `pyarrow`, `pandas`, `pyyaml`, `python-dotenv`.

<!-- MANUAL: Notes added below this line are preserved on regeneration -->
