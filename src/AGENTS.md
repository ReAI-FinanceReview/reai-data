<!-- Parent: ../AGENTS.md -->
<!-- Generated: 2026-08-10T08:45:46Z | Updated: 2026-11-28T00:00:00Z -->

# src

## Purpose
All application code for the review ETL pipeline, split by medallion stage rather than by technical
layer. Data flows `crawlers/` (Bronze Parquet + batch registration) → `loaders/` (Parquet → DB
index) → `processing/` (Bronze → Silver cleansing) → `gold/` (embedding, ABSA, actionability,
aggregation) with `pipeline/` providing the step dispatcher, CLI, and post-aggregate validation.
`models/`, `schemas/`, and `utils/` are shared infrastructure used by every stage.

## Key Files

| File | Description |
|------|-------------|
| `bootstrap_db.py` | Local DB bootstrap: reset `public` schema, apply `sql/schema_v4.sql`, stamp `20260430_0001` and upgrade to head, then load seed SQL and verify seed counts (39/65/61) — the order `test_bootstrap_runs_migrations_before_seed_sql` in `tests/test_bootstrap_db.py` pins. Refuses non-local hosts, and, when `DATABASE_URL` comes from `.env`/the shell, a URL port that disagrees with `POSTGRES_PORT` (`validate_published_port`) |

## Subdirectories

| Directory | Purpose |
|-----------|---------|
| `crawlers/` | App Store / Play Store crawling → Bronze Parquet + `ingestion_batch` PENDING, plus the one-off `app_reviews.app_version` backfill (see `crawlers/AGENTS.md`) |
| `loaders/` | Pending Parquet batches → `review_master_index` RAW rows (see `loaders/AGENTS.md`) |
| `processing/` | Bronze → Silver text cleansing pipeline (see `processing/AGENTS.md`) |
| `gold/` | Embedding, ABSA, action analysis, orchestration, and daily aggregation (see `gold/AGENTS.md`) |
| `pipeline/` | Step wrappers, argparse CLI, failure queries, post-aggregate validation (see `pipeline/AGENTS.md`) |
| `models/` | SQLAlchemy ORM models mirroring schema v4 (see `models/AGENTS.md`) |
| `schemas/` | Pydantic schemas validating Parquet payloads (see `schemas/AGENTS.md`) |
| `utils/` | Logger, DB connector, MinIO client, Parquet writer, path resolver (see `utils/AGENTS.md`) |

## For AI Agents

### Working In This Directory
- `src/` has no `__init__.py`; it is imported as a namespace package with the repo root on
  `sys.path` (`PYTHONPATH=.`). Do not add a top-level `src/__init__.py` without checking every
  entrypoint.
- Import style is mixed: `src/crawlers/*` uses relative imports (`..utils.logger`), most newer code
  uses absolute (`from src.utils.logger import get_logger`). Match the file you are editing.
- Heavy dependencies (torch, transformers, snorkel, openai) are imported lazily inside
  `src/pipeline/steps.py` functions and `GoldOrchestrator.__init__` so unrelated steps stay
  importable when those packages are missing. Preserve that laziness.
- Docstrings and log messages are Korean in the crawler/gold/processing code and English in the
  pipeline/bootstrap code. Follow the surrounding file.

### Testing Requirements
Each subpackage has matching tests under `tests/` (e.g. `src/gold/aggregator.py` ↔
`tests/test_gold_aggregator.py`). Most require a live PostgreSQL:

```bash
PYTHONPATH=. uv run pytest tests/test_gold_aggregator.py -q
```
Database-backed tests need the Docker test database; start and connect it as described in
`tests/AGENTS.md` (Testing Requirements).

### Common Patterns
- Constructors accept `config_path` (default `"config/crawler_config.yml"`) and instantiate
  `DatabaseConnector(config_path)` themselves — there is no DI container.
- Session handling is explicit: `session = self.db_connector.get_session()` inside
  `try/except → rollback / finally → close`.
- Batch processors expose both `process(session, review_id) -> bool` (single record, used by the
  orchestrator) and `process_batch(batch_size, limit)` (standalone entry).

## Dependencies

### Internal
`config/` YAML for runtime settings, `sql/` + `alembic/` for the schema these models mirror.

### External
`sqlalchemy`, `pydantic`, `pyarrow`, `boto3`, `pandas`, `openai`, `sentence-transformers`,
`snorkel`, `konlpy`, `uuid6`.

<!-- MANUAL: hand-maintained; keep this marker as the last line and add notes to their section (root AGENTS.md) -->
