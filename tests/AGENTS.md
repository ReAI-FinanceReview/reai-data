<!-- Parent: ../AGENTS.md -->
<!-- Generated: 2026-08-10T08:45:46Z | Updated: 2026-08-10T08:45:46Z -->

# tests

## Purpose
Flat pytest suite covering every pipeline stage against a **real PostgreSQL** database. There is no
SQLite fallback and no mocked database layer — the schema is initialized from `sql/schema_v4.sql`
so pgvector, enums, partitions, and constraints are all exercised. Only external HTTP APIs
(App Store RSS, google-play-scraper) are stubbed.

## Key Files
| File | Description |
|------|-------------|
| `conftest.py` | Shared infrastructure: session-scoped engine/schema, function-scoped rollback session, temp Parquet dirs, sample review payloads, pre-populated DB states, API stubs, marker registration |
| `test_backend_datamart_contract.py` | Backend-facing mart contract for the four Gold tables (largest suite) |
| `test_backend_datamart_serving_readiness.py` | Serving-readiness evidence checks for the mart |
| `test_post_aggregate_validation.py` | `PostAggregateValidator` checks, severities, and report shape |
| `test_cleanse.py` | Bronze → Silver cleansing rules, dictionaries, PII masking |
| `test_gold_absa_analyzer.py` | Keyword/sentiment/category extraction, negation, embedding fallback |
| `test_gold_action_analyzer.py` | Attention/action rules, Snorkel labeling functions, LLM summary |
| `test_gold_aggregator.py` | Fact table + serving mart UPSERT semantics |
| `test_gold_embedding_generator.py` | Embedding generation and persistence |
| `test_gold_orchestrator.py` | CLEANED → ANALYZED/FAILED state machine and retry policy |
| `test_batch_loader.py` | Pending/failed batch consumption into `review_master_index` |
| `test_bronze_loading.py` | Bronze Parquet → DB load integration |
| `test_crawler_consistency.py` | App Store vs Play Store crawler output parity |
| `test_database_schema.py` | Schema objects created by `schema_v4.sql` |
| `test_parquet_schemas.py` / `test_parquet_writer.py` | Pydantic Parquet schemas and writer partitioning |
| `test_path_resolver.py` | `${VAR}` substitution and medallion path resolution |
| `test_minio_client.py` | MinIO/S3 wrapper behavior |
| `test_pipeline_steps.py` / `test_pipeline_failures.py` / `test_pipeline_integration.py` | Step dispatch, dead-letter queries, end-to-end wiring |
| `test_cli_parsing.py` / `test_pipeline_cli_validation.py` | argparse contract and `--target-date` validation |
| `test_bootstrap_db.py` / `test_alembic_config.py` / `test_local_dev_setup.py` / `test_ci_workflows.py` | Bootstrap, migration config, compose files, and GitHub workflow contract |
| `test_airflow_dag_validation_wiring.py` / `test_airflow_readiness_docs.py` | DAG wiring and readiness-doc contract |
| `test_enums.py` | Central ENUM definitions |

## Subdirectories
None — the suite is intentionally flat, one `test_<module>.py` per source module.

## For AI Agents

### Working In This Directory
- `test_db_url` resolves `TEST_DATABASE_URL`, defaulting to
  `postgresql://testuser:testpass@localhost:${TEST_POSTGRES_PORT:-5433}/testdb`. The schema fixture
  runs `DROP SCHEMA public CASCADE` — **never** point it at a production database.
- The image must be `pgvector/pgvector:pg17` (or equivalent); `schema_v4.sql` requires the `vector`
  extension.
- `test_db_session` is function-scoped and rolls back, so tests must not rely on committed state
  from a previous test; use `db_with_apps`, `db_with_failed_reviews`, `db_with_pending_batches`, or
  `db_with_failed_batches` to seed.
- Do not introduce database mocks or in-memory substitutes to make a test pass — the suite's value
  is that it runs against the real schema.
- There is no `[tool.pytest.ini_options]` section in `pyproject.toml`; markers are registered in
  `pytest_configure` at `conftest.py:619`.

### Testing Requirements
```bash
docker compose -f docker-compose.test.yml up -d test-postgres
TEST_DATABASE_URL="postgresql://testuser:testpass@localhost:5433/testdb" \
  PYTHONPATH=. uv run pytest            # full suite
PYTHONPATH=. uv run pytest -m "not requires_db"   # DB-free subset
```
Markers: `slow`, `integration`, `requires_db`.

### Common Patterns
- `@pytest.mark.requires_db` on anything touching PostgreSQL.
- Fixtures return live `Session` objects; assertions query through the ORM models in `src/models/`.
- Sample crawler payloads mirror the real upstream response shapes (App Store RSS entries,
  google-play-scraper dicts) so schema drift is caught at the boundary.

## Dependencies

### Internal
`src/models/`, `src/schemas/parquet/`, `sql/schema_v4.sql`, `docker-compose.test.yml`,
`config/dictionaries/`.

### External
`pytest`, `sqlalchemy`, `uuid6`, `requests_mock` (used by `mock_appstore_api`), `pyarrow`.

<!-- MANUAL: Notes added below this line are preserved on regeneration -->
