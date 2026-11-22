<!-- Parent: ../AGENTS.md -->
<!-- Generated: 2026-08-10T08:45:46Z | Updated: 2026-11-22T00:00:00Z -->

# tests

## Purpose
Flat pytest suite covering every pipeline stage. Tests that use the database fixtures run against a
**real PostgreSQL** database — there is no SQLite fallback, and the schema is initialized from
`sql/schema_v4.sql` so pgvector, enums, partitions, and constraints are all exercised. Many unit
tests do replace collaborators with `MagicMock` (DB sessions in `test_gold_orchestrator.py`, MinIO
and the DB connector in `test_crawler_consistency.py`, boto3 in `test_minio_client.py`), so a
passing mock-based test says nothing about the real database. The HTTP stub fixtures in
`conftest.py` are unused.

## Key Files

| File | Description |
|------|-------------|
| `conftest.py` | Shared infrastructure: session-scoped engine/schema, function-scoped rollback session, temp Parquet dirs, sample review payloads, pre-populated DB states, API stubs, marker registration |
| `test_backend_datamart_contract.py` | Backend-facing mart contract for the four Gold tables |
| `test_backend_datamart_serving_readiness.py` | Serving-readiness evidence checks for the mart |
| `test_post_aggregate_validation.py` | `PostAggregateValidator` checks, severities, and report shape |
| `test_cleanse.py` | Bronze → Silver cleansing rules, dictionaries, PII masking |
| `test_gold_absa_analyzer.py` | Keyword/sentiment/category extraction, negation, embedding fallback |
| `test_gold_action_analyzer.py` | Attention/action rules, Snorkel labeling functions, LLM summary |
| `test_gold_aggregator.py` | Fact table + serving mart UPSERT semantics |
| `test_gold_embedding_generator.py` | Embedding generation and persistence |
| `test_gold_orchestrator.py` | CLEANED → ANALYZED/FAILED state machine and retry policy |
| `test_gold_dept_assigner.py` | Rule and LLM department assigners, org-path expansion, tie-breaking |
| `test_assign_dept_cli.py` / `test_eval_assignment_cli.py` | Argument contracts for `scripts/assign_dept.py` and `scripts/eval_assignment.py` |
| `test_batch_loader.py` | Pending/failed batch consumption into `review_master_index` |
| `test_bronze_loading.py` | Bronze Parquet → DB load integration |
| `test_crawler_consistency.py` | `save_daily_batch` upload → PENDING registration order and failure cleanup (App Store crawler only; no cross-store parity check) |
| `test_database_schema.py` | Schema objects created by `schema_v4.sql` |
| `test_parquet_schemas.py` / `test_parquet_writer.py` | Pydantic Parquet schemas and writer partitioning |
| `test_path_resolver.py` | `${VAR}` substitution and medallion path resolution |
| `test_minio_client.py` | MinIO/S3 wrapper behavior |
| `test_pipeline_steps.py` / `test_pipeline_failures.py` / `test_pipeline_integration.py` | Step dispatch, dead-letter queries, end-to-end wiring |
| `test_cli_parsing.py` / `test_pipeline_cli_validation.py` | argparse contract and `--target-date` validation |
| `test_bootstrap_db.py` / `test_alembic_config.py` / `test_local_dev_setup.py` / `test_ci_workflows.py` | Bootstrap, migration config, compose files, and GitHub workflow contract |
| `test_airflow_dag_validation_wiring.py` / `test_airflow_readiness_docs.py` | DAG wiring and readiness-doc contract |
| `test_enums.py` | Central ENUM definitions |
| `test_agents_docs.py` | Guards for this AGENTS.md tree: ignore rules, headers, parent links, citation line ranges, and per-directory coverage |

## Subdirectories

| Directory | Purpose |
|-----------|---------|
| `fixtures/` | Checked-in sample inputs — currently `dept_labels.sample.csv` for the assignment evaluator. Data-only, so it deliberately has no `AGENTS.md` of its own |

The test modules themselves stay flat: one `test_<module>.py` per source module.

## For AI Agents

### Working In This Directory
- `test_db_url` (`tests/conftest.py:58-60`) uses `TEST_DATABASE_URL` when it is set; otherwise it
  connects to the `docker-compose.test.yml` database on `localhost:${TEST_POSTGRES_PORT:-5433}`.
  The schema fixture runs `DROP SCHEMA public CASCADE` — **never** point it at a production database.
- The image must be `pgvector/pgvector:pg17` (or equivalent); `schema_v4.sql` requires the `vector`
  extension.
- `test_db_session` is function-scoped and rolls back, so tests must not rely on committed state
  from a previous test; use `db_with_apps`, `db_with_failed_reviews`, `db_with_pending_batches`, or
  `db_with_failed_batches` to seed.
- Do not introduce database mocks or in-memory substitutes to make a test pass — the suite's value
  is that it runs against the real schema.
- There is no `[tool.pytest.ini_options]` section in `pyproject.toml`; markers are registered in
  `pytest_configure` at `conftest.py:662`.

### Testing Requirements
This is the one canonical way to run the suite; other `AGENTS.md` files only name the modules to
pass to the same `pytest` command.

```bash
docker compose -f docker-compose.test.yml up -d test-postgres
PYTHONPATH=. uv run pytest            # full suite
PYTHONPATH=. uv run pytest -m "not requires_db"   # NOT DB-free, see below
```
Leave `TEST_DATABASE_URL` unset for the Docker test database. The compose port binding
(`docker-compose.test.yml:10`) and the fallback URL in `test_db_url` both read `TEST_POSTGRES_PORT`
(default 5433), so if that port is taken, export `TEST_POSTGRES_PORT` once for both commands rather
than editing a URL. Set `TEST_DATABASE_URL` only to target a different database; it takes precedence.

Markers: `slow`, `integration`, `requires_db`. The marker is not applied to every test that uses the
database fixtures, so `-m "not requires_db"` still selects some of them, and two
(`tests/test_bootstrap_db.py:123` and `:180`) run `DROP SCHEMA public CASCADE` on whatever database
`test_db_url` reaches. To run it without touching any database, point `TEST_DATABASE_URL` at an
unreachable address; those two tests then skip.

### Common Patterns
- `@pytest.mark.requires_db` on anything touching PostgreSQL (not yet applied consistently; see
  Testing Requirements).
- Fixtures return live `Session` objects; assertions query through the ORM models in `src/models/`.
- Sample crawler payloads mirror the real upstream response shapes (App Store RSS entries,
  google-play-scraper dicts) so schema drift is caught at the boundary.

## Dependencies

### Internal
`src/models/`, `src/schemas/parquet/`, `sql/schema_v4.sql`, `docker-compose.test.yml`,
`config/dictionaries/`.

### External
`pytest`, `sqlalchemy`, `uuid6`, `pyarrow`. `mock_appstore_api` in `conftest.py` expects the
requests-mock plugin, which is not a dependency (see `src/crawlers/AGENTS.md`).

<!-- MANUAL: Notes added below this line are preserved on regeneration -->
