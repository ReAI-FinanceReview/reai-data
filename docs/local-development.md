# Local Development Stack

This project runs its local infrastructure with Docker Compose using PostgreSQL and MinIO only. Python entrypoints run from the host with `uv run ...`.

## Start the stack

```bash
docker compose up -d
```

Services:

- PostgreSQL: `localhost:5432`
- MinIO API: `localhost:9000`
- MinIO Console: `http://localhost:9001`

The `minio-init` one-shot service creates the `reai-data` bucket automatically.

## Configure the app

Create a local env file from `.env.local.example`.

```bash
cp .env.local.example .env
uv sync
```

Key values in `.env.local.example`:

- `DATABASE_URL=postgresql+psycopg2://reai:reai@localhost:5432/reai`
- `MINIO_ENDPOINT=localhost:9000`
- `MINIO_BUCKET=reai-data`

## Bootstrap the database

Use the bootstrap command instead of manually applying `schema_v4.sql`, `app_service_data.sql`, `apps_data.sql`, and `app_metadata_data.sql`.

```bash
PYTHONPATH=. uv run python scripts/bootstrap_db.py
```

This command is intentionally local-development oriented:

- it resets the `public` schema
- reapplies the immutable `schema_v4.sql` Alembic baseline
- stamps the Alembic baseline revision and applies migrations to `head`
- loads required reference seed data from `app_service_data.sql`, `apps_data.sql`, and `app_metadata_data.sql`
- verifies the expected seed counts before returning success

Schema migration commands and reference-data ownership are documented in `docs/schema-management.md`.

## Run the minimum ETL flow

```bash
PYTHONPATH=. uv run python scripts/crawl_reviews.py
PYTHONPATH=. uv run python scripts/load_reviews.py
PYTHONPATH=. uv run python scripts/cleanse_reviews.py --date YYYY-MM-DD
```

Notes:

- `crawl_reviews.py` uploads Bronze Parquet batches to MinIO and registers `ingestion_batch` rows.
- `load_reviews.py` consumes those pending batches into PostgreSQL.
- Replace `YYYY-MM-DD` with the Bronze partition date you actually crawled, or use a shell expression such as `$(date -I)` for today's partition.
- Gold analyze/aggregate steps require a real `OPENAI_API_KEY`.

## Prove backend datamart serving readiness

After the minimum ETL flow and Gold analysis have completed for a date, run the
real pipeline entrypoint for the backend-facing mart step:

```bash
PYTHONPATH=. uv run python scripts/run_pipeline.py --steps gold,aggregate --target-date YYYY-MM-DD
```

For a PR-safe PostgreSQL proof that does not require live store or LLM access,
run the focused contract suite against the Docker test database:

```bash
docker compose -f docker-compose.test.yml up -d test-postgres
TEST_DATABASE_URL="${TEST_DATABASE_URL:-postgresql://testuser:testpass@localhost:5433/testdb}" \
PYTHONPATH=. uv run pytest tests/test_backend_datamart_contract.py -q
```

This suite seeds upstream analyzed review rows, invokes the real `aggregate`
step dispatcher, and asserts generated rows plus backend-facing semantics in:

- `fact_service_review_daily`
- `fact_service_aspect_daily`
- `fact_category_radar_scores`
- `srv_daily_review_list`

## Manual live crawl smoke evidence

For release evidence, keep live crawl proof separate from PR automation because
store responses and LLM credentials can be externally flaky. Record:

- crawl command used
- source/service target
- timestamp
- `target_date`
- row counts for all four backend-facing mart tables
- failures or caveats

Use this SQL shape for the row-count evidence:

```sql
SELECT 'fact_service_review_daily' AS table_name, COUNT(*) FROM fact_service_review_daily WHERE date = :target_date
UNION ALL
SELECT 'fact_service_aspect_daily', COUNT(*) FROM fact_service_aspect_daily WHERE date = :target_date
UNION ALL
SELECT 'fact_category_radar_scores', COUNT(*) FROM fact_category_radar_scores WHERE date = :target_date
UNION ALL
SELECT 'srv_daily_review_list', COUNT(*) FROM srv_daily_review_list WHERE date = :target_date;
```

## Verify results

- DataGrip / PostgreSQL:
  - `select count(*) from ingestion_batch;`
  - `select processing_status, count(*) from review_master_index group by 1 order by 1;`
- MinIO Console:
  - Bronze objects under `bronze/app_reviews/...`
  - Silver objects under `silver/reviews/...`

## Stop the stack

```bash
docker compose down
```

To remove persisted local data as well:

```bash
docker compose down -v
```

## Scope note

Airflow is intentionally out of scope for this compose file. Add it separately once the local infra-only workflow is stable.
