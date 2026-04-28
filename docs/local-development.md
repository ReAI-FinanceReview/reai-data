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
- reapplies `schema_v4.sql`
- loads required reference seed data from `app_service_data.sql`, `apps_data.sql`, and `app_metadata_data.sql`
- verifies the expected seed counts before returning success

## Run the minimum ETL flow

```bash
PYTHONPATH=. uv run python scripts/crawl_reviews.py
PYTHONPATH=. uv run python scripts/load_reviews.py
PYTHONPATH=. uv run python scripts/cleanse_reviews.py --date 2026-04-26
```

Notes:

- `crawl_reviews.py` uploads Bronze Parquet batches to MinIO and registers `ingestion_batch` rows.
- `load_reviews.py` consumes those pending batches into PostgreSQL.
- `cleanse_reviews.py --date ...` must match the Bronze partition date you actually crawled.
- Gold analyze/aggregate steps require a real `OPENAI_API_KEY`.

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
