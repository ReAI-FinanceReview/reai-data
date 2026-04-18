# Local Development Stack

This project can run its local infrastructure with Docker Compose using PostgreSQL and MinIO only.

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

Create a local env file from `.env.local.example` and load it before running scripts from the host environment.

Key values in `.env.local.example`:

- `DATABASE_URL=postgresql+psycopg2://reai:reai@localhost:5432/reai`
- `MINIO_ENDPOINT=localhost:9000`
- `MINIO_BUCKET=reai-data`

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
