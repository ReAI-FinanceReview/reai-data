# Backend Datamart Serving Readiness Runbook

This runbook records the manual release proof for backend-facing datamarts. It
is intentionally separate from PR automation because live crawls depend on
external store/network state and production credentials are out of scope.

## Automated local proof

The CI/local test proof uses Docker PostgreSQL from `docker-compose.test.yml`,
seeds analyzed upstream pipeline tables, invokes the real aggregate CLI
entrypoint, and asserts rows plus backend semantics in:

- `fact_service_review_daily`
- `fact_service_aspect_daily`
- `fact_category_radar_scores`
- `srv_daily_review_list`

Run the full suite before completion:

```bash
PYTHONPATH=. uv run pytest -q
```

Targeted readiness proof:

```bash
PYTHONPATH=. uv run pytest -q tests/test_backend_datamart_serving_readiness.py
```

## Manual live crawl smoke

Start local PostgreSQL and MinIO, then run host-side Python entrypoints:

```bash
docker compose up -d
PYTHONPATH=. uv run python scripts/bootstrap_db.py
PYTHONPATH=. uv run python scripts/crawl_reviews.py
PYTHONPATH=. uv run python scripts/load_reviews.py
PYTHONPATH=. uv run python scripts/cleanse_reviews.py --date YYYY-MM-DD
PYTHONPATH=. uv run python -m src.pipeline.cli --steps aggregate --target-date YYYY-MM-DD
```

Use the date actually produced by the crawl in place of `YYYY-MM-DD`. MinIO
should contain Bronze/Silver Parquet evidence under the configured bucket before
the aggregate step is accepted as release evidence.

## Evidence path

Store manual smoke evidence under:

```text
docs/evidence/backend-datamart-serving-readiness/YYYY-MM-DD.md
```

The evidence file must include:

| Field | Required content |
|---|---|
| `timestamp` | Absolute timestamp with timezone for the smoke run. |
| `crawl command used` | Exact crawl command and config target. |
| `source/service target` | Store/platform app or service target used. |
| `MinIO evidence` | Bucket/key prefix for produced Bronze and Silver objects. |
| `fact_service_review_daily` | Row count for the target date. |
| `fact_service_aspect_daily` | Row count for the target date. |
| `fact_category_radar_scores` | Row count for the target date. |
| `srv_daily_review_list` | Row count for the target date. |
| `failures or caveats` | External flakiness, empty crawl caveats, or `none`. |

Suggested SQL for counts:

```sql
SELECT 'fact_service_review_daily' AS table_name, COUNT(*) FROM fact_service_review_daily WHERE date = DATE 'YYYY-MM-DD'
UNION ALL
SELECT 'fact_service_aspect_daily', COUNT(*) FROM fact_service_aspect_daily WHERE date = DATE 'YYYY-MM-DD'
UNION ALL
SELECT 'fact_category_radar_scores', COUNT(*) FROM fact_category_radar_scores WHERE date = DATE 'YYYY-MM-DD'
UNION ALL
SELECT 'srv_daily_review_list', COUNT(*) FROM srv_daily_review_list WHERE date = DATE 'YYYY-MM-DD';
```
