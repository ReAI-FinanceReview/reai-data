# Backend Datamart Serving Readiness Evidence

This directory is the release evidence path for backend datamart serving readiness.
Automated PR checks prove the local Docker PostgreSQL + MinIO integration path with
a deterministic fixture. Live crawl remains a manual smoke proof because external
store/network availability and production credentials are not PR-gate inputs.

## Automated local proof

Start the isolated test services, then run the readiness proof:

```bash
docker compose -f docker-compose.test.yml up -d test-postgres test-minio test-minio-init
PYTHONPATH=. TEST_MINIO_ENDPOINT=localhost:9002 TEST_MINIO_BUCKET=reai-test-data \
  uv run pytest -q tests/test_backend_datamart_serving_readiness.py
```

The test writes and reads a real MinIO Parquet object, invokes the real pipeline
CLI aggregate entrypoint, and asserts generated rows plus semantics for:

- `fact_service_review_daily`
- `fact_service_aspect_daily`
- `fact_category_radar_scores`
- `srv_daily_review_list`

## Manual live crawl smoke

Use this command only in a local release-smoke environment with real non-production
credentials and store/network access:

```bash
PYTHONPATH=. uv run python scripts/bootstrap_db.py
PYTHONPATH=. uv run python scripts/run_pipeline.py \
  --steps crawl,load,gold,aggregate \
  --target-date YYYY-MM-DD \
  --batch-size 100
```

Record each run under this directory with a timestamped markdown file such as
`docs/evidence/backend-datamart-serving-readiness/2026-05-03-live-smoke.md`.
Include:

| Field | Evidence to record |
|---|---|
| Crawl command | Exact command and config file used |
| Source/service target | App/store/service target and environment name |
| Timestamp | Start/end time with timezone |
| MinIO proof | Bucket, object prefix, and object count/path sample |
| Mart counts | Row counts for all four backend-facing mart tables |
| Semantic sample | One representative backend-facing row per mart or query output |
| Caveats | External API/network/key issues, if any |

Suggested SQL for mart counts:

```sql
SELECT 'fact_service_review_daily' AS table_name, COUNT(*) FROM fact_service_review_daily
UNION ALL SELECT 'fact_service_aspect_daily', COUNT(*) FROM fact_service_aspect_daily
UNION ALL SELECT 'fact_category_radar_scores', COUNT(*) FROM fact_category_radar_scores
UNION ALL SELECT 'srv_daily_review_list', COUNT(*) FROM srv_daily_review_list;
```
