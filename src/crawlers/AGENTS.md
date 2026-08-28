<!-- Parent: ../AGENTS.md -->
<!-- Generated: 2026-08-10T08:45:46Z | Updated: 2026-08-28T00:00:00Z -->

# src/crawlers

## Purpose
Bronze-layer ingestion. Each crawler pulls reviews for its store's app-id list, converts the raw API
payload into `AppReviewSchema` records, writes them as Parquet to MinIO, and registers an
`ingestion_batch` row in `PENDING`. Crawlers never write `review_master_index` — that is the load
stage's job (`src/loaders/`), which keeps crawl failures isolated from DB state.

## Key Files

| File | Description |
|------|-------------|
| `base_crawler.py` | `BaseCrawler(ABC)`: config/app-id loading, request pacing, `collect_app_records()`, `save_crawl_batch()`, `save_daily_batch()`, `run()` template. Owns Parquet format consistency for all stores |
| `appstore_crawler.py` | `AppStoreCrawler`: App Store RSS/API via `requests`; app-id file from `config.app_ids.appstore` |
| `playstore_crawler.py` | `PlayStoreCrawler`: `google_play_scraper.reviews` + `app`; app-id file from `config.app_ids.playstore` |
| `unified_crawler.py` | `UnifiedCrawler`: runs both stores sequentially, catching each store's failure so one store cannot abort the other. Deliberately **not** a `BaseCrawler` subclass |
| `exceptions.py` | `ParquetWriteError` — Parquet write failure during the crawl stage |
| `__init__.py` | `Store` enum (`appstore` / `playstore` / `unified`) and the `get_crawler(store, config_path)` factory |

## Subdirectories
None.

## For AI Agents

### Working In This Directory
- New stores subclass `BaseCrawler` and implement `crawl_reviews(app_id)`; register them in the
  `Store` enum and `get_crawler()`. Parquet schema consistency comes from the base class — do not
  write Parquet directly from a store crawler.
- `UnifiedCrawler` is an orchestrator, not a crawler. Keep it out of the `BaseCrawler` hierarchy.
- Parquet upload plus `ingestion_batch` registration can be disabled with `ENABLE_PARQUET_WRITE=false`
  (see `.env.example`); code paths must tolerate that flag.
- Request pacing comes from `crawler_config.yml` `global.delay_between_requests` /
  `max_retries` / `timeout`. Never hardcode sleeps or retry counts.
- A `ParquetWriteError` must fail the crawl for that batch rather than silently continuing — a
  registered batch with no Parquet object would strand the loader.
- Logs in this package are Korean and use emoji markers (🍎 / 🤖 / ✅ / ❌); match that style.

### Testing Requirements
`tests/test_crawler_consistency.py` asserts App Store and Play Store crawlers emit structurally
identical records; `tests/test_bronze_loading.py` covers the Parquet → DB handoff.
`tests/conftest.py` provides `mock_appstore_api` (requests_mock) and `mock_playstore_api`
(monkeypatched scraper) plus `sample_app_id_file` — use them instead of hitting live stores.

```bash
TEST_DATABASE_URL="${TEST_DATABASE_URL:-postgresql://testuser:testpass@localhost:5433/testdb}" \
  PYTHONPATH=. uv run pytest tests/test_crawler_consistency.py tests/test_bronze_loading.py -q
```

### Common Patterns
- App-id files are read with `read_app_ids()`, which skips blank lines and `#` comments.
- Review IDs are UUID v7 (`uuid6.uuid7`) so Bronze partitions stay time-ordered.
- Timestamps are timezone-aware UTC (`datetime.now(timezone.utc)`).

## Dependencies

### Internal
`src/utils/logger.py`, `src/utils/db_connector.py`, `src/utils/minio_client.py`,
`src/utils/parquet_writer.py`, `src/schemas/parquet/app_review.py`,
`src/models/ingestion_batch.py`, `config/app_ids/`, `config/crawler_config.yml`.

### External
`requests`, `google-play-scraper`, `pyarrow`, `boto3`, `uuid6`, `pyyaml`.

<!-- MANUAL: Notes added below this line are preserved on regeneration -->
