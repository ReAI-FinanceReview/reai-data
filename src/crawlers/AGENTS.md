<!-- Parent: ../AGENTS.md -->
<!-- Generated: 2026-08-10T08:45:46Z | Updated: 2026-11-26T00:00:00Z -->

# src/crawlers

## Purpose
Bronze-layer ingestion. Each crawler pulls reviews for its store's app-id list, converts the raw API
payload into `AppReviewSchema` records, writes them as Parquet to MinIO, and registers an
`ingestion_batch` row in `PENDING`. Crawlers never write `review_master_index` — that is the load
stage's job (`src/loaders/`), which keeps crawl failures isolated from DB state.
`app_version_backfill.py` is a one-off maintenance module, not a crawler: it re-fetches reviews
already in `app_reviews` and UPDATEs only their NULL `app_version`, plus `updated_at` (issue #125).

## Key Files

| File | Description |
|------|-------------|
| `base_crawler.py` | `BaseCrawler(ABC)`: config/app-id loading, request pacing, `collect_app_records()`, `save_crawl_batch()`, `save_daily_batch()`, `run()` template. Owns Parquet format consistency for all stores |
| `appstore_crawler.py` | `AppStoreCrawler`: App Store RSS/API via `requests` (URL template from `appstore.reviews_rss_url`, no coded default); app-id file from `config.app_ids.appstore`. Record `app_version` comes from the entry's `im:version.label` via the static `_extract_app_version` |
| `playstore_crawler.py` | `PlayStoreCrawler`: `google_play_scraper.reviews` + `app`; app-id file from `config.app_ids.playstore`. Record `app_version` comes from `reviewCreatedVersion` via the static `_extract_app_version` (google-play-scraper fills `appVersion` from the same payload element), `None` when the store omits it |
| `unified_crawler.py` | `UnifiedCrawler`: runs both stores sequentially, catching each store's failure so one store cannot abort the other. Deliberately **not** a `BaseCrawler` subclass |
| `app_version_backfill.py` | `backfill_app_versions()`: groups `app_reviews` rows with `app_version IS NULL` by app (`load_targets`, App Store first), re-fetches each app (`fetch_appstore_versions` / `fetch_playstore_versions`), and writes or, in dry-run, counts the matches (`settle_app`, `UPDATE ... SET app_version, updated_at = NOW() ... AND app_version IS NULL`). Misses are split into `unmatched_confirmed` (the store's answer was read in full: `appstore_feed_complete`, or a Play stop in `PLAY_COMPLETE_STOPS`) and `unverified`. Play paging decisions live in `scan_play_page` / `play_stop_reason`. Settings come from `crawler_config.yml` (`load_settings`); `format_report` renders the per-app and per-platform report. Entry point: `scripts/backfill_app_version.py` |
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
- Store field paths live once, in each store crawler's static `_extract_platform_review_id` /
  `_extract_app_version`, and the RSS URL template once, in `appstore.reviews_rss_url`. The
  record builders and `app_version_backfill.match_versions` both call the extractors, and both
  App Store fetchers read that key; change a field there, not in a copy. The
  backfill calls them on the classes without an instance, because constructing a store crawler
  creates the `output.base_directory` folders (`BaseCrawler.__init__`) and a `DatabaseConnector`.
- The App Store RSS can answer 200 with no entries for pages inside the range (on 2026-11-26 one
  app returned pages 1-2 and 5-10 empty and 3-4 full; a full local dry-run that day got 325
  empty pages out of 340).
  `fetch_appstore_versions` counts such pages as `empty_pages` and keeps reading; an all-empty
  read is `feed_empty`. `appstore_feed_complete` confirms misses only when the pages are full up
  to the last non-empty one and, if empty pages follow, that one is partial
  (`appstore.rss_page_size`), which is how the feed ends for small apps. After
  `appstore.backfill_max_consecutive_empty_feeds` `feed_empty` apps in a row the remaining App
  Store apps are skipped (`skipped_after_empty_feeds`).
  `AppStoreCrawler.get_app_store_reviews_and_appname` still stops at the first empty page.
- `google_play_scraper.reviews()` takes no timeout and swallows request errors, returning an
  empty page with no next-page token. The backfill therefore never treats an empty Play page as
  the end: the first one is `feed_empty`, a later one `empty_page`, both unverified. Only a page
  with reviews and no token (`exhausted`) confirms misses. `global.timeout` does not apply to Play.
- The backfill holds no DB transaction while a store is being asked: it rolls back after
  loading targets and ends each app with a commit (`--apply`) or a rollback (dry-run).
- Logs in this package are Korean and use emoji markers (🍎 / 🤖 / ✅ / ❌); match that style.

### Testing Requirements
`tests/test_crawler_consistency.py` covers `save_daily_batch` only: the MinIO upload happens before
the `ingestion_batch` PENDING row, a failed upload opens no DB session, and a failed commit deletes
the uploaded object. It drives `AppStoreCrawler` with stand-in records, so no test compares App
Store and Play Store output. `tests/test_bronze_loading.py` covers the Parquet → DB handoff and
each store's `app_version` mapping into Bronze. `tests/test_app_version_backfill.py` covers the
backfill: `fetch_appstore_versions` and `backfill_app_versions` run over real HTTP against the
local `appstore_rss_server` (conftest) and against committed rows (`backfill_db`); Play paging
is covered through `scan_play_page` with real-shaped pages, since the rest of its loop is the
live google-play-scraper call. `tests/test_backfill_app_version_cli.py` covers the script.
`tests/conftest.py` still defines `mock_appstore_api` and `mock_playstore_api`, but no test uses
them, and `mock_appstore_api` cannot run: it depends on the `requests_mock` fixture of the
requests-mock plugin, which is in neither `pyproject.toml` nor `uv.lock`. Using it means adding that
dev dependency first.

```bash
PYTHONPATH=. uv run pytest tests/test_crawler_consistency.py tests/test_bronze_loading.py \
  tests/test_app_version_backfill.py tests/test_backfill_app_version_cli.py -q
```
Database-backed tests need the Docker test database; start and connect it as described in
`tests/AGENTS.md` (Testing Requirements).

### Common Patterns
- App-id files are read with `read_app_ids()`, which skips blank lines and `#` comments.
- Review IDs are UUID v7 (`uuid6.uuid7`) so Bronze partitions stay time-ordered.
- Timestamps are timezone-aware UTC (`datetime.now(timezone.utc)`).

## Dependencies

### Internal
`src/utils/logger.py`, `src/utils/db_connector.py`, `src/utils/minio_client.py`,
`src/utils/parquet_writer.py`, `src/schemas/parquet/app_review.py`,
`src/models/ingestion_batch.py`, `src/models/enums.py`, `config/app_ids/`,
`config/crawler_config.yml`.

### External
`requests`, `google-play-scraper`, `pyarrow`, `boto3`, `uuid6`, `pyyaml`.

<!-- MANUAL: hand-maintained; keep this marker as the last line and add notes to their section (root AGENTS.md) -->
