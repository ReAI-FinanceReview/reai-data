<!-- Parent: ../AGENTS.md -->
<!-- Generated: 2026-08-10T08:45:46Z | Updated: 2026-11-27T00:00:00Z -->

# src/crawlers

## Purpose
Bronze-layer ingestion. Each crawler pulls reviews for its store's app-id list, converts the raw API
payload into `AppReviewSchema` records, writes them as Parquet to MinIO, and registers an
`ingestion_batch` row in `PENDING`. Crawlers never write `review_master_index` — that is the load
stage's job (`src/loaders/`), which keeps crawl failures isolated from DB state.
Each store crawler's `run()` returns a `StoreCrawlSummary` (issue #127): an outcome per app
(`ok` / `feed_empty` / `gap` / `error`, warnings when not `ok`) and a store-level `failure` that
makes that store's crawl step fail.
`app_version_backfill.py` is a one-off maintenance module, not a crawler: it re-fetches reviews
already in `app_reviews` and UPDATEs only their NULL `app_version`, plus `updated_at` (issue #125).

## Key Files

| File | Description |
|------|-------------|
| `base_crawler.py` | `BaseCrawler(ABC)`: config/app-id loading, request pacing, `collect_app_records()`, `save_crawl_batch()`, `save_daily_batch()`, `run()` template. Owns Parquet format consistency for all stores |
| `appstore_crawler.py` | `AppStoreCrawler`: reads each app through `appstore_rss.read_feed` (`read_reviews`: stops once `appstore.max_reviews_per_app` reviews are in, never at an empty page); app-id file from `config.app_ids.appstore`. `run()` returns a `StoreCrawlSummary` with each app's `read_outcome`, and sets `failure` at the empty-feed cutoff (`appstore.max_consecutive_empty_feeds`). App name: the RSS app-metadata entry's `im:name`, else `app_{app_id}`. Record `app_version` comes from the entry's `im:version.label` via the static `_extract_app_version` |
| `playstore_crawler.py` | `PlayStoreCrawler`: `google_play_scraper.reviews` + `app`; app-id file from `config.app_ids.playstore`. `run()` returns a `StoreCrawlSummary` (`ok` / `feed_empty` / `error` per app) whose `failure` is `playstore_failure`: every app returned zero reviews. Record `app_version` comes from `reviewCreatedVersion` via the static `_extract_app_version` (google-play-scraper fills `appVersion` from the same payload element), `None` when the store omits it |
| `unified_crawler.py` | `UnifiedCrawler(config_path, stores)`: runs the chosen stores in order (`CRAWL_STORE_CHOICES`: `appstore`, `playstore`, `all` = App Store then Play) and returns one `StoreCrawlSummary` each. A store's crawler is built and run inside its own `try`, so its exception becomes that store's `failure` and the next store still runs. Deliberately **not** a `BaseCrawler` subclass |
| `appstore_rss.py` | The one App Store RSS reader, shared by `AppStoreCrawler` and the backfill (issue #127): `read_feed` (URL from the `reviews_rss_url` template; reads up to `pages` without stopping at an empty page; `on_page` may stop it; a failed request ends it with `error` naming the page), `appstore_review_entries` (keeps entries with `im:rating`, dropping the app-metadata entry), `appstore_app_name`, `appstore_feed_complete`, `FeedRead` (`fetched`, `empty_pages`, `feed_empty`) and `read_outcome` (`error` / `feed_empty` / `gap` / `ok`) |
| `crawl_outcome.py` | Crawl outcome types: the `OUTCOME_*` constants, `AppCrawlResult` (per app: `outcome`, `fetched`, `new`, `detail`), `StoreCrawlSummary` (per store: `apps`, `storage_path`, `failure`, `warnings`, `as_dict`) and `playstore_failure` |
| `app_version_backfill.py` | `backfill_app_versions()`: groups `app_reviews` rows with `app_version IS NULL` by app (`load_targets`, App Store first), re-fetches each app (`fetch_appstore_versions`, which reads through `appstore_rss.read_feed`, / `fetch_playstore_versions`), and writes or, in dry-run, counts the matches (`settle_app`, `UPDATE ... SET app_version, updated_at = NOW() ... AND app_version IS NULL`). Misses are split into `unmatched_confirmed` (the store's answer was read in full: `appstore_feed_complete`, or a Play stop in `PLAY_COMPLETE_STOPS`) and `unverified`. Play paging decisions live in `scan_play_page` / `play_stop_reason`. Settings come from `crawler_config.yml` (`load_settings`); `format_report` renders the per-app and per-platform report. Entry point: `scripts/backfill_app_version.py` |
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
  App Store fetchers read the RSS through `appstore_rss.read_feed`, which formats that template;
  change a field or the page handling there, not in a copy. The
  backfill calls them on the classes without an instance, because constructing a store crawler
  creates the `output.base_directory` folders (`BaseCrawler.__init__`) and a `DatabaseConnector`.
- The App Store RSS can answer 200 with no entries for pages inside the range (on 2026-11-26 one
  app returned pages 1-2 and 5-10 empty and 3-4 full; a full local dry-run that day got 325
  empty pages out of 340; from 05:23 UTC every page of every app came back empty).
  `appstore_rss.read_feed` therefore never ends a read at an empty page: it counts it in
  `page_counts` and keeps reading up to `appstore.pages_to_crawl` (or until `on_page` stops it).
  An all-empty read is `feed_empty`. `appstore_feed_complete` accepts a read only when the pages
  are full up to the last non-empty one and, if empty pages follow, that one is partial
  (`appstore.rss_page_size`), which is how the feed ends for small apps.
  - Backfill: `fetch_appstore_versions` confirms misses only for a complete read. After
    `appstore.max_consecutive_empty_feeds` `feed_empty` apps in a row the remaining App Store apps
    are skipped (`skipped_after_empty_feeds`); a failed request does not count, because it already
    makes the run exit 1.
  - Crawl: `AppStoreCrawler.run` records `read_outcome` per app (`gap` when reviews came after an
    empty or partial page). It counts apps that read no review at all, an empty feed or a request
    that failed before the first review, and after `appstore.max_consecutive_empty_feeds` of them
    in a row stops requesting (`skipped_after_empty_feeds`), still uploads what it read, and sets
    `StoreCrawlSummary.failure`.
- A store fails as a whole only when its crawler raises (no app ids, `ParquetWriteError`, the
  database at `create_tables`) or reports `failure` (the App Store cutoff, every Play app empty).
  One app that is empty, has a gap, or fails is a warning: logged at WARNING by the crawler and
  listed in `RunResult.validations[store]['warnings']` by `run_crawl`. A single app such as Hana EZ
  (1472964746), whose page 1 came back empty on the only recorded crawl, must stay a warning.
- `google_play_scraper.reviews()` takes no timeout and swallows request errors, returning an
  empty page with no next-page token. The backfill therefore never treats an empty Play page as
  the end: the first one is `feed_empty`, a later one `empty_page`, both unverified.
  `global.timeout` does not apply to Play.
- A Play page with reviews and no next-page token is not trusted as the end either. In
  google-play-scraper 1.2.7 one `reviews()` call sends further requests itself when a response
  holds fewer reviews than requested together with a token, and if one of those later requests
  fails it returns the reviews collected so far with a `None` token, the same shape as the real
  end. `fetch_playstore_versions` therefore requests that page once more with the same
  continuation token (after `global.delay_between_requests`); `confirm_play_end` merges any
  targets the second answer holds, and `play_end_stop_reason` confirms the end (`exhausted`,
  misses `unmatched_confirmed`) only if both answers carry the same review ids and no token.
  Anything else is `end_not_confirmed` (misses unverified). The one failure still taken for the
  end is one that repeats identically on both requests: the same partial list, twice without a
  token.
- The backfill holds no DB transaction while a store is being asked: it rolls back after
  loading targets and ends each app with a commit (`--apply`) or a rollback (dry-run).
- One app's failure does not end the backfill: an exception in its fetch or write rolls back
  that app only, is logged with `logger.exception`, and becomes an `error` row with every
  target unverified (`try/except → rollback` per app, as `src/AGENTS.md` prescribes). Apps
  committed earlier stay committed; the script then exits 1.
- Logs in this package are Korean and use emoji markers (🍎 / 🤖 / ✅ / ❌); match that style.

### Testing Requirements
`tests/test_appstore_rss.py` covers `read_feed` and `AppStoreCrawler` over real HTTP against the
local `appstore_rss_server` (empty pages read past, `on_page` stop, failed request, app-metadata
entry, `max_reviews_per_app` stop, one empty app as a warning, gap pages, the empty-feed cutoff and
failed requests counting toward it, the count resetting) with Parquet writes off
(`crawl_config`). `tests/test_crawl_outcome.py` covers `playstore_failure` with
google-play-scraper-shaped data, `run_crawl` store selection and per-store failure, the pipeline
CLI crawl step, and an Airflow-style retry after the cutoff on the same day against a real MinIO
bucket, `BatchLoader` and `ReviewCleaningPipeline` (each review loaded once and cleansed once).
`tests/test_crawl_reviews_cli.py` covers `scripts/crawl_reviews.py` exit codes.
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
  tests/test_app_version_backfill.py tests/test_backfill_app_version_cli.py \
  tests/test_appstore_rss.py tests/test_crawl_outcome.py tests/test_crawl_reviews_cli.py -q
```
Database-backed tests need the Docker test database, and the re-run test the test MinIO; start and
connect them as described in `tests/AGENTS.md` (Testing Requirements).

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
