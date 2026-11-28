<!-- Parent: ../AGENTS.md -->
<!-- Generated: 2026-08-10T08:45:46Z | Updated: 2026-11-28T00:00:00Z -->

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
| `playstore_crawler.py` | `PlayStoreCrawler`: reads each app through `playstore_reviews.fetch_reviews_page` (`get_playstore_reviews`: up to `playstore.reviews_per_app` newest reviews, at most `playstore.page_size` per request, following continuation tokens until a page has none; `run()` reads through `_read_reviews`, which also returns the `PlayReviewsError` that stopped it, and `get_playstore_reviews` raises it); app-id file from `config.app_ids.playstore`. `run()` returns a `StoreCrawlSummary` (`ok` / `feed_empty` / `error` per app; an app whose request failed is `error`, keeping as records and in `fetched` the reviews read before the failure) whose `failure` is `playstore_failure`: every app returned zero reviews. App name: the catalog `apps` row's name when that row exists, else `app_{app_id}`. Record `app_version` comes from `reviewCreatedVersion` via the static `_extract_app_version`, `None` when the store omits it |
| `playstore_reviews.py` | The one Play reviews client, shared by `PlayStoreCrawler` and the backfill (issue #133), replacing google-play-scraper: `fetch_reviews_page` (one batchexecute POST per page to the `playstore.reviews_url` template with RPC id `playstore.reviews_rpc_id`, through `requests` with TLS verification and `global.timeout`; raises `PlayReviewsError` on failure), `parse_reviews_response` (also raises `PlayReviewsError` for a review without a non-empty string `reviewId`, an int `score` from 1 to 5 and an int epoch `at`, which every live review carried), `review_from_item` (google-play-scraper 1.2.7's payload positions for `reviewId`, `userName`, `score`, `content`, `at`, `thumbsUpCount`, `replyContent`, `repliedAt`, `reviewCreatedVersion`), `PlayPage` (`reviews`, `next_token`) and `play_reviews_settings` / `PlayReviewsSettings` |
| `unified_crawler.py` | `UnifiedCrawler(config_path, stores)`: runs the chosen stores in order (`CRAWL_STORE_CHOICES`: `appstore`, `playstore`, `all` = App Store then Play) and returns one `StoreCrawlSummary` each. A store's crawler is built and run inside its own `try`, so its exception becomes that store's `failure` and the next store still runs. Deliberately **not** a `BaseCrawler` subclass |
| `appstore_rss.py` | The one App Store RSS reader, shared by `AppStoreCrawler` and the backfill (issue #127): `read_feed` (URL from the `reviews_rss_url` template; reads up to `pages` without stopping at an empty page; `on_page` may stop it; a failed request ends it with `error` naming the page), `appstore_review_entries` (keeps entries with `im:rating`, dropping the app-metadata entry), `appstore_app_name`, `appstore_feed_complete`, `FeedRead` (`fetched`, `empty_pages`, `feed_empty`) and `read_outcome` (`error` / `feed_empty` / `gap` / `ok`) |
| `crawl_outcome.py` | Crawl outcome types: the `OUTCOME_*` constants, `AppCrawlResult` (per app: `outcome`, `fetched`, `new`, `detail`), `StoreCrawlSummary` (per store: `apps`, `storage_path`, `failure`, `warnings`, `as_dict`) and `playstore_failure` |
| `app_version_backfill.py` | `backfill_app_versions()`: groups `app_reviews` rows with `app_version IS NULL` by app (`load_targets`, App Store first), re-fetches each app (`fetch_appstore_versions`, which reads through `appstore_rss.read_feed`, / `fetch_playstore_versions`, which reads through `playstore_reviews.fetch_reviews_page`), and writes or, in dry-run, counts the matches (`settle_app`, `UPDATE ... SET app_version, updated_at = NOW() ... AND app_version IS NULL`). Misses are split into `unmatched_confirmed` (the store's answer was read in full: `appstore_feed_complete`, or a Play stop in `PLAY_COMPLETE_STOPS`) and `unverified`. Play paging decisions live in `scan_play_page` / `play_stop_reason`. Settings come from `crawler_config.yml` (`load_settings`); `format_report` renders the per-app and per-platform report. Entry point: `scripts/backfill_app_version.py` |
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
  `max_retries` / `timeout`, and Play request retries from `playstore.max_retries` /
  `retry_backoff_seconds`. Never hardcode sleeps or retry counts.
- A `ParquetWriteError` must fail the crawl for that batch rather than silently continuing — a
  registered batch with no Parquet object would strand the loader.
- Store field paths live once, in each store crawler's static `_extract_platform_review_id` /
  `_extract_app_version`, and the RSS URL template once, in `appstore.reviews_rss_url`. The
  record builders and `app_version_backfill.match_versions` both call the extractors, and both
  App Store fetchers read the RSS through `appstore_rss.read_feed`, which formats that template;
  change a field or the page handling there, not in a copy. The
  backfill calls them on the classes without an instance, because constructing a store crawler
  creates the `output.base_directory` folders (`BaseCrawler.__init__`) and a `DatabaseConnector`.
  Likewise both Play readers go through `playstore_reviews.fetch_reviews_page`, and the positions
  inside Play's review arrays live once, in `playstore_reviews.review_from_item`.
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
- Play reviews are read only through `playstore_reviews.fetch_reviews_page`: one request per page,
  TLS verified by `requests` (the process-wide default TLS context is left alone), and
  `global.timeout` per request. Connection errors (a connection dropped while the body is read
  included), timeouts, HTTP 429 / 5xx and a body without a payload that carries PlayGatewayError are
  retried `playstore.max_retries` times, waiting `playstore.retry_backoff_seconds` times the retry
  number. A request that still fails, a TLS failure (certificate verification or handshake, raised
  at once), any other HTTP error, a body that does not parse (the payload-less entry Play sends for
  a bad continuation token included), and a review without a non-empty string `reviewId`, an int
  `score` from 1 to 5 or an int epoch `at` (every live review had all three, so their positions
  moved; the error names the item, the field and its type, never the value) raise
  `PlayReviewsError`, so a failure never looks like the end of the feed or a page of empty reviews.
  No live PlayGatewayError answer has been captured, so its name is matched anywhere in a body, but
  a body with a payload is read as a page: review text can quote the name. The end is a page whose
  payload `[-2]` is null, so it has no next-page token. Play answers an app with no reviews, and an
  app id it does not know, with an empty page and no token. Measured on 2026-11-28 (ko/kr, newest
  first): the 5 apps paged to their end each ended on a partial page without a token, and none of
  about 50 pages from 7 apps was empty or partial with a token.
  - Crawl: `PlayStoreCrawler.run` makes an app whose request failed `error`, a warning. Reviews
    read before a later page failed are kept: they become records and count in `fetched`, as
    `OUTCOME_ERROR` defines and as the App Store crawler and the backfill do. A page without
    reviews that still carries a token also ends the app as `error` (never seen live; following
    it would request pages without end).
  - Backfill: `fetch_playstore_versions` turns `PlayReviewsError` into an `error` stop naming the
    page. The matches found before it are written, the rest stay unverified, and the script exits
    1. An empty page still never ends a read as complete: the first is `feed_empty`, a later one
    `empty_page`, both unverified.
- A Play page with reviews and no next-page token is still not trusted as the end on its own.
  `fetch_playstore_versions` requests that page once more with the same continuation token
  (after `global.delay_between_requests`); `confirm_play_end` merges any targets the second
  answer holds, and `play_end_stop_reason` confirms the end (`exhausted`, misses
  `unmatched_confirmed`) only if both answers carry the same review ids and no token. Anything
  else is `end_not_confirmed` (misses unverified). The check was added for google-play-scraper
  1.2.7, whose `reviews()` swallowed a failed follow-up request and returned the partial list
  without a token. With one request per page and raised errors that cause is gone. It stays
  because the sample above cannot rule out the store itself ending a feed early, and it costs one
  request per app that reaches its end.
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
(`crawl_config`). `tests/test_crawl_outcome.py` covers `playstore_failure` with Play review
dicts, `run_crawl` store selection and per-store failure, the pipeline CLI crawl step, and an
Airflow-style retry after the cutoff on the same day against a real MinIO bucket, `BatchLoader`
and `ReviewCleaningPipeline` (each review loaded once and cleansed once).
`tests/test_crawl_reviews_cli.py` covers `scripts/crawl_reviews.py` exit codes.
`tests/test_crawler_consistency.py` covers `save_daily_batch` only: the MinIO upload happens before
the `ingestion_batch` PENDING row, a failed upload opens no DB session, and a failed commit deletes
the uploaded object. It drives `AppStoreCrawler` with stand-in records, so no test compares App
Store and Play Store output. `tests/test_bronze_loading.py` covers the Parquet → DB handoff and
each store's `app_version` mapping into Bronze. `tests/test_playstore_reviews.py` covers the Play
client over real HTTP against the local `play_reviews_server` (first, next, last and empty page,
HTTP errors, HTTP 429 / 5xx, dropped-connection and PlayGatewayError retries, the linear backoff, a
review quoting the PlayGatewayError name read at once, a TLS failure raised at once, timeout,
malformed bodies, reviews missing a required field or shifted by one position, a bad token, field
extraction with and without reply and version, and the default TLS context left alone on import,
checked in a fresh interpreter) and `PlayStoreCrawler.run` over it (every app empty or failing fails
the store, one failed app is a warning, a review whose positions moved makes its app an error, a
page without reviews but with a token ends its app as an error, a later page failing keeps the
earlier reviews into Bronze, paging up to `reviews_per_app`, reviews into Bronze through a real
MinIO bucket with the catalog app name kept).
`tests/test_app_version_backfill.py` covers the backfill: `fetch_appstore_versions`,
`fetch_playstore_versions` and `backfill_app_versions` run over real HTTP against the local
`appstore_rss_server` and `play_reviews_server` (conftest) and against committed rows
(`backfill_db`); Play paging decisions are also covered on their own through `scan_play_page`.
`tests/test_backfill_app_version_cli.py` covers the script. `tests/conftest.py` still defines
`mock_appstore_api`, but no test uses it, and it cannot run: it depends on the `requests_mock`
fixture of the requests-mock plugin, which is in neither `pyproject.toml` nor `uv.lock`. Using it
means adding that dev dependency first.

```bash
PYTHONPATH=. uv run pytest tests/test_crawler_consistency.py tests/test_bronze_loading.py \
  tests/test_app_version_backfill.py tests/test_backfill_app_version_cli.py \
  tests/test_appstore_rss.py tests/test_playstore_reviews.py tests/test_crawl_outcome.py \
  tests/test_crawl_reviews_cli.py -q
```
Database-backed tests need the Docker test database, and the re-run and Play Bronze tests the test
MinIO; start and connect them as described in `tests/AGENTS.md` (Testing Requirements).

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
`requests`, `pyarrow`, `boto3`, `uuid6`, `pyyaml`.

<!-- MANUAL: hand-maintained; keep this marker as the last line and add notes to their section (root AGENTS.md) -->
