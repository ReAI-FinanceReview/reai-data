<!-- Parent: ../AGENTS.md -->
<!-- Generated: 2026-08-10T08:45:46Z | Updated: 2026-11-26T00:00:00Z -->

# config/app_ids

## Purpose
Crawl targets. Plain-text lists of the store app identifiers to crawl, grouped by Korean bank
category, plus an exclusion list. These files define the pipeline's scope: 61 apps across two
stores (36 App Store + 25 Play Store), matching the active `app_metadata` links in the seed catalog
(`sql/apps_data.sql` holds 65 apps; the other 4 are retired).

## Key Files

| File | Description |
|------|-------------|
| `appstore_app_ids.txt` | Numeric App Store IDs, one per line, sectioned by bank type (`### 특수 은행 ###` etc.). Referenced by `crawler_config.yml → app_ids.appstore` |
| `playstore_app_ids.txt` | Play Store package names, one per line, same sectioning. Referenced by `app_ids.playstore` |
| `excluded_apps.txt` | Exclusion list in `platform:app_id  # reason` form. Currently all lines are comments, so nothing is excluded |

## Subdirectories
None.

## For AI Agents

### Working In This Directory
- `BaseCrawler.read_app_ids()` skips blank lines and lines starting with `#`, so section headers and
  documentation comments are safe.
- Adding an app here is only half the change: crawled reviews resolve against `apps` /
  `app_metadata`, so a new app also needs seed rows in `sql/apps_data.sql` and
  `sql/app_metadata_data.sql` (and the bootstrap verification counts of 39 / 65 / 61 will need
  updating in `src/bootstrap_db.py`).
- Retiring an app is the mirror image: turn its line into a `#` comment that states the reason and
  date, and close its `app_metadata` row (`valid_to`, `is_active = FALSE`) rather than deleting the
  `apps` row, so reviews already loaded for it keep their mapping.
- App Store entries are numeric IDs; Play Store entries are package names. Mixing them silently
  yields empty crawls.
- File locations are configurable via `config/crawler_config.yml`; do not hardcode these paths in
  new code — read them from config as `AppStoreCrawler.__init__` /
  `PlayStoreCrawler.__init__` do.

### Testing Requirements
No test reads these files directly (`tests/conftest.py` defines a `sample_app_id_file` fixture, but
no test uses it; the crawl tests write their own lists through `crawl_config`). There is no
per-app crawl: `scripts/crawl_reviews.py` crawls every app in one list (`--store appstore` or
`--store playstore`) or, by default, in both (`run_crawl` through `UnifiedCrawler`), uploading
Bronze Parquet to MinIO and registering `ingestion_batch` rows. Run it only against the local stack
(`docs/local-development.md`), and expect it to write a full batch there:

```bash
PYTHONPATH=. uv run python scripts/crawl_reviews.py
```

### Common Patterns
`#`-prefixed comments carry section names and exclusion reasons; entries stay grouped by bank
category so the list stays reviewable by humans.

## Dependencies

### Internal
`config/crawler_config.yml`, `src/crawlers/base_crawler.py`, `src/crawlers/appstore_crawler.py`,
`src/crawlers/playstore_crawler.py`, `sql/apps_data.sql`.

### External
App Store and Google Play identifier namespaces.

<!-- MANUAL: hand-maintained; keep this marker as the last line and add notes to their section (root AGENTS.md) -->
