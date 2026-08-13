<!-- Parent: ../AGENTS.md -->
<!-- Generated: 2026-08-10T08:45:46Z | Updated: 2026-08-10T08:45:46Z -->

# config/app_ids

## Purpose
Crawl targets. Plain-text lists of the store app identifiers to crawl, grouped by Korean bank
category, plus an exclusion list. These files define the pipeline's scope: 63 apps across two
stores, matching the seed catalog in `sql/apps_data.sql`.

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
  `sql/app_metadata_data.sql` (and the bootstrap verification counts of 39 / 63 / 63 will need
  updating in `src/bootstrap_db.py`).
- App Store entries are numeric IDs; Play Store entries are package names. Mixing them silently
  yields empty crawls.
- File locations are configurable via `config/crawler_config.yml`; do not hardcode these paths in
  new code — read them from config as `appstore_crawler.py:37` / `playstore_crawler.py:36` do.

### Testing Requirements
No test reads these files directly — `tests/conftest.py` provides a `sample_app_id_file` fixture
instead. Verify changes with a scoped live crawl:

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

<!-- MANUAL: Notes added below this line are preserved on regeneration -->
