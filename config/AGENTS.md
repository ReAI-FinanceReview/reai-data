<!-- Parent: ../AGENTS.md -->
<!-- Generated: 2026-08-10T08:45:46Z | Updated: 2026-11-26T00:00:00Z -->

# config

## Purpose
File-based runtime configuration: crawler tuning, logging setup, medallion storage paths, target
app-id lists, and Korean NLP dictionaries. Secrets never live here — they come from `.env` /
environment variables, and `paths.yml` only references them via `${VARIABLE}` placeholders.

## Key Files

| File | Description |
|------|-------------|
| `crawler_config.yml` | Default config for every component (`config_path` default). Global delay/retry/timeout, per-store country/language/page counts, output settings, and app-id file locations. `appstore.reviews_rss_url` is the RSS URL template read by both `AppStoreCrawler` and the `app_version` backfill. `appstore.rss_page_size`, `appstore.backfill_max_consecutive_empty_feeds`, `playstore.backfill_page_size` and `playstore.backfill_max_reviews_per_app` are read only by the backfill (`src/crawlers/app_version_backfill.py`) |
| `paths.yml` | Medallion storage layout (`bronze_dir`, `silver_dir`, `gold_dir`, `embeddings_dir`, `logs_dir`) built from `${PARQUET_BASE_PATH}`; read by `src/utils/path_resolver.py`, which no production code imports |
| `logging_config.yml` | Logging dictConfig actually loaded by `src/utils/logger.py:30`: console + rotating `logs/crawler/info.log` and `logs/error/error.log` (10 MB × 5) |
| `logging.yml` | Near-identical logging config that no code references; `logging_config.yml` is the live one |

## Subdirectories

| Directory | Purpose |
|-----------|---------|
| `app_ids/` | Store-specific app-id lists and the exclusion list (see `app_ids/AGENTS.md`) |
| `dictionaries/` | Synonym, profanity, and stopword resources for cleansing (see `dictionaries/AGENTS.md`) |

## For AI Agents

### Working In This Directory
- `crawler_config.yml` is the default `config_path` for crawlers, loaders, gold analyzers, and the
  CLI (`--config`). Removing a key can break several components at once — check
  `self.config.get(...)` call sites in `src/crawlers/` before pruning.
- Review-volume knobs are currently set for testing (`appstore.max_reviews_per_app: 10`,
  `playstore.reviews_per_app: 10`); raise them deliberately, not incidentally. The
  `app_version` backfill ignores `appstore.max_reviews_per_app` and reads every page up to
  `appstore.pages_to_crawl`.
- `appstore.reviews_rss_url` and every key the backfill reads have no coded default:
  `AppStoreCrawler.__init__` and the backfill's `load_settings` index the parsed YAML directly, so
  a missing key fails with `KeyError` instead of falling back to a guessed value. Keep them in
  any alternative config passed with `--config`.
- `output.*` in `crawler_config.yml` describes the legacy CSV dump path. The production Bronze and
  Silver locations are not configured in this directory: their MinIO object keys are built in code
  (`src/crawlers/base_crawler.py:433-436` writes `bronze/app_reviews/...`;
  `src/processing/cleanse.py:173-178,197-201` reads Bronze and writes `silver/reviews/...`) inside
  the bucket named by `MINIO_BUCKET`.
- `paths.yml` and `PathResolver` / `get_medallion_paths()` are not used by any code under `src/`,
  `scripts/`, or `dags/` — only `tests/test_path_resolver.py` imports them. Adding a location to
  `paths.yml` changes nothing at run time; change the code that builds the key instead. Do not
  hardcode absolute filesystem paths.
- If you touch logging, edit `logging_config.yml`; `logging.yml` is a stale duplicate and changing
  it alone has no effect.

### Testing Requirements
`tests/test_path_resolver.py` covers placeholder substitution and medallion path building;
`tests/test_cleanse.py` builds its own temporary dictionaries rather than reading these files, so
dictionary edits need a manual cleansing run to verify.

```bash
PYTHONPATH=. uv run pytest tests/test_path_resolver.py -q
```

### Common Patterns
YAML is loaded with `yaml.safe_load`. Call sites that read through
`config.get('section', {}).get('key', <default>)` degrade to a coded default when a key is missing.
`PathResolver.get_path()` is the deliberate exception: it fails fast with a `KeyError` listing the
available keys (`src/utils/path_resolver.py:188-192`) rather than inventing a path. The keys
listed above as having no coded default fail fast the same way.

## Dependencies

### Internal
`src/utils/path_resolver.py`, `src/utils/logger.py`, `src/crawlers/*`, `src/pipeline/cli.py`,
`scripts/cleanse_reviews.py`.

### External
`pyyaml`; the environment variables listed in `.env.example`. `PARQUET_BASE_PATH`, read only through
`paths.yml`, is not among them.

<!-- MANUAL: hand-maintained; keep this marker as the last line and add notes to their section (root AGENTS.md) -->
