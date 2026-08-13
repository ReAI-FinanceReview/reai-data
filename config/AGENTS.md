<!-- Parent: ../AGENTS.md -->
<!-- Generated: 2026-08-10T08:45:46Z | Updated: 2026-08-10T08:45:46Z -->

# config

## Purpose
File-based runtime configuration: crawler tuning, logging setup, medallion storage paths, target
app-id lists, and Korean NLP dictionaries. Secrets never live here — they come from `.env` /
environment variables, and `paths.yml` only references them via `${VARIABLE}` placeholders.

## Key Files
| File | Description |
|------|-------------|
| `crawler_config.yml` | Default config for every component (`config_path` default). Global delay/retry/timeout, per-store country/language/page counts, output settings, and app-id file locations |
| `paths.yml` | Medallion storage layout (`bronze_dir`, `silver_dir`, `gold_dir`, `embeddings_dir`, `logs_dir`) built from `${PARQUET_BASE_PATH}`; consumed by `src/utils/path_resolver.py` |
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
  `playstore.reviews_per_app: 10`); raise them deliberately, not incidentally.
- `output.*` in `crawler_config.yml` describes the legacy CSV dump path; the production Bronze path
  is MinIO Parquet driven by `paths.yml` + `PARQUET_BASE_PATH`.
- Do not hardcode absolute paths. Add new storage locations to `paths.yml` using `${VARIABLE}` and
  resolve them through `PathResolver` / `get_medallion_paths()`.
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
YAML is loaded with `yaml.safe_load` and accessed defensively via
`config.get('section', {}).get('key', <default>)`, so a missing key degrades to a coded default
instead of raising.

## Dependencies

### Internal
`src/utils/path_resolver.py`, `src/utils/logger.py`, `src/crawlers/*`, `src/pipeline/cli.py`,
`scripts/cleanse_reviews.py`.

### External
`pyyaml`; `PARQUET_BASE_PATH` and the other variables documented in `.env.example`.

<!-- MANUAL: Notes added below this line are preserved on regeneration -->
