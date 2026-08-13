<!-- Parent: ../AGENTS.md -->
<!-- Generated: 2026-08-10T08:45:46Z | Updated: 2026-08-10T08:45:46Z -->

# src/models

## Purpose
SQLAlchemy ORM models mirroring `sql/schema_v4.sql`, organized by medallion layer. One declarative
`Base` and one central ENUM module keep type definitions from drifting across files. Some models
(`Review`, `ReviewPreprocessed`) exist mainly as structural documentation of tables whose payload
actually lives in Parquet.

## Key Files
| File | Description |
|------|-------------|
| `base.py` | The single `declarative_base()` every model inherits |
| `enums.py` | Central ENUMs: `PlatformType`, `AppType`, `ProcessingStatusType`, `AnalysisStatusType`, `SentimentType`, `IngestionBatchStatusType`, `CategoryType` |
| `__init__.py` | Package version (`5.0.0`) and the curated re-export list grouped by layer |
| `app_service.py` | `AppService` — logical service master (39 rows) |
| `apps.py` | `App` — physical app instance per platform (63 rows) |
| `app_metadata.py` | `AppMetadata` — app ↔ service link with SCD Type 2 history (`valid_from`/`valid_to`/`is_active`) |
| `organizations.py` | `Organization` — 114-row org hierarchy. **Docstring claims `ltree`, but the DDL has none**: `org_id` is plain `TEXT` with hyphen levels (`1`, `1-1`, `10-5`). ltree was removed in issue #32 |
| `review_master_index.py` | `ReviewMasterIndex` — central hub across Bronze → Silver → Gold; carries `processing_status`, `retry_count`, `error_message` |
| `ingestion_batch.py` | `IngestionBatch` — Parquet batch ingestion state and batch-level DLQ |
| `review.py` | `Review` — Bronze raw review (stored as Parquet, not queried in DB) |
| `review_preprocessed.py` | `ReviewPreprocessed` — Silver cleansed text (stored as Parquet) |
| `review_embedding.py` | `ReviewEmbedding` — pgvector embedding column |
| `review_aspects.py` | `ReviewAspect` — ABSA keyword/sentiment/category rows |
| `review_action_analysis.py` | `ReviewActionAnalysis` — Snorkel actionability results and LLM summary |
| `llm_analysis_log.py` | `LLMAnalysisLog` — LLM call audit trail with JSONB payload |
| `review_assigned.py` | `ReviewAssigned` — final department assignment (Gold) |
| `fact_service_review_daily.py` | `FactServiceReviewDaily` — service × platform × date review counts and averages |
| `fact_service_aspect_daily.py` | `FactServiceAspectDaily` — service × date × keyword mentions and sentiment |
| `fact_category_radar_scores.py` | `FactCategoryRadarScores` — service × date × `CategoryType` radar scores (USABILITY / STABILITY / DESIGN / CUSTOMER_SUPPORT / SPEED) |
| `srv_daily_review_list.py` | `SrvDailyReviewList` — denormalized wide serving mart, `PARTITION BY RANGE(date)`; partitions must pre-exist |
| `dictionary.py` | `Synonym`, `Profanity`, `FinancialTerm` reference tables |

## Subdirectories
None.

## For AI Agents

### Working In This Directory
- **Never define an enum locally.** Import from `enums.py`; duplicated enum types break the
  PostgreSQL type mapping.
- These models do not create the schema. DDL comes from `sql/schema_v4.sql` plus Alembic revisions —
  editing a model does not migrate anything, and `alembic --autogenerate` is disabled because model
  metadata is not yet lossless against the baseline.
- Adding a model means adding the import **and** the `__all__` entry in `__init__.py`; several
  modules rely on `from src.models import X`.
- `Review` and `ReviewPreprocessed` describe Parquet-resident tables; do not add query paths that
  assume their rows are populated in PostgreSQL.
- `ReviewEmbedding` requires `pgvector`, which is why tests need `pgvector/pgvector:pg17` rather than
  plain PostgreSQL. `Organization`'s docstring still claims the `ltree` extension, but
  `schema_v4.sql` contains zero `ltree` references — treat the docstring as stale.

### Testing Requirements
```bash
TEST_DATABASE_URL="postgresql://testuser:testpass@localhost:5433/testdb" \
  PYTHONPATH=. uv run pytest tests/test_enums.py tests/test_database_schema.py -q
```
Any model change must also be checked against `sql/schema_v4.sql` and the mart contract in
`docs/backend-datamart-contract.md`.

### Common Patterns
Korean module docstring plus English body comments; `Column(UUID(as_uuid=True))` for identifiers,
`Enum as SQLEnum` bound to the central enums, `server_default=func.now()` for audit timestamps.

## Dependencies

### Internal
`sql/schema_v4.sql` (authoritative DDL), `alembic/versions/`, consumed by `src/loaders/`,
`src/gold/`, `src/pipeline/`, and `tests/`.

### External
`sqlalchemy`, `pgvector.sqlalchemy` (optional import guard), PostgreSQL dialect types
(`UUID`, `JSONB`, `ARRAY`).

<!-- MANUAL: Notes added below this line are preserved on regeneration -->
