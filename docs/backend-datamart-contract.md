# Backend Data Mart Contract

This document defines the first-pass backend-facing physical table contract for the current Gold/data mart outputs. Backend consumers may read these physical tables directly, but only the four tables listed here are in scope.

## Contract boundary

| Table | Backend use | Contract owner | Source evidence |
|---|---|---|---|
| `fact_service_review_daily` | Service summary dashboard metrics by date, service, and platform | Data pipeline / backend integration | `sql/schema_v4.sql:476-501`, `src/models/fact_service_review_daily.py:13-39`, `src/gold/aggregator.py:275-319` |
| `fact_service_aspect_daily` | Keyword/aspect trend and cloud metrics | Data pipeline / backend integration | `sql/schema_v4.sql:507-522`, `src/models/fact_service_aspect_daily.py:12-28`, `src/gold/aggregator.py:321-345` |
| `fact_category_radar_scores` | Category radar chart metrics | Data pipeline / backend integration | `sql/schema_v4.sql:528-543`, `src/models/fact_category_radar_scores.py:15-36`, `src/gold/aggregator.py:347-371` |
| `srv_daily_review_list` | Hot review-list serving table | Data pipeline / backend integration | `sql/schema_v4.sql:553-584`, `src/models/srv_daily_review_list.py:16-49`, `src/gold/aggregator.py:373-420` |

Out of scope for this handoff:

- Backend API endpoints, routers, controllers, DTOs, or service-layer code.
- New `fact_*`, `dim_*`, `srv_*`, view, or materialized-view tables.
- Broad `GoldAggregator` redesigns or unrelated data-quality framework work.

## Global semantics

### Date/timezone semantics

- `fact_* .date` is the aggregation date generated from `DATE_TRUNC('day', review_master_index.review_created_at)::date` in the current aggregator SQL.
- `srv_daily_review_list.date` is the partition key and is generated from the same `DATE_TRUNC('day', review_master_index.review_created_at)::date` expression in the current aggregator. Schema comments describe it as typically `DATE(reviewed_at)`.
- `review_master_index.review_created_at` stores platform review time. The schema comments mark mart dates as UTC, so backend consumers should treat the produced `date` as a UTC reporting date and keep the PostgreSQL session timezone stable when running aggregation jobs.
- If future aggregation changes alter timezone normalization, that is a semantic contract change even if the SQL column type remains `DATE`.

### Referential behavior

All four contract tables currently reference `app_service(service_id)` with `ON DELETE CASCADE` in `sql/schema_v4.sql:641-667`. Backend consumers should treat service deletion as removing matching mart rows. Changing or removing this cascade behavior is backend-impacting.

### TTL/partition retention

`srv_daily_review_list` is a partitioned hot serving table with TTL intent. The table comment states a 7-14 day hot window, and `GoldAggregator.run(..., retention_days=14)` uses a default 14-day partition retention argument. Backend review-list reads should target the hot window; long-range history is a separate data-product decision.

### Nullability and availability

- `fact_* .service_id` columns are non-null contract keys.
- Nullable `srv_daily_review_list.service_id`: `srv_daily_review_list.service_id` is currently nullable. Service-scoped backend reads must either filter `service_id = :service_id` and naturally exclude null-service rows, or handle null-service rows explicitly in unscoped review feeds.
- Numeric aggregate fields may be null unless the current schema marks them `NOT NULL`; consumers should use `COALESCE` at presentation boundaries when a zero default is required.

## Column contract matrices

### `fact_service_review_daily`

Primary key: `(date, service_id, platform_type)`.

Required serving index: `idx_fact_service_review_daily_date` on `(date)`.

| Column | PostgreSQL type | Nullable | Key / index role | Semantic meaning | Example value | Backend usage |
|---|---|---:|---|---|---|---|
| `date` | `DATE` | No | PK member; covered by `idx_fact_service_review_daily_date` | UTC reporting date derived from review creation time | `2026-05-01` | Date-range dashboard filter |
| `service_id` | `UUID` | No | PK member; FK to `app_service(service_id)` | Logical service identifier | `:service_id` | Service-scoped dashboard filter |
| `platform_type` | `platform_type` enum | No | PK member | Source platform: `APPSTORE` or `PLAYSTORE` | `PLAYSTORE` | Platform breakdown/filter |
| `total_review_cnt` | `INT` | Yes | Metric | Count of analyzed reviews in the group | `128` | Total review volume |
| `action_required_cnt` | `INT` | Yes | Metric | Count of reviews requiring action | `11` | Operations workload indicator |
| `attention_required_cnt` | `INT` | Yes | Metric | Count of reviews requiring attention | `24` | Monitoring indicator |
| `pos_count` | `INT` | Yes | Metric | Count of reviews with average sentiment score `>= 0.5` | `92` | Positive review count |
| `neg_count` | `INT` | Yes | Metric | Count of reviews with average sentiment score `< 0.5` | `36` | Negative review count |
| `avg_rating` | `FLOAT` | Yes | Metric | Average platform rating | `4.21` | Rating trend |
| `action_ratio` | `FLOAT` | Yes | Metric | `action_required_cnt / total_review_cnt`, rounded by current aggregator SQL | `0.0859` | Action-required rate |

### `fact_service_aspect_daily`

Primary key: `(date, service_id, keyword)`.

Required serving index: `idx_fact_service_aspect_daily_date` on `(date)`.

| Column | PostgreSQL type | Nullable | Key / index role | Semantic meaning | Example value | Backend usage |
|---|---|---:|---|---|---|---|
| `date` | `DATE` | No | PK member; covered by `idx_fact_service_aspect_daily_date` | UTC reporting date derived from review creation time | `2026-05-01` | Date-range trend filter |
| `service_id` | `UUID` | No | PK member; FK to `app_service(service_id)` | Logical service identifier | `:service_id` | Service-scoped trend filter |
| `keyword` | `TEXT` | No | PK member | Extracted aspect keyword | `login` | Keyword label |
| `mention_cnt` | `INT` | Yes | Metric | Number of keyword mentions | `37` | Cloud weight / ranking |
| `avg_sentiment_score` | `FLOAT` | Yes | Metric | Average sentiment score for the keyword | `0.68` | Keyword sentiment trend |

### `fact_category_radar_scores`

Primary key: `(date, service_id, category_type)`.

Required serving index: `idx_fact_category_radar_scores_date` on `(date)`.

Current category allow-list from the aggregator: `USABILITY`, `STABILITY`, `DESIGN`, `CUSTOMER_SUPPORT`, `SPEED`.

| Column | PostgreSQL type | Nullable | Key / index role | Semantic meaning | Example value | Backend usage |
|---|---|---:|---|---|---|---|
| `date` | `DATE` | No | PK member; covered by `idx_fact_category_radar_scores_date` | UTC reporting date derived from review creation time | `2026-05-01` | Radar date filter |
| `service_id` | `UUID` | No | PK member; FK to `app_service(service_id)` | Logical service identifier | `:service_id` | Service-scoped radar filter |
| `category_type` | `category_type` enum | No | PK member | Radar category | `USABILITY` | Radar axis |
| `avg_sentiment_score` | `FLOAT` | Yes | Metric | Average sentiment score for the category | `0.72` | Radar score |
| `review_cnt` | `INT` | Yes | Metric | Distinct review count contributing to the category | `54` | Sample-size display / confidence hint |

### `srv_daily_review_list`

Primary key: `(review_id, date)`.

Required serving indexes:

- `idx_srv_daily_review_list_service_id` on `(service_id)`.
- `idx_srv_daily_review_list_date` on `(date)`.
- `idx_srv_daily_review_list_is_action_required` partial index on `(is_action_required)` where `is_action_required = true`.
- `idx_srv_daily_review_list_keyword` GIN index on `(keyword)`.

The table is partitioned by `RANGE (date)`.

| Column | PostgreSQL type | Nullable | Key / index role | Semantic meaning | Example value | Backend usage |
|---|---|---:|---|---|---|---|
| `review_id` | `UUID` | No | PK member | Global review identifier | `:review_id` | Detail lookup / pagination tie-breaker |
| `date` | `DATE` | No | PK member; partition key; covered by `idx_srv_daily_review_list_date` | Review-list partition/reporting date | `2026-05-01` | Hot-window and partition-pruning filter |
| `service_id` | `UUID` | Yes | FK to `app_service(service_id)`; covered by `idx_srv_daily_review_list_service_id` | Logical service identifier when available | `:service_id` | Service-scoped review list |
| `refined_text` | `TEXT` | Yes | Display field | Preprocessed review text | `The latest update is slow.` | Review body snippet |
| `review_summary` | `TEXT` | Yes | Display field | LLM one-sentence summary | `User reports slow update.` | Summary display |
| `rating` | `INT` | Yes | Display/filter field | Platform star rating | `2` | Rating display/filter |
| `reviewed_at` | `TIMESTAMPTZ` | Yes | Sort/display field | Original platform review timestamp | `2026-05-01T10:15:00Z` | Stable ordering and display |
| `sentiment_score` | `FLOAT` | Yes | Metric | Average sentiment score for the review | `0.32` | Sentiment display/filter |
| `is_action_required` | `BOOLEAN` | Yes | Covered by partial action index for `true` rows | Whether the review needs action | `true` | Action queue filter |
| `is_attention_required` | `BOOLEAN` | Yes | Filter/display field | Whether the review needs attention | `false` | Attention queue filter |
| `assigned_dept` | `TEXT[]` | Yes | Display/filter candidate | Assigned department list | `{support,product}` | Routing display |
| `keyword` | `TEXT[]` | Yes | GIN-indexed keyword array | Review keyword array | `{login,crash}` | Keyword filtering/search |
| `confidence` | `FLOAT` | Yes | Metric | Department assignment confidence | `0.91` | Routing confidence display |

## Backend query examples

The examples use named bind parameters. They intentionally avoid hardcoded service IDs, date ranges, and pagination cursors.

### Service summary dashboard

Reads `fact_service_review_daily` by date range, service, and optional platform.

```sql
SELECT
  date,
  platform_type,
  COALESCE(total_review_cnt, 0) AS total_review_cnt,
  COALESCE(action_required_cnt, 0) AS action_required_cnt,
  COALESCE(attention_required_cnt, 0) AS attention_required_cnt,
  COALESCE(pos_count, 0) AS pos_count,
  COALESCE(neg_count, 0) AS neg_count,
  avg_rating,
  action_ratio
FROM fact_service_review_daily
WHERE service_id = :service_id
  AND date BETWEEN :start_date AND :end_date
  AND (:platform_type IS NULL OR platform_type = :platform_type)
ORDER BY date ASC, platform_type ASC;
```

### Keyword/aspect trend

Reads `fact_service_aspect_daily` by date range and service, ranking keywords by mentions.

```sql
SELECT
  date,
  keyword,
  COALESCE(mention_cnt, 0) AS mention_cnt,
  avg_sentiment_score
FROM fact_service_aspect_daily
WHERE service_id = :service_id
  AND date BETWEEN :start_date AND :end_date
ORDER BY date ASC, mention_cnt DESC, keyword ASC
LIMIT :limit;
```

### Radar chart

Reads `fact_category_radar_scores` by date, service, and optional category list.

```sql
SELECT
  category_type,
  avg_sentiment_score,
  COALESCE(review_cnt, 0) AS review_cnt
FROM fact_category_radar_scores
WHERE service_id = :service_id
  AND date = :date
  AND (:category_types IS NULL OR category_type = ANY(:category_types))
ORDER BY category_type ASC;
```

### Review list with stable pagination order

Service-scoped reads naturally exclude rows where `service_id IS NULL` by requiring `service_id = :service_id`. For unscoped operational feeds, decide explicitly whether null-service rows should be included.

```sql
SELECT
  review_id,
  date,
  service_id,
  reviewed_at,
  rating,
  refined_text,
  review_summary,
  sentiment_score,
  is_action_required,
  is_attention_required,
  assigned_dept,
  keyword,
  confidence
FROM srv_daily_review_list
WHERE service_id = :service_id
  AND date BETWEEN :start_date AND :end_date
  AND (:action_required_only IS NOT TRUE OR is_action_required = true)
  AND (
    :cursor_reviewed_at IS NULL
    OR (reviewed_at, review_id) < (:cursor_reviewed_at, :cursor_review_id)
  )
ORDER BY reviewed_at DESC NULLS LAST, review_id DESC
LIMIT :limit;
```

Current serving indexes support service, date, action-required, and keyword filtering, but there is no composite pagination index for `(service_id, date, reviewed_at, review_id)` or equivalent. Do not promise cursor-pagination latency as a hard contract until a follow-up index decision is made and migrated.

### Keyword-filtered review list

```sql
SELECT
  review_id,
  date,
  service_id,
  reviewed_at,
  rating,
  refined_text,
  keyword
FROM srv_daily_review_list
WHERE service_id = :service_id
  AND date BETWEEN :start_date AND :end_date
  AND keyword @> ARRAY[:keyword]::text[]
ORDER BY reviewed_at DESC NULLS LAST, review_id DESC
LIMIT :limit;
```

## Breaking-change policy and compatibility rules

A change is backend-breaking when it does any of the following to one of the four contract tables:

- Removes, renames, or repurposes a contract table.
- Removes, renames, or repurposes a contract column.
- Changes PostgreSQL type, enum domain, nullability, primary key, partition key, foreign-key cascade behavior, or required serving index.
- Changes aggregate meaning, timezone/date semantics, retention semantics, or row inclusion/exclusion semantics even when SQL types remain unchanged.
- Adds a backend dependency on a new mart object without updating this document and the regression contract tests.

Breaking changes require:

1. Alembic revision.
2. Contract document update.
3. Contract regression test update.
4. Backend coordination note that names the migration impact and rollout expectation.

Future schema changes should follow `docs/schema-management.md`; direct edits to the baseline `sql/schema_v4.sql` are not the normal forward-migration path.

## Docker validation and verification commands

Focused contract and serving-readiness command expected for this handoff:

```bash
PYTHONPATH=. uv run pytest tests/test_backend_datamart_contract.py -q
```

Related existing suite:

```bash
PYTHONPATH=. uv run pytest tests/test_gold_aggregator.py tests/test_database_schema.py -q
```

Docker PostgreSQL schema proof:

```bash
docker compose -f docker-compose.test.yml up -d test-postgres
TEST_DATABASE_URL="${TEST_DATABASE_URL:-postgresql://testuser:testpass@localhost:5433/testdb}" \
PYTHONPATH=. uv run pytest tests/test_backend_datamart_contract.py -q
```

The focused contract suite includes both schema/query contract checks and a
serving-readiness row proof. The row proof seeds upstream analyzed review rows,
invokes the real `aggregate` pipeline step through `run_steps(["aggregate"],
target_date=...)`, and asserts generated rows plus backend-facing semantics in
all four contract tables.

## Serving readiness proof

The automated readiness proof intentionally exercises the real aggregate step
instead of direct mart inserts:

1. Seed a committed PostgreSQL fixture with upstream rows in `app_service`,
   `apps`, `review_master_index`, `app_reviews`, `reviews_preprocessed`,
   `review_action_analysis`, `review_aspects`, and `reviews_assigned`.
2. Set `DATABASE_URL` to the isolated test PostgreSQL URL.
3. Run the pipeline step dispatcher with `aggregate` and an explicit
   `target_date`.
4. Assert generated rows in:
   - `fact_service_review_daily`
   - `fact_service_aspect_daily`
   - `fact_category_radar_scores`
   - `srv_daily_review_list`
5. Assert semantic values used by backend consumers: review counts, action and
   attention flags, sentiment aggregates, category scores, review summary,
   assigned departments, keywords, and confidence.

This proof covers AC2-AC4 for the aggregate serving boundary. Full live crawl,
load, cleanse, and Gold analysis remain manual release evidence because they can
depend on external store availability and LLM credentials.

## Manual live crawl smoke proof

Before release, record a manual live smoke run separately from PR automation:

```bash
docker compose up -d postgres minio minio-init
PYTHONPATH=. uv run python scripts/bootstrap_db.py
PYTHONPATH=. uv run python scripts/crawl_reviews.py
PYTHONPATH=. uv run python scripts/load_reviews.py
PYTHONPATH=. uv run python scripts/cleanse_reviews.py --date "$(date -I)"
PYTHONPATH=. uv run python scripts/run_pipeline.py --steps gold,aggregate --target-date "$(date -I)"
```

Capture the command timestamp, service/source target, and these row counts:

```sql
SELECT 'fact_service_review_daily' AS table_name, COUNT(*) FROM fact_service_review_daily WHERE date = :target_date
UNION ALL
SELECT 'fact_service_aspect_daily', COUNT(*) FROM fact_service_aspect_daily WHERE date = :target_date
UNION ALL
SELECT 'fact_category_radar_scores', COUNT(*) FROM fact_category_radar_scores WHERE date = :target_date
UNION ALL
SELECT 'srv_daily_review_list', COUNT(*) FROM srv_daily_review_list WHERE date = :target_date;
```

Store the evidence with the release notes or PR checklist. If the crawl or LLM
analysis is flaky because of external network, store, or credential state,
record the caveat and keep the automated PostgreSQL readiness proof as the PR
gate.
