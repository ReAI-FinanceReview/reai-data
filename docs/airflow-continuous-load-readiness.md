# Airflow Continuous Load Readiness

## Contract boundary

`financial_review_etl_pipeline`의 운영 성공 기준은 Airflow task 실행 성공에 더해
`gold_aggregate` 이후 `post_aggregate_validate` DB 검증 task 성공까지 포함한다.

검증 task는 target date(`{{ ds }}`) 기준으로 기존 DB 테이블을 조회한다. 1차 범위에서는
신규 모니터링 테이블, Slack/Email/PagerDuty, live App Store/Play Store API gate,
Metabase/Grafana dashboard 구현을 추가하지 않는다.

## Warning vs failure policy

| 판정 | 의미 | Airflow 결과 |
|---|---|---|
| `fresh_ingestion` warning | target date 신규 batch/review count가 0건 | 성공 가능, report 확인 |
| `batch_state` failure | `PENDING`, `FAILED`, `RETRYING`, `DEAD_LETTER` batch가 남음 | task 실패 |
| `review_state` failure | `RAW`, `CLEANED`, retry-exhausted `FAILED` review가 남음 | task 실패 |
| `metadata_mapping` failure | `service_id` 또는 active `app_metadata` mapping 누락 | task 실패 |
| `orphan_integrity` failure | analyzed row의 action/app review/preprocessed/aspect 연결 누락 | task 실패 |
| `mart_freshness` failure | analyzed upstream이 있는데 target-date mart row가 없음 | task 실패 |
| `mart_count_consistency` failure | `fact_service_review_daily.total_review_cnt`와 upstream count 불일치 | task 실패 |
| `mart_quality` failure | mart count/rating/ratio/sentiment/confidence 범위 위반 | task 실패 |

## Operator commands

Airflow task와 동일한 검증은 아래 명령으로 재현한다.

```bash
PYTHONPATH=. uv run python -m src.pipeline.cli \
  --steps post_aggregate_validate \
  --target-date YYYY-MM-DD
```

집계까지 포함한 로컬 검증은 아래 순서로 실행한다.

```bash
PYTHONPATH=. uv run python -m src.pipeline.cli \
  --steps aggregate,post_aggregate_validate \
  --target-date YYYY-MM-DD
```

## Interpreting validation output

CLI 로그의 `Result` JSON에서 다음 필드를 확인한다.

| 필드 | 해석 |
|---|---|
| `status` | `success`면 필수 DB 검증 통과, `failed`면 Airflow task non-zero 대상 |
| `warnings` | 실패는 아니지만 운영자가 확인해야 할 report성 신호 |
| `checks[].severity` | `failure`는 task 실패 기준, `warning`은 report 기준 |
| `checks[].metrics` | SQL count 기반 원인 파악용 수치 |

## Verification commands

```bash
PYTHONPATH=. uv run pytest \
  tests/test_post_aggregate_validation.py \
  tests/test_pipeline_cli_validation.py \
  tests/test_airflow_dag_validation_wiring.py \
  tests/test_airflow_readiness_docs.py

PYTHONPATH=. uv run pytest \
  tests/test_backend_datamart_contract.py \
  tests/test_backend_datamart_serving_readiness.py \
  tests/test_pipeline_steps.py
```

DB 테스트는 실제 PostgreSQL test DB가 필요하다. 기본 연결은 `TEST_DATABASE_URL` 또는
`TEST_POSTGRES_PORT`/`docker-compose.test.yml` 정책을 따른다.

## Follow-up: Metabase/Grafana

Metabase/Grafana readiness metrics dashboard는 1차 구현 범위가 아니다. 별도 이슈로
다음 항목을 등록한다.

- daily ingestion count trend
- warning/failure check trend
- mart freshness and row-count trend
- service/platform breakdown for failed checks
